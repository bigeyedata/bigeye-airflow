import datetime
import json
import logging
from typing import List

from airflow.hooks.http_hook import HttpHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from functools import reduce


def get_case_sensitive_field_name(table: dict, inbound_field_name: str) -> str:
    for f in table['fields']:
        if f['fieldName'].lower() == inbound_field_name.lower():
            return f['fieldName']


def get_not_metric_time_enabled_statistics():
    # To support a list from API
    return [
        'HOURS_SINCE_MAX_TIMESTAMP'.lower(),
        'HOURS_SINCE_MAX_DATE'.lower(),
        'PERCENT_DATE_NOT_IN_FUTURE'.lower(),
        'PERCENT_NOT_IN_FUTURE'.lower(),
        'COUNT_DATE_NOT_IN_FUTURE'.lower()
    ]


def enforce_lookback_type_defaults(metric_name: str, lookback_type: str) -> str:
    if metric_name.lower() in get_not_metric_time_enabled_statistics():
        return 'DATA_TIME_LOOKBACK_TYPE'
    elif lookback_type is None:
        return "METRIC_TIME_LOOKBACK_TYPE"
    else:
        return lookback_type


class CreateMetricOperator(BaseOperator):

    @apply_defaults
    def __init__(self,
                 connection_id: str,
                 warehouse_id: int,
                 configuration: list(dict(schema_name=None, table_name=None, column_name=None,
                                          update_schedule=None, metric_name=None,
                                          extras=...)),
                 run_after_upsert: bool = False,
                 *args,
                 **kwargs):
        super(CreateMetricOperator, self).__init__(*args, **kwargs)
        self.connection_id = connection_id
        self.warehouse_id = warehouse_id
        self.configuration = configuration
        self.run_after_upsert = run_after_upsert

    def execute(self, context):

        num_failing_metric_runs = 0
        created_metrics_ids: List[int] = []

        for c in self.configuration:
            table_name = c["table_name"]
            schema_name = c["schema_name"]
            column_name = c["column_name"]
            default_check_frequency_hours = c.get("default_check_frequency_hours", 2)
            update_schedule = c.get("update_schedule", None)
            delay_at_update = c.get("delay_at_update", "0 minutes")
            timezone = c.get("timezone", "UTC")
            notifications = c.get("notifications", [])
            metric_name = c.get("metric_name")
            should_backfill = c.get("should_backfill", False)
            lookback_type = enforce_lookback_type_defaults(metric_name=metric_name,
                                                           lookback_type=c.get("lookback_type", None))
            lookback_days = c.get("lookback_days", 2)
            window_size_seconds = self._get_seconds_from_window_size(c.get("window_size", "1 day"))
            thresholds = c.get("thresholds", [])
            filters = c.get("filters", [])

            if metric_name is None:
                raise Exception("Metric name must be present in configuration", c)

            # Getting Table
            table = self._get_table_for_name(schema_name, table_name)
            if table is None or table.get("id") is None:
                raise Exception("Could not find table: ", schema_name, table_name)

            # this uglyness goes away by using a dataclass in airflow2.
            if 'group_by' in c:
                if isinstance(c['group_by'], list):
                    group_by = [get_case_sensitive_field_name(table, c) for c in c['group_by']]
                elif c['group_by'] is None:
                    group_by = []
                else:
                    raise Exception(f'Configuration group_by element must be a list or None.\n'
                                    f'Value: {c["group_by"]}.\n'
                                    f'Type: {type(c["group_by"])}')
            else:
                group_by = []

            # Getting Existing Metric
            existing_metric = self._get_existing_metric(table, column_name, metric_name, group_by, filters)
            metric = self._get_metric_object(existing_metric, table, notifications, column_name,
                                             update_schedule, delay_at_update, timezone, default_check_frequency_hours,
                                             metric_name, lookback_type, lookback_days, window_size_seconds, thresholds,
                                             filters, group_by)
            logging.info("Sending metric to create: %s", metric)
            if metric.get("id") is None and not self._is_freshness_metric(metric_name):
                should_backfill = True
            bigeye_post_hook = self.get_hook('POST')
            result = bigeye_post_hook.run("api/v1/metrics",
                                          headers={"Content-Type": "application/json", "Accept": "application/json"},
                                          data=json.dumps(metric))

            metric_id = result.json().get("id")

            logging.info("Create metric status: %s", result.status_code)
            logging.info("Create result: %s", result.json())
            logging.info(f"Created Metric ID: {metric_id}")

            created_metrics_ids.append(metric_id)

            if should_backfill and metric_id is not None and self._table_has_metric_time(table):
                bigeye_post_hook.run("api/v1/metrics/backfill",
                                     headers={"Content-Type": "application/json", "Accept": "application/json"},
                                     data=json.dumps({"metricIds": [result.json()["id"]]}))

            if self.run_after_upsert and metric_id is not None:
                hook = self.get_hook('GET')
                logging.info(f"Running metric: {metric}")
                metric_result = hook.run(
                    f"statistics/runOne/{metric_id}",
                    headers={"Content-Type": "application/json", "Accept": "application/json"}).json()

                for mr in metric_result:
                    if not mr['statusOk']:
                        logging.error("Metric is not OK: %s", metric_id)
                        logging.error("Metric result: %s", mr)
                        num_failing_metric_runs += 1

        return created_metrics_ids



    def _table_has_metric_time(self, table):
        for field in table["fields"]:
            if field["metricTimeField"]:
                return True
        return False

    def _get_seconds_from_window_size(self, window_size):
        if window_size == "1 day":
            return 86400
        elif window_size == "1 hour":
            return 3600
        else:
            raise Exception("Can only set window size of '1 hour' or '1 day'")

    def get_hook(self, method) -> HttpHook:
        return HttpHook(http_conn_id=self.connection_id, method=method)

    def _get_metric_object(self, existing_metric, table, notifications, column_name, update_schedule, delay_at_update,
                           timezone, default_check_frequency_hours, metric_name, lookback_type, lookback_days,
                           window_size_seconds, thresholds, filters, group_by):
        is_freshness_metric = self._is_freshness_metric(metric_name)
        if is_freshness_metric:
            metric_name = self._get_freshness_metric_name_for_field(table, column_name)
            if update_schedule is None:
                raise Exception("Update schedule can not be null for freshness schedule thresholds")
        metric = {
            "scheduleFrequency": {
                "intervalType": "HOURS_TIME_INTERVAL_TYPE",
                "intervalValue": default_check_frequency_hours
            },
            "thresholds": self._get_thresholds_for_metric(metric_name, timezone, delay_at_update, update_schedule,
                                                          thresholds),
            "warehouseId": self.warehouse_id,
            "datasetId": table.get("id"),
            "metricType": {
                "predefinedMetric": {
                    "metricName": metric_name
                }
            },
            "parameters": [
                {
                    "key": "arg1",
                    "columnName": column_name
                }
            ],
            "lookback": {
                "intervalType": "DAYS_TIME_INTERVAL_TYPE",
                "intervalValue": lookback_days
            },
            "notificationChannels": self._get_notification_channels(notifications),
            "filters": filters,
            "groupBys": group_by,
        }
        table_has_metric_time = False
        if not is_freshness_metric:
            for field in table["fields"]:
                if field["metricTimeField"]:
                    table_has_metric_time = True
        if table_has_metric_time:
            metric["lookbackType"] = lookback_type
            if lookback_type == "METRIC_TIME_LOOKBACK_TYPE":
                metric["grainSeconds"] = window_size_seconds
        if existing_metric is None:
            return metric
        else:
            existing_metric["thresholds"] = metric["thresholds"]
            existing_metric["notificationChannels"] = metric.get("notificationChannels", [])
            existing_metric["scheduleFrequency"] = metric["scheduleFrequency"]
            if not is_freshness_metric and table_has_metric_time:
                existing_metric["lookbackType"] = metric["lookbackType"]
                existing_metric["lookback"] = metric["lookback"]
                if lookback_type == "METRIC_TIME_LOOKBACK_TYPE":
                    existing_metric["grainSeconds"] = metric["grainSeconds"]
            return existing_metric

    def _is_freshness_metric(self, metric_name):
        return "HOURS_SINCE_MAX" in metric_name

    def _get_thresholds_for_metric(self, metric_name, timezone, delay_at_update, update_schedule, thresholds):
        if thresholds:
            return thresholds
        # Current path for freshness
        if self._is_freshness_metric(metric_name):
            return [{
                "freshnessScheduleThreshold": {
                    "bound": {
                        "boundType": "UPPER_BOUND_SIMPLE_BOUND_TYPE",
                        "value": -1
                    },
                    "cron": update_schedule,
                    "timezone": timezone,
                    "delayAtUpdate": self._get_time_interval_for_delay_string(delay_at_update,
                                                                              metric_name,
                                                                              update_schedule)
                }
            }]
        # Default to autothresholds
        return [
            {"autoThreshold": {"bound": {"boundType": "LOWER_BOUND_SIMPLE_BOUND_TYPE", "value": -1.0},
                               "modelType": "UNDEFINED_THRESHOLD_MODEL_TYPE"}},
            {"autoThreshold": {"bound": {"boundType": "UPPER_BOUND_SIMPLE_BOUND_TYPE", "value": -1.0},
                               "modelType": "UNDEFINED_THRESHOLD_MODEL_TYPE"}}
        ]

    def _get_existing_metric(self, table, column_name, metric_name, group_by, filters):
        hook = self.get_hook('GET')
        result = hook.run("api/v1/metrics?warehouseIds={warehouse_id}&tableIds={table_id}"
                          .format(warehouse_id=self.warehouse_id,
                                  table_id=table.get("id")),
                          headers={"Accept": "application/json"})
        metrics = result.json()
        for m in metrics:
            if self._is_same_type_metric(m, metric_name, group_by, filters) \
                    and self._is_same_column_metric(m, column_name):
                return m
        return None

    def _is_same_column_metric(self, m, column_name):
        return m["parameters"][0].get("columnName").lower() == column_name.lower()

    def _is_same_type_metric(self, metric, metric_name, group_by, filters):
        keys = ["metricType", "predefinedMetric", "metricName"]
        result = reduce(lambda val, key: val.get(key) if val else None, keys, metric)
        if result is None:
            return False
        both_metrics_freshness = self._is_freshness_metric(result) and self._is_freshness_metric(metric_name)
        same_group_by = metric.get('groupBys', []) == group_by
        same_filters = metric.get('filters', []) == filters
        return result is not None and (result == metric_name or both_metrics_freshness) \
               and same_group_by and same_filters

    def _get_table_for_name(self, schema_name, table_name):
        hook = self.get_hook('GET')
        result = hook.run("dataset/tables/{warehouse_id}/{schema_name}"
                          .format(warehouse_id=self.warehouse_id,
                                  schema_name=schema_name),
                          headers={"Accept": "application/json"})
        tables = result.json()
        for t in tables:
            if t['datasetName'].lower() == table_name.lower():
                return t
        return None

    def _get_notification_channels(self, notifications):
        channels = []
        for n in notifications:
            if n.startswith('#') or n.startswith('@'):
                channels.append({"slackChannel": n})
            elif '@' in n and '.' in n:
                channels.append({"email": n})
        return channels

    def _get_freshness_metric_name_for_field(self, table, column_name):
        for f in table.get("fields"):
            if f.get("fieldName").lower() == column_name.lower():
                if f.get("type") == "TIMESTAMP_LIKE":
                    return "HOURS_SINCE_MAX_TIMESTAMP"
                elif f.get("type") == "DATE_LIKE":
                    return "HOURS_SINCE_MAX_DATE"

    def _get_time_interval_for_delay_string(self, delay_at_update, metric_name, update_schedule):
        split_input = delay_at_update.split(" ", 1)
        interval_value = int(split_input[0])
        interval_type = self._get_proto_interval_type(split_input[1])
        if metric_name == "HOURS_SINCE_MAX_DATE":
            hours_from_cron = self._get_max_hours_from_cron(update_schedule)
            if interval_type == "HOURS_TIME_INTERVAL_TYPE" or interval_type == "MINUTES_TIME_INTERVAL_TYPE":
                logging.warning("Delay granularity for date column must be in days, ignoring value")
                interval_type = "HOURS_TIME_INTERVAL_TYPE"
                interval_value = hours_from_cron
            elif interval_type == "WEEKDAYS_TIME_INTERVAL_TYPE":
                lookback_weekdays = interval_value + 1 if datetime.datetime.now().hour <= hours_from_cron \
                    else interval_value
                logging.info("Weekdays to look back {}".format(lookback_weekdays))
                days_since_last_business_day = self._get_days_since_n_weekdays(datetime.date.today(), lookback_weekdays)
                logging.info("total days to use for delay {}".format(days_since_last_business_day))
                interval_type = "HOURS_TIME_INTERVAL_TYPE"
                interval_value = (days_since_last_business_day + lookback_weekdays) * 24 + hours_from_cron
            elif interval_type == "DAYS_TIME_INTERVAL_TYPE":
                interval_type = "HOURS_TIME_INTERVAL_TYPE"
                interval_value = interval_value * 24 + hours_from_cron
        return {
            "intervalValue": interval_value,
            "intervalType": interval_type
        }

    def _get_days_since_n_weekdays(self, start_date, n):
        days_since_last_business_day = 0
        weekday_ordinal = datetime.date.weekday(start_date - datetime.timedelta(days=n))
        # 5 is Saturday, 6 is Sunday
        if weekday_ordinal >= 5:
            days_since_last_business_day = 2
        return days_since_last_business_day

    def _get_proto_interval_type(self, interval_type):
        if "minute" in interval_type:
            return "MINUTES_TIME_INTERVAL_TYPE"
        elif "hour" in interval_type:
            return "HOURS_TIME_INTERVAL_TYPE"
        elif "weekday" in interval_type:
            return "WEEKDAYS_TIME_INTERVAL_TYPE"
        elif "market day" in interval_type:
            return "MARKET_DAYS_TIME_INTERVAL_TYPE"
        elif "day" in interval_type:
            return "DAYS_TIME_INTERVAL_TYPE"

    def _get_max_hours_from_cron(self, cron):
        cron_values = cron.split(" ")
        hours = cron_values[1]
        if hours == "*":
            return 0
        return int(hours.split(",")[-1])
