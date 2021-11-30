import logging
from typing import List

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from bigeye_airflow.functions.metric_functions import build_metric_object, is_freshness_metric, table_has_metric_time
from bigeye_airflow.models.configurations import CreateMetricConfiguration
from bigeye_airflow.bigeye_requests.catalog_requests import get_asset_ix
from bigeye_airflow.bigeye_requests.metric_requests import get_existing_metric, upsert_metric, backfill_metric

def case_sensitive_field_name(table: dict, inbound_field_name: str) -> str:
    for f in table['fields']:
        if f['fieldName'].lower() == inbound_field_name.lower():
            return f['fieldName']


class CreateMetricOperator(BaseOperator):

class CreateMetricOperator(BaseOperator):
    """
    The Create Metric Operator takes a list of CreateMetricConfiguration objects and instantiates them according to the
    business logic of Bigeye's API.
    """

    @apply_defaults
    def __init__(self,
                 connection_id: str,
                 warehouse_id: int,
                 configuration: list(dict(schema_name=None, table_name=None, column_name=None,
                                          update_schedule=None, metric_name=None, group_by=None,
                                          extras=...)),
                 *args,
                 **kwargs):
        """
        :param connection_id: string referencing a defined connection in the Airflow deployment.
        :param warehouse_id: int id of the warehouse where the the operator will upsert the metrics.
        :param configuration: list of metric configurations to upsert.  The dicts passed as a list must conform to the
        dataclass CreateMetricConfiguration.
        :param args: not currently supported
        :param kwargs: not currently supported
        """

        super(CreateMetricOperator, self).__init__(*args, **kwargs)
        self.connection_id = connection_id
        self.warehouse_id = warehouse_id

        self.configuration = [CreateMetricConfiguration(**c) for c in
                              configuration]

        self.asset_ix = {}  # Initializing asset_ix

    def _get_table_entry_for_name(self, schema_name: str, table_name: str) -> dict:
        """
        :param schema_name: name of schema containing table
        :param table_name: name of table
        :return: table entry as a dictionary
        """
        try:
            return self.asset_ix[schema_name.lower()][table_name.lower()]
        except KeyError as ex:
            logging.error(f'schema: {schema_name}, warehouse: {self.warehouse_id} does not contain table: {table_name}')

    def execute(self, context):
        self.asset_ix = get_asset_ix(self.connection_id,
                                     self.warehouse_id,
                                     self.configuration
                                     )

        # Iterate each configuration
        for c in self.configuration:
            # Set attributes:
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

            if metric_name is None:
                raise Exception("Metric name must be present in configuration", c)

            table = self._get_table_for_name(schema_name, table_name)

            if isinstance(c['group_by'], list):
                group_by = [case_sensitive_field_name(table, c) for c in c['group_by']]
            elif c['group_by'] is None:
                group_by = []
            else:
                raise Exception(f'Configuration group_by element must be a list or None.\n'
                                f'Value: {c["group_by"]}.\n'
                                f'Type: {type(c["group_by"])}')

            if table is None or table.get("id") is None:
                raise Exception("Could not find table: ", c.schema_name, c.table_name)

            existing_metric = get_existing_metric(self.connection_id, self.warehouse_id, table, c.column_name, c.metric_name, c.group_by)

            metric = build_metric_object(warehouse_id=self.warehouse_id
                                         , existing_metric=existing_metric
                                         , table=table
                                         , conf=c)

            if metric.get("id") is None and not is_freshness_metric(c.metric_name):
                c.should_backfill = True

            result = upsert_metric(self.connection_id, metric)

            logging.info("Create result: %s", result.json())
            if should_backfill and result.json().get("id") is not None:
                bigeye_post_hook.run("/api/v1/metrics/backfill",
                                     headers={"Content-Type": "application/json", "Accept": "application/json"},
                                     data=json.dumps({"metricIds": [result.json()["id"]]}))

    def get_hook(self, method) -> HttpHook:
        return HttpHook(http_conn_id=self.connection_id, method=method)

    def _get_metric_object(self, existing_metric, table, notifications, column_name,
                           update_schedule, delay_at_update, timezone, default_check_frequency_hours, metric_name,
                           group_by=[]):
        if self._is_freshness_metric(metric_name):
            metric_name = self._get_freshness_metric_name_for_field(table, column_name)
            if update_schedule is None:
                raise Exception("Update schedule can not be null for freshness schedule thresholds")
        metric = {
            "scheduleFrequency": {
                "intervalType": "HOURS_TIME_INTERVAL_TYPE",
                "intervalValue": default_check_frequency_hours
            },
            "thresholds": self._get_thresholds_for_metric(metric_name, timezone, delay_at_update, update_schedule),
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
                "intervalValue": 1
            },
            # 24 hour window mexis metric
            "lookbackType": "METRIC_TIME_LOOKBACK_TYPE",
            "grainSeconds": 86400,
            "notificationChannels": self._get_notification_channels(notifications),
            "groupBys": group_by,
        }
        if existing_metric is None:
            return metric
        else:
            existing_metric["thresholds"] = metric["thresholds"]
            existing_metric["notificationChannels"] = metric.get("notificationChannels", [])
            existing_metric["scheduleFrequency"] = metric["scheduleFrequency"]
            return existing_metric

    def _is_freshness_metric(self, metric_name):
        return "HOURS_SINCE_MAX" in metric_name

    def _get_thresholds_for_metric(self, metric_name, timezone, delay_at_update, update_schedule):
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

    def _get_existing_metric(self, table, column_name, metric_name):
        hook = self.get_hook('GET')
        result = hook.run("api/v1/metrics?warehouseIds={warehouse_id}&tableIds={table_id}"
                          .format(warehouse_id=self.warehouse_id,
                                  table_id=table.get("id")),
                          headers={"Accept": "application/json"})
        metrics = result.json()
        for m in metrics:
            if self._is_same_type_metric(m, metric_name) and self._is_same_column_metric(m, column_name):
                return m
        return None

    def _is_same_column_metric(self, m, column_name):
        return m["parameters"][0].get("columnName").lower() == column_name.lower()

    def _is_same_type_metric(self, metric, metric_name):
        keys = ["metricType", "predefinedMetric", "metricName"]
        result = reduce(lambda val, key: val.get(key) if val else None, keys, metric)
        return result is not None and result == metric_name

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
        split_input = delay_at_update.split(" ")
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
                logging.info("Weekdays to look back ", lookback_weekdays)
                days_since_last_business_day = self._get_days_since_n_weekdays(datetime.date.today(), lookback_weekdays)
                logging.info("total days to use for delay ", days_since_last_business_day)
                interval_type = "HOURS_TIME_INTERVAL_TYPE"
                interval_value = (days_since_last_business_day + lookback_weekdays) * 24 + hours_from_cron
            else:
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
        elif "day" in interval_type:
            return "DAYS_TIME_INTERVAL_TYPE"

    def _get_max_hours_from_cron(self, cron):
        cron_values = cron.split(" ")
        hours = cron_values[1]
        if hours == "*":
            return 0
        return int(hours.split(",")[-1])
