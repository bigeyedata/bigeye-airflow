import datetime
import logging
from functools import reduce

from bigeye_airflow.models.configurations import CreateMetricConfiguration


def table_has_metric_time(table: dict):
    for field_key, field in table["fields"].items():
        if field["loadedDateField"]:
            return True
    return False


def is_freshness_metric(metric_name: str) -> bool:
    return "HOURS_SINCE_MAX" in metric_name


def is_same_type_metric(metric, metric_name, group_by):
    keys = ["metricType", "predefinedMetric", "metricName"]
    result = reduce(lambda val, key: val.get(key) if val else None, keys, metric)
    if result is None:
        return False
    both_metrics_freshness = is_freshness_metric(result) and is_freshness_metric(metric_name)
    same_group_by = metric.get('groupBys', []) == group_by
    return result is not None and (result == metric_name or both_metrics_freshness) and same_group_by


def is_same_column_metric(m, column_name):
    return m["parameters"][0].get("columnName").lower() == column_name.lower()


def get_proto_interval_type(interval_type):
    if "minute" in interval_type:
        return "MINUTES_TIME_INTERVAL_TYPE"
    elif "hour" in interval_type:
        return "HOURS_TIME_INTERVAL_TYPE"
    elif "weekday" in interval_type:
        return "WEEKDAYS_TIME_INTERVAL_TYPE"
    elif "day" in interval_type:
        return "DAYS_TIME_INTERVAL_TYPE"


def get_max_hours_from_cron(cron):
    cron_values = cron.split(" ")
    hours = cron_values[1]
    if hours == "*":
        return 0
    return int(hours.split(",")[-1])


def get_days_since_n_weekdays(start_date, n):
    days_since_last_business_day = 0
    weekday_ordinal = datetime.date.weekday(start_date - datetime.timedelta(days=n))
    # 5 is Saturday, 6 is Sunday
    if weekday_ordinal >= 5:
        days_since_last_business_day = 2
    return days_since_last_business_day


def get_notification_channels(notifications):
    channels = []
    for n in notifications:
        if n.startswith('#') or n.startswith('@'):
            channels.append({"slackChannel": n})
        elif '@' in n and '.' in n:
            channels.append({"email": n})
    return channels


def get_freshness_metric_name_for_field(table, column_name):
    for fk, f in table["fields"].items():
        if f.get("fieldName").lower() == column_name.lower():
            if f.get("type") == "TIMESTAMP_LIKE":
                return "HOURS_SINCE_MAX_TIMESTAMP"
            elif f.get("type") == "DATE_LIKE":
                return "HOURS_SINCE_MAX_DATE"


def get_thresholds_for_metric(metric_name, timezone, delay_at_update, update_schedule, thresholds):
    if thresholds:
        return thresholds
    # Current path for freshness
    if is_freshness_metric(metric_name):
        return [{
            "freshnessScheduleThreshold": {
                "bound": {
                    "boundType": "UPPER_BOUND_SIMPLE_BOUND_TYPE",
                    "value": -1
                },
                "cron": update_schedule,
                "timezone": timezone,
                "delayAtUpdate": get_time_interval_for_delay_string(delay_at_update,
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


def get_time_interval_for_delay_string(delay_at_update, metric_name, update_schedule):
    split_input = delay_at_update.split(" ")
    interval_value = int(split_input[0])
    interval_type = get_proto_interval_type(split_input[1])
    if metric_name == "HOURS_SINCE_MAX_DATE":
        hours_from_cron = get_max_hours_from_cron(update_schedule)
        if interval_type == "HOURS_TIME_INTERVAL_TYPE" or interval_type == "MINUTES_TIME_INTERVAL_TYPE":
            logging.warning("Delay granularity for date column must be in days, ignoring value")
            interval_type = "HOURS_TIME_INTERVAL_TYPE"
            interval_value = hours_from_cron
        elif interval_type == "WEEKDAYS_TIME_INTERVAL_TYPE":
            lookback_weekdays = interval_value + 1 if datetime.datetime.now().hour <= hours_from_cron \
                else interval_value
            logging.info("Weekdays to look back {}".format(lookback_weekdays))
            days_since_last_business_day = get_days_since_n_weekdays(datetime.date.today(), lookback_weekdays)
            logging.info("total days to use for delay {}".format(days_since_last_business_day))
            interval_type = "HOURS_TIME_INTERVAL_TYPE"
            interval_value = (days_since_last_business_day + lookback_weekdays) * 24 + hours_from_cron
        else:
            interval_type = "HOURS_TIME_INTERVAL_TYPE"
            interval_value = interval_value * 24 + hours_from_cron
    return {
        "intervalValue": interval_value,
        "intervalType": interval_type
    }


def build_metric_object(warehouse_id: int, existing_metric: dict, table: dict, conf: CreateMetricConfiguration):  # TODO: what if existing metric doesnt exist
    metric_name = conf.metric_name
    ifm = is_freshness_metric(metric_name)

    if ifm:
        metric_name = get_freshness_metric_name_for_field(table, conf.column_name)
        if conf.update_schedule is None:
            raise Exception("Update schedule can not be null for freshness schedule thresholds")

    metric = {
        "scheduleFrequency": {
            "intervalType": "HOURS_TIME_INTERVAL_TYPE",
            "intervalValue": conf.default_check_frequency_hours
        },
        "thresholds": get_thresholds_for_metric(metric_name, conf.timezone, conf.delay_at_update,
                                                conf.update_schedule,
                                                conf.thresholds),
        "warehouseId": warehouse_id,
        "datasetId": table.get("id"),
        "metricType": {
            "predefinedMetric": {
                "metricName": metric_name
            }
        },
        "parameters": [
            {
                "key": "arg1",
                "columnName": conf.column_name
            }
        ],
        "lookback": {
            "intervalType": "DAYS_TIME_INTERVAL_TYPE",
            "intervalValue": conf.lookback_days
        },
        "notificationChannels": get_notification_channels(conf.notifications),
        "filters": conf.filters,
        "groupBys": conf.group_by,
    }

    table_has_metric_time = False

    if not ifm:  # TODO: Look into repeated logic.
        for field_key, field in table["fields"].items():
            if field["loadedDateField"]:
                table_has_metric_time = True

    if table_has_metric_time:
        metric["lookbackType"] = conf.lookback_type
        if conf.lookback_type == "METRIC_TIME_LOOKBACK_TYPE":
            metric["grainSeconds"] = conf.window_size_seconds

    if existing_metric is None:
        return metric
    else:
        existing_metric["thresholds"] = metric["thresholds"]
        existing_metric["notificationChannels"] = metric.get("notificationChannels", [])
        existing_metric["scheduleFrequency"] = metric["scheduleFrequency"]
        if not ifm and table_has_metric_time:
            existing_metric["lookbackType"] = metric["lookbackType"]
            existing_metric["lookback"] = metric["lookback"]
            existing_metric["grainSeconds"] = metric["grainSeconds"]
        return existing_metric
