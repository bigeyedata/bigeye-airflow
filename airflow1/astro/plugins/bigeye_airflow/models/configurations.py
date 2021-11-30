def _get_seconds_from_window_size(window_size):
    if window_size == "1 day":
        return 86400
    elif window_size == "1 hour":
        return 3600
    else:
        raise Exception("Can only set window size of '1 hour' or '1 day'")


class CreateMetricConfiguration:

    def __init__(self,
                 table_name: str,
                 schema_name: str,
                 column_name: str,
                 metric_name: str,
                 notifications: list = None,
                 thresholds: list = None,
                 filters: list = None,
                 group_by: list = None,
                 default_check_frequency_hours: int = 2,
                 update_schedule: str = None,
                 delay_at_update: str = "0 minutes",
                 timezone: str = "UTC",
                 should_backfill: bool = False,
                 lookback_type: str = "METRIC_TIME_LOOKBACK_TYPE",
                 lookback_days: int = 2,
                 window_size: str = "1 day"
                 ) -> None:
        self.table_name: str = table_name
        self.schema_name: str = schema_name
        self.column_name: str = column_name
        self.metric_name: str = metric_name
        self.notifications: list = notifications if notifications else []
        self.thresholds: list = thresholds if thresholds else []
        self.filters: list = filters if filters else []
        self.group_by: list = group_by if group_by else []
        self.default_check_frequency_hours: int = default_check_frequency_hours
        self.update_schedule: str = update_schedule
        self.delay_at_update: str = delay_at_update
        self.timezone: str = timezone
        self.should_backfill: bool = should_backfill
        self.lookback_type: str = lookback_type
        self.lookback_days: int = lookback_days
        self.window_size: str = window_size

        self.window_size_seconds = _get_seconds_from_window_size(window_size)
