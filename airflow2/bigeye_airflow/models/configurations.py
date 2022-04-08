from dataclasses import dataclass, field


def _get_seconds_from_window_size(window_size):
    if window_size == "1 day":
        return 86400
    elif window_size == "1 hour":
        return 3600
    else:
        raise Exception("Can only set window size of '1 hour' or '1 day'")


@dataclass
class CreateMetricConfiguration:
    table_name: str
    schema_name: str
    column_name: str
    metric_name: str
    notifications: list = field(default_factory=lambda: [])
    thresholds: list = field(default_factory=lambda: [])
    filters: list = field(default_factory=lambda: [])
    group_by: list = field(default_factory=lambda: [])
    default_check_frequency_hours: int = 2
    update_schedule = None
    delay_at_update: str = "0 minutes"
    timezone: str = "UTC"
    should_backfill: bool = False
    lookback_type: str = "METRIC_TIME_LOOKBACK_TYPE"
    lookback_days: int = 2
    window_size: str = "1 day"
    window_size_seconds = _get_seconds_from_window_size(window_size)
