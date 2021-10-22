def _get_seconds_from_window_size(window_size):
    if window_size == "1 day":
        return 86400
    elif window_size == "1 hour":
        return 3600
    else:
        raise Exception("Can only set window size of '1 hour' or '1 day'")