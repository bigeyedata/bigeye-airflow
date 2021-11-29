from airflow.hooks.http_hook import HttpHook


def get_hook(connection_id, method) -> HttpHook:
    return HttpHook(http_conn_id=connection_id, method=method)