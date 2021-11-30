import json
from typing import List

from airflow2.bigeye_airflow.functions.metric_functions import is_same_type_metric, is_same_column_metric
import logging

from airflow2.bigeye_airflow.bigeye_requests.http_hook import get_hook


def get_existing_metric(connection_id: str, warehouse_id: int, table: dict, column_name: str, metric_name: str,
                        group_by: List[str]):
    hook = get_hook(connection_id, 'GET')
    result = hook.run("api/v1/metrics?warehouseIds={warehouse_id}&tableIds={table_id}"
                      .format(warehouse_id=warehouse_id,
                              table_id=table.get("id")),
                      headers={"Accept": "application/json"})
    metrics = result.json()
    for m in metrics:
        if is_same_type_metric(m, metric_name, group_by) and is_same_column_metric(m, column_name):
            return m
    return None


def upsert_metric(connection_id: str, metric: str):
    logging.info("Sending metric to create: %s", metric)

    bigeye_post_hook = get_hook(connection_id, 'POST')

    result = bigeye_post_hook.run("api/v1/metrics",
                                  headers={"Content-Type": "application/json", "Accept": "application/json"},
                                  data=json.dumps(metric))
    if result.status_code == 200:
        logging.info("Create metric status: %s", result.status_code)
    else:
        logging.error("Create metric status: %s", result.status_code)

    return result


def backfill_metric(connection_id: str, metric_ids: List[int]):
    bigeye_post_hook = get_hook(connection_id, 'POST')
    bigeye_post_hook.run("api/v1/metrics/backfill",
                         headers={"Content-Type": "application/json", "Accept": "application/json"},
                         data=json.dumps({"metricIds": metric_ids}))