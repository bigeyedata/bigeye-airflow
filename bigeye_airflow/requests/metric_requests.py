import json
from typing import List

from bigeye_airflow.functions.metric_functions import is_same_type_metric, is_same_column_metric, \
    is_freshness_metric, _table_has_metric_time
from bigeye_airflow.requests import get_hook
import logging


def get_existing_metric(connection_id: str, warehouse_id: int, table: dict, column_name: str, metric_name: str,
                        group_by):
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


def upsert_metric(metric: str):
    logging.info("Sending metric to create: %s", metric)

    bigeye_post_hook = get_hook('POST')

    result = bigeye_post_hook.run("api/v1/metrics",
                                  headers={"Content-Type": "application/json", "Accept": "application/json"},
                                  data=json.dumps(metric))
    if result.status_code == 200:
        logging.info("Create metric status: %s", result.status_code)
    else:
        logging.error("Create metric status: %s", result.status_code)

    return result


def backfill_metric(metric_ids: List[int]):
    bigeye_post_hook = get_hook('POST')
    bigeye_post_hook.run("api/v1/metrics/backfill",
                         headers={"Content-Type": "application/json", "Accept": "application/json"},
                         data=json.dumps({"metricIds": metric_ids}))