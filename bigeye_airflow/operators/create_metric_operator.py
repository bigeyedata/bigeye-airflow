import logging
from typing import List

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from bigeye_airflow.functions.metric_functions import build_metric_object, is_freshness_metric, table_has_metric_time
from bigeye_airflow.models.configurations import CreateMetricConfiguration
from bigeye_airflow.requests.catalog_requests import get_asset_ix
from bigeye_airflow.requests.metric_requests import get_existing_metric, upsert_metric, backfill_metric


class CreateMetricOperator(BaseOperator):
    # TODO: Ryan R. -- Think I solved some of this with the CreateMetricConfiguration class.  Will chat up Egor.
    # Only for Python 3.8+
    # TODO - find a way to check what Python version is running
    # class FreshnessConfig(TypedDict, total=False):
    #     schema_name: str
    #     table_name: str
    #     column_name: str
    #     hours_between_update: int
    #     hours_delay_at_update: int
    #     notifications: List[str]
    #     default_check_frequency_hours: int

    @apply_defaults
    def __init__(self,
                 connection_id: str,
                 warehouse_id: int,
                 configuration: dict,
                 *args,
                 **kwargs):
        """
        :param connection_id: string containing basic auth TODO: research auth here.
        :param warehouse_id: int id of the warehouse where the the operator will upsert the metrics.
        :param configuration: list of metric configurations to upsert
        :param args: not currently supported TODO: are we using?
        :param kwargs: not currently supported TODO: are we using?
        """
        super(CreateMetricOperator, self).__init__(*args, **kwargs)
        self.connection_id = connection_id
        self.warehouse_id = warehouse_id
        self.configuration: List[CreateMetricConfiguration] = [CreateMetricConfiguration(**c) for c in configuration]

        # Dictionary to reduce round trips to the API
        self.asset_ix = get_asset_ix(self.connection_id, self.warehouse_id, self.configuration)

    def _get_table_entry_for_name(self, schema_name: str, table_name: str) -> dict:
        """
        :param schema_name: name of schema containing table
        :param table_name: name of table
        :return: table entry as a dictionary
        """
        return self.asset_ix[schema_name.lower()][table_name.lower]

    def execute(self, context):
        # Iterate each configuration
        for c in self.configuration:

            if c.metric_name is None:
                raise Exception("Metric name must be present in configuration", c)

            table: dict = self._get_table_entry_for_name(c.schema_name, c.table_name)

            # Validate and replace group column names -- to ameliorate incorrect case.
            c.group_by = [table['fields'][col.lower()]['fieldName'] for col in c.group_by]

            if table is None or table.get("id") is None:
                raise Exception("Could not find table: ", c.schema_name, c.table_name)

            existing_metric = get_existing_metric(table, c.column_name, c.metric_name, c.group_by)

            metric = build_metric_object(existing_metric, table, c)

            if metric.get("id") is None and not is_freshness_metric(c.metric_name):
                c.should_backfill = True

            result = upsert_metric(metric)

            logging.info("Create result: %s", result.json())
            if c.should_backfill and result.json().get("id") is not None and table_has_metric_time(table):
                backfill_metric([result.json()["id"]])
