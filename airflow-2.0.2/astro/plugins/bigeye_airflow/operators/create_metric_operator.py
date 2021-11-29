import logging
from typing import List

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from bigeye_airflow.functions.metric_functions import build_metric_object, is_freshness_metric, table_has_metric_time
from bigeye_airflow.models.configurations import CreateMetricConfiguration
from bigeye_airflow.bigeye_requests.catalog_requests import get_asset_ix
from bigeye_airflow.bigeye_requests.metric_requests import get_existing_metric, upsert_metric, backfill_metric


class CreateMetricOperator(BaseOperator):
    """
    The Create Metric Operator takes a list of CreateMetricConfiguration objects and instantiates them according to the
    business logic of Bigeye's API.
    """

    @apply_defaults
    def __init__(self,
                 connection_id: str,
                 warehouse_id: int,
                 configuration: List[dict],
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

            if c.metric_name is None:
                raise Exception("Metric name must be present in configuration", c)

            table: dict = self._get_table_entry_for_name(c.schema_name, c.table_name)

            logging.info(table)

            # Validate and replace group column names -- to ameliorate incorrect case.
            c.group_by = [table['fields'][col.lower()]['fieldName'] for col in c.group_by]

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
            if c.should_backfill and result.json().get("id") is not None and table_has_metric_time(table):
                backfill_metric(self.connection_id, [result.json()["id"]])
