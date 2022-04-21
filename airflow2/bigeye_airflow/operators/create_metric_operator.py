import logging
from typing import List, Dict

from airflow.models import BaseOperator
from bigeye_sdk.functions.metric_functions import is_freshness_metric, table_has_metric_time
from bigeye_sdk.functions.table_functions import transform_table_list_to_dict
from bigeye_sdk.model.configuration_templates import SimpleUpsertMetricRequest
from bigeye_airflow.airflow_datawatch_client import AirflowDatawatchClient


class CreateMetricOperator(BaseOperator):
    """
    The CreateMetricOperator takes a list of SimpleUpsertMetricRequest objects and instantiates them according to the
    business logic of Bigeye's API.
    """

    def __init__(self,
                 connection_id: str,
                 warehouse_id: int,
                 configuration: List[dict],
                 *args,
                 **kwargs):
        """
        param connection_id: string referencing a defined connection in the Airflow deployment.
        param warehouse_id: int id of the warehouse where the operator will upsert the metrics.
        param configuration: list of metric configurations to upsert.  The dicts passed as a list must conform to the
        dataclass SimpleMetricTemplate.
        param args: not currently supported
        param kwargs: not currently supported
        """

        super(CreateMetricOperator, self).__init__(*args, **kwargs)
        self.connection_id = connection_id
        self.warehouse_id = warehouse_id

        self.configuration = [SimpleUpsertMetricRequest.from_dict(c)
                              for c in configuration]

        self.asset_ix = {}  # Initializing asset_ix
        self.client = AirflowDatawatchClient(connection_id)

    def _get_table_entry_for_name(self, schema_name: str, table_name: str) -> dict:
        """
        param schema_name: name of schema containing table
        param table_name: name of table
        return: table entry as a dictionary
        """
        try:
            return self.asset_ix[schema_name.lower()][table_name.lower()]
        except KeyError:
            logging.error(f'schema: {schema_name}, warehouse: {self.warehouse_id} does not contain table: {table_name}')

    def execute(self, context):
        self.asset_ix = self._build_asset_ix(self.warehouse_id, self.configuration)
        created_metrics_ids: List[int] = []

        # Iterate each configuration
        for c in self.configuration:

            if c.metric_template.metric_name is None:
                raise Exception("Metric name must be present in configuration", c)

            table: dict = self._get_table_entry_for_name(c.schema_name, c.table_name)

            logging.info(table)

            # Validate and replace group column names -- to ameliorate incorrect case.
            c.group_by = [table['fields'][col.lower()]['fieldName'] for col in c.metric_template.group_by]

            if table is None or table.get("id") is None:
                raise Exception("Could not find table: ", c.schema_name, c.table_name)

            c.existing_metric = self.client.get_existing_metric(self.warehouse_id,
                                                                table,
                                                                c.column_name,
                                                                c.metric_template.metric_name,
                                                                c.group_by,
                                                                c.metric_template.filters)

            metric = c.build_upsert_request_object(warehouse_id=self.warehouse_id, table=table)

            should_backfill = False
            if metric.id is None and not is_freshness_metric(c.metric_template.metric_name):
                should_backfill = True

            result = self.client.create_metric(metric_configuration=metric)
            created_metrics_ids.append(result.id)

            logging.info("Create result: %s", result.to_json())
            if should_backfill and result.id is not None and table_has_metric_time(table):
                self.client.backfill_metric(metric_ids=[result.id])

            return created_metrics_ids

    def _build_asset_ix(self, warehouse_id: int, conf: List[SimpleUpsertMetricRequest]) -> dict:
        """
        Builds a case-insensitive, keyable index of assets needed by the CreateMetricConfiguration
        :param warehouse_id: int id of Bigeye warehouse
        :param conf: the CreateMetricConfiguration object
        :return: { <schema_name.lower>: { <table_name.lower>: <transformed_table_entry> }}
        """
        return {sn.lower(): self.__get_asset_ix(warehouse_id, sn)
                for sn in {c.schema_name for c in conf}}

    def __get_asset_ix(self, warehouse_id: int, schema_name: str) -> Dict[str, dict]:
        return transform_table_list_to_dict(
            self.client.get_dataset(warehouse_id, schema_name)
        )
