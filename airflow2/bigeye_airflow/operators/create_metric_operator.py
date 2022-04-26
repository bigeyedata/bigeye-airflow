import logging
from typing import List, Dict

from airflow.models import BaseOperator
from bigeye_sdk.functions.metric_functions import is_freshness_metric, table_has_metric_time
from bigeye_sdk.generated.com.torodata.models.generated import TableList, Table
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

        self.client = AirflowDatawatchClient(connection_id)


    def execute(self, context):
        self.asset_ix = self._build_asset_ix(self.warehouse_id, self.configuration)
        created_metrics_ids: List[int] = []

        # Iterate each configuration
        for c in self.configuration:

            if c.metric_template.metric_name is None:
                raise Exception("Metric name must be present in configuration", c)

            table: Table = self.client.get_tables(warehouse_id=self.warehouse_id, schema=c.schema_name,
                                                  table_name=c.table_name).tables[0]

            if not table:
                raise Exception("Could not find table: ", c.schema_name, c.table_name)

            c.existing_metric = self.client.get_existing_metric(self.warehouse_id,
                                                                table,
                                                                c.column_name,
                                                                c.metric_template.metric_name,
                                                                c.metric_template.group_by,
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
