import logging
from typing import List

from bigeye_sdk.client.datawatch_client import DatawatchClient

from bigeye_sdk.model.configuration_templates import SimpleUpsertMetricRequest

from bigeye_airflow.airflow_datawatch_client import AirflowDatawatchClient
from bigeye_airflow.operators.client_extensible_operator import ClientExtensibleOperator


class CreateMetricOperator(ClientExtensibleOperator):
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

        self.connection_id = connection_id
        self.client = None

    def get_client(self) -> DatawatchClient:
        if not self.client:
            self.client = AirflowDatawatchClient(self.connection_id)
        return self.client

    def execute(self, context):
        created_metrics_ids: List[int] = []

        # Iterate each configuration
        for c in self.configuration:

            r = self.get_client().upsert_metric_from_simple_template(sumr=c, target_warehouse_id=self.warehouse_id)
            created_metrics_ids.append(r)

        return created_metrics_ids

