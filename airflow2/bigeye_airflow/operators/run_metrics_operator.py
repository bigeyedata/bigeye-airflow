import logging
from typing import List, Optional

from airflow.models import BaseOperator
from bigeye_sdk.generated.com.torodata.models.generated import Table, MetricConfiguration, \
    MetricInfo, MetricRunStatus

from bigeye_airflow.airflow_datawatch_client import AirflowDatawatchClient


class RunMetricsOperator(BaseOperator):
    """
        The RunMetricsOperator will run metrics in Bigeye based on the following:
        1. All metrics for a given table, by providing warehouse ID, schema name and table name.
        2. Any or all metrics, given a list of metric IDs.
        Currently, if a list of metric IDs is provided these will be run instead of metrics provided for
        warehouse_id, schema_name, table_name.
    """

    template_fields = ["metric_ids"]

    def __init__(self,
                 connection_id: str,
                 warehouse_id: Optional[int] = None,
                 schema_name: Optional[str] = None,
                 table_name: Optional[str] = None,
                 metric_ids: Optional[List[int]] = None,
                 *args,
                 **kwargs):
        """
                param connection_id: str referencing a defined connection in the Airflow deployment.
                param warehouse_id: Optional[int] id of the warehouse where the operator will run the metrics.
                param schema_name: Optional[str] name of the schema where the table resides.
                param table_name: Optional[str] name of the table to run all metrics.
                param metric_ids: Optional[List[int]] list of metric IDs to run.
                param args: not currently supported
                param kwargs: not currently supported
        """
        super(RunMetricsOperator, self).__init__(*args, **kwargs)
        self.connection_id = connection_id
        self.warehouse_id = warehouse_id
        self.schema_name = schema_name
        self.table_name = table_name
        self.metric_ids = metric_ids
        self.client = AirflowDatawatchClient(connection_id)

    def execute(self, context):

        metric_ids_to_run = self._set_metric_ids_to_run()
        return self._run_metrics(metric_ids_to_run)

    def _get_table_for_name(self, schema_name, table_name) -> Table:
        tables = self.client.get_tables(warehouse_id=[self.warehouse_id],
                                        schema=[schema_name],
                                        table_name=[table_name]).tables

        if not tables:
            raise Exception(f"Could not find table: {self.table_name} in {self.schema_name}")
        else:
            return tables.pop()

    def _set_metric_ids_to_run(self) -> List[int]:
        if self.metric_ids is None:
            table = self._get_table_for_name(self.schema_name, self.table_name)
            metrics: List[MetricConfiguration] = self.client.search_metric_configuration(
                warehouse_ids=[table.warehouse_id],
                table_ids=[table.id]).metrics

            return [m.id for m in metrics]
        else:
            return self.metric_ids

    def _run_metrics(self, metric_ids_to_run: List[int]) -> dict:
        success: List[str] = []
        failure: List[str] = []
        logging.debug("Running metric IDs: %s", metric_ids_to_run)
        metric_infos: List[MetricInfo] = self.client.run_metric_batch(metric_ids=metric_ids_to_run).metric_infos
        num_failing_metrics = 0
        for mi in metric_infos:
            if mi.status is not MetricRunStatus.METRIC_RUN_STATUS_OK:
                logging.error(f"Metric is not OK: {mi.metric_configuration.name}")
                logging.error(f"Metric result: {mi.metric_configuration}")
                failure.append(mi.to_json())
                num_failing_metrics += 1
            else:
                success.append(mi.to_json())

        return {"success": success, "failure": failure}
