import logging
from typing import List

from airflow.models import BaseOperator
from bigeye_sdk.generated.com.torodata.models.generated import Table, MetricConfiguration, \
    MetricInfo, MetricRunStatus

from airflow2.airflow_datawatch_client import AirflowDatawatchClient


class RunMetricsOperator(BaseOperator):

    def __init__(self,
                 connection_id,
                 warehouse_id,
                 schema_name,
                 table_name,
                 metric_ids=None,
                 *args,
                 **kwargs):
        super(RunMetricsOperator, self).__init__(*args, **kwargs)
        self.connection_id = connection_id
        self.warehouse_id = warehouse_id
        self.schema_name = schema_name
        self.table_name = table_name
        self.metric_ids = metric_ids
        self.client = AirflowDatawatchClient(connection_id)

    def execute(self, context):

        metric_ids_to_run = self._set_metric_ids_to_run()

        num_failing_metrics = 0
        success_and_failures = {}
        logging.debug("Running metric IDs: %s", metric_ids_to_run)
        metric_infos: List[MetricInfo] = self.client.run_metric_batch(metric_ids=metric_ids_to_run).metric_infos
        for mi in metric_infos:
            if mi.status is not MetricRunStatus.METRIC_RUN_STATUS_OK:
                logging.error(f"Metric is not OK: {mi.metric_configuration.name}")
                logging.error(f"Metric result: {mi.metric_configuration}")
                success_and_failures["failure"] = mi
                num_failing_metrics += 1
            else:
                success_and_failures["success"] = mi
        # TODO: We shouldn't kill the pipeline because of errors. The user should be able to handle how they choose.
        # if num_failing_metrics > 0:
        #     error_message = "There are {num_failing} failing metrics; see logs for more details"
        #     raise ValueError(error_message.format(num_failing=num_failing_metrics))

        return success_and_failures

    def _get_table_for_name(self, schema_name, table_name) -> Table:
        tables = self.client.get_tables(warehouse_id=self.warehouse_id,
                                        schemas=[schema_name],
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
