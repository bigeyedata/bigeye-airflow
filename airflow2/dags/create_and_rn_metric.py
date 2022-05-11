from airflow import DAG
from datetime import datetime
import json
from bigeye_airflow.operators.create_metric_operator import CreateMetricOperator
from bigeye_airflow.operators.run_metrics_operator import RunMetricsOperator
with DAG(
        'create_and_run_metrics',
        schedule_interval=None,
        start_date=datetime.now(),
        catchup=False,
) as dag:
    create_metric = CreateMetricOperator(
        task_id='create_metrics',
        connection_id='bigeye_prod',
        warehouse_id=560,
        configuration=[
            {"schema_name": "GREENE_HOMES_DEMO_PROD.STAGE",
             "table_name": "CONTRACT_STATUS",
             "column_name": "STATUS_ID",
             "user_defined_metric_name": "Status ID Null Count",
             "metric_name": "COUNT_NULL",
             "default_check_frequency_hours": 6
             }
        ],
        dag=dag
    )
    run_metric = RunMetricsOperator(
        task_id='run_metrics',
        connection_id='bigeye_prod',
        warehouse_id=0,
        schema_name=None,
        table_name=None,
        metric_ids=create_metric.output,
        dag=dag)
