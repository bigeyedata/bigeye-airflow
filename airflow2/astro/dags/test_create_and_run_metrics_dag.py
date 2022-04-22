from datetime import datetime, timedelta

from airflow import DAG
from bigeye_airflow.operators.create_metric_operator import CreateMetricOperator
from bigeye_airflow.operators.run_metrics_operator import RunMetricsOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG('test_create_and_run_metric_dag',
         start_date=datetime(2020, 8, 31, 0, 0, 0),
         max_active_runs=1,
         schedule_interval="@once",
         default_args=default_args,
         catchup=False
         ) as dag:
    created_metrics_results = CreateMetricOperator(
        task_id='create_metrics',
        connection_id='bigeye_connection',
        warehouse_id=560,
        configuration=[
            {"schema_name": "GREENE_HOMES_DEMO_PROD.STAGE",
             "table_name": "CONTRACT",
             "column_name": "CONTRACT_PRICE",
             "metric_name": "MAX",
             "default_check_frequency_hours": 6
             },
            {"schema_name": "GREENE_HOMES_DEMO_PROD.STAGE",
             "table_name": "CONTRACT",
             "column_name": "SALES_MANAGER_ID",
             "metric_name": "COUNT_NULL",
             "default_check_frequency_hours": 6
             }
        ],
        dag=dag
    )
    run_metric_results = RunMetricsOperator(task_id='run_metrics', connection_id='bigeye_connection', metric_ids=)
