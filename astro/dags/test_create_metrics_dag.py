from datetime import datetime, timedelta

from airflow import DAG
from bigeye_airflow.operators.create_metric_operator import CreateMetricOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG('test_create_metric_dag',
         start_date=datetime(2020, 8, 31, 0, 0, 0),
         max_active_runs=1,
         schedule_interval="@once",
         default_args=default_args,
         catchup=False
         ) as dag:
    create_metrics_from_config = CreateMetricOperator(
        task_id='create_metrics',
        connection_id='bigeye_connection',
        warehouse_id=516,
        configuration=[
            {"schema_name": "BIGEYE_DEMO.DEMO",
             "table_name": "PAYMENTS",
             "column_name": "MERCHANT_CATEGORY",
             "metric_name": "COUNT_DISTINCT",
             "default_check_frequency_hours": 6,
             "group_by": ['MERCHANT_CATEGORY']
             }
        ],
        dag=dag
    )
