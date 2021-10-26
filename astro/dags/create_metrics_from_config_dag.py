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

with DAG('covid_to_redshift',
         start_date=datetime(2020, 8, 31, 0, 0, 0),
         max_active_runs=1,
         schedule_interval="@once",
         default_args=default_args,
         catchup=False
         ) as dag:
    create_metrics_from_config = CreateMetricOperator(
        task_id='create_metrics',
        connection_id='bigeye_connection',
        warehouse_id=374,
        metric_configs=[
            {"schema_name": "MACRO_ECON.CRUX_GENERAL",
             "table_name": "CX13540--DOMESTIC_DEMAND_FORECAST--AQKkwVKlhBkHtUwu52HdmQl6Gw",
             "column_name": "Value",
             "metric_name": "MIN",
             "default_check_frequency_hours": 6,
             "filters": ["\"LOCATION_Code\" = 'LUX'", "\"SUBJECT_Code\" = 'TOT'"],
             "group_by": ["Country"]
             }
        ],
        dag=dag
    )
