from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

from ems_ingest import ingest_2024_data
from ems_clean import clean_ems_data

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'dag_EMS_ingest_clean',
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=['ems', 'ingest', 'clean']
) as dag:

    ingest = PythonOperator(
        task_id='ingest_2024',
        python_callable=ingest_2024_data,
    )

    clean = PythonOperator(
        task_id='clean_2024',
        python_callable=clean_ems_data,
    )

    ingest >> clean