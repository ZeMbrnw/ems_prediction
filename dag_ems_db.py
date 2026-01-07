from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
from load_db_ems import load_data

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 10, 12),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
        'dag_EMS_db_load',
        default_args=default_args,
        description='Pushes final EMS predictions to MySQL RDS',
        schedule_interval=None,
        catchup=False
) as dag:
    load_db = PythonOperator(
        task_id='push_to_db',
        python_callable=load_data,
    )

    load_db