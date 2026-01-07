from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

from transform_ems import transform_ems_data
from features_ems import extract_features_ems
from build_train_ems import build_train_model_ems

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'dag_EMS_transform_train',
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=['ems', 'transform', 'train']
) as dag:

    transform = PythonOperator(
        task_id='transform_2024',
        python_callable=transform_ems_data,
    )

    feature_extraction = PythonOperator(
        task_id='feature_extraction_2024',
        python_callable=extract_features_ems,
    )

    build_train = PythonOperator(
        task_id='build_train_model_2024',
        python_callable=build_train_model_ems,
    )

    transform >> feature_extraction >> build_train