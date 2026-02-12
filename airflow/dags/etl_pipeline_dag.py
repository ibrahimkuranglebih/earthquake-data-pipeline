from airflow import DAG
import pendulum
import os
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from extract.fetch_data import fetch_earthquake_data
from transform.transform_data import transform_data
from load.load_to_postgres import load_to_postgres

local_time = pendulum.timezone("Asia/Jakarta")
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2026, 1, 4),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
with DAG(
    dag_id = "etl_earthquake_data_pipeline",
    start_date=pendulum.datetime(2026, 1, 4, tz=local_time),
    schedule="0 8,20 * * *",
    default_args=default_args,
    catchup=False,
    tags=['football', 'pipeline','dbt','elt']
) as dag :
    extract_task = PythonOperator(
        task_id = "extract_data_earthquake",
        python_callable = fetch_earthquake_data
    )
    
    transform_task = PythonOperator(
        task_id = "transform_data_earthquake",
        python_callable = transform_data
    )
    
    load_task = PythonOperator(
        task_id = "load_data_earthquake",
        python_callable = load_to_postgres
    )
    
    extract_task >> transform_task >> load_task
    