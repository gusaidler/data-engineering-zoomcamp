import os
from airflow import DAG
from datetime import datetime

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from ingest_script import ingest_callable


AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

PG_HOST = os.getenv('PG_HOST')
PG_USER = os.getenv('PG_USER')
PG_PASSWORD = os.getenv('PG_PASSWORD')
PG_PORT = os.getenv('PG_PORT')
PG_DATABASE = os.getenv('PG_DATABASE')


dataset_file = "yellow_tripdata_{{ execution_date.strftime('%Y-%m') }}.parquet"
dataset_url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{dataset_file}"
output_file = AIRFLOW_HOME + "/output_{{ execution_date.strftime('%Y-%m') }}.parquet"

table_name = "yellow_taxi_{{ execution_date.strftime('%Y_%m') }}"

default_args = {
    "owner": "airflow",
    "start_date": datetime(2021, 1, 1),
    "depends_on_past": False,
    "retries": 1,
}


local_workflow = DAG(
    dag_id="LocalIngestionDag",
    schedule_interval='0 6 2 * *',
    default_args=default_args
)

with local_workflow:

    wget_task = BashOperator(
        task_id='wget',
        bash_command=f'curl -sSL {dataset_url} > {output_file}'
    )


    ingest_task = PythonOperator(
        task_id='ingest',
        python_callable=ingest_callable,
        op_kwargs=dict(
            user=PG_USER,
            password=PG_PASSWORD,
            host=PG_HOST,
            port=PG_PORT,
            db=PG_DATABASE,
            table_name=table_name,
            parquet_file=output_file
        )
    )

    
    wget_task >> ingest_task