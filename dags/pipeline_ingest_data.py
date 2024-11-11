import os 
import sys
import json

from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor

def create_dag(_dag_id, _schedule):
    _default_args = {
        'owner': 'tungnt',
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
        'execution_timeout': timedelta(minutes=15),
        "start_date": datetime.today()
    }

    @dag(
        dag_id=_dag_id,
        schedule=_schedule,
        default_args=_default_args,
        tags=['ingest_pipeline'],
        catchup=False
    )
    def get_dag():
        s3_sensor = S3KeySensor(
            task_id='s3_file_check',
            poke_interval=10,
            timeout=30,
            soft_fail=False,
            retries=2,
            bucket_key='retail_db_json/categories/*',
            bucket_name='raw',
            aws_conn_id='aws_default',
        )

        @task
        def process_data():
            # Dummy processing task
            print("Processing data...")

        s3_sensor >> process_data()
    
    return get_dag()

globals()['ingest_data'] = create_dag('ingest_data', '@daily')
