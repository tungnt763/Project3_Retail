import os
import csv
import io
from airflow.decorators import task, task_group
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from psycopg2.extras import execute_values
from datetime import datetime

@task_group(group_id="landing_layer")
def landing_layer(**kwargs):

    _my_table_name = f'dim_{kwargs.get("table_name")}_ld'
    _my_dataset = kwargs.get('landing_dataset')

    # Define the S3 path for the CSV file
    _bucket_name = kwargs.get('bucket_name')
    _bucket_key = kwargs.get('bucket_key')

    _sql_template = os.path.join(kwargs.get('template_root_path'), '1_landing', 'dim_tables_cmn_ld.sql')
    _columns = list(kwargs.get('columns_detail').keys())

    _prms_schema_columns = []
    for col in _columns:
        _prms_schema_columns.append(f'{col} VARCHAR')

    @task(task_id=f'create_{_my_table_name}')
    def create_table():
        
        s3_hook = S3Hook(aws_conn_id='aws_default')
        postgres_hook = PostgresHook(postgres_conn_id='postgres')

        # Get the file content from MinIO (S3)
        file_obj = s3_hook.get_key(key=_bucket_key, bucket_name=_bucket_name)
        file_content = file_obj.get()['Body'].read().decode('utf-8')
        
        # Read the CSV data
        _csv_data = csv.reader(io.StringIO(file_content))
        
        with open(_sql_template, 'r') as f:
            _sql_query_template = f.read()
        
        _sql_query = _sql_query_template.format(
            my_dataset=_my_dataset,
            my_table_name=_my_table_name,
            schema_columns=',\n\t'.join(_prms_schema_columns),
            columns=', '.join(_columns)       
        )

        print("Sql: ", _sql_query)

        truncate_sql = f"""
        TRUNCATE TABLE {_my_dataset}.{_my_table_name};
        """

        # Prepare the PostgreSQL connection
        with postgres_hook.get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(truncate_sql)
                execute_values(cursor, _sql_query, _csv_data)
                conn.commit()
        
    create_table()
    