import os
import csv
import io
from airflow.decorators import task, task_group
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from psycopg2.extras import execute_values
from datetime import datetime

@task_group(group_id="landing_layer")
def landing_layer(**kwargs):

    _my_table_name = f'dim_{kwargs.get("table_name")}_ld'
    _my_project = kwargs.get('project')
    _my_dataset = kwargs.get('landing_dataset')

    _columns = list(kwargs.get('columns_detail').keys())
    _sql_template = os.path.join('resources', 'sql_template', '1_landing', 'dim_tables_cmn_ld.sql')

    _prms_schema_columns = []

    for col in _columns:
        _prms_schema_columns.append(f'{col} VARCHAR')

    _values = []


    @task(task_id=f'create_{_my_table_name}')
    def create_table():
        
        s3_hook = S3Hook(aws_conn_id='aws_default')
        postgres_hook = PostgresHook(postgres_conn_id='postgres')

        # Define the S3 path for the CSV file
        bucket_name = 'raw'
        folder_path = f'{kwargs.get("table_name")}/{kwargs.get("table_name")}.csv'

        # Get the file content from MinIO (S3)
        file_obj = s3_hook.get_key(key=folder_path, bucket_name=bucket_name)
        file_content = file_obj.get()['Body'].read().decode('utf-8')
        
        # Read the CSV data
        _csv_data = csv.reader(io.StringIO(file_content))
        
        # Prepare the SQL for the CREATE TABLE statement
        create_sql = f"""
        CREATE TABLE IF NOT EXISTS {_my_dataset}.{_my_table_name} (
            {', '.join(_prms_schema_columns)},
            insert_dt TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        
        # Prepare the SQL for the TRUNCATE TABLE statement
        truncate_sql = f"""
        TRUNCATE TABLE {_my_dataset}.{_my_table_name};
        """
        
        # Prepare the PostgreSQL connection
        with postgres_hook.get_conn() as conn:
            with conn.cursor() as cursor:
                # Execute the CREATE TABLE SQL
                cursor.execute(create_sql)
                
                # Execute the TRUNCATE TABLE SQL
                cursor.execute(truncate_sql)

                # # Use `execute_values` for efficient bulk insert
                insert_sql = f"""
                INSERT INTO {_my_dataset}.{_my_table_name} ({', '.join(_columns)})
                VALUES %s;
                """
                execute_values(cursor, insert_sql, _csv_data)

                # Commit the changes to PostgreSQL
                conn.commit()
        
    create_table()
    