from airflow.decorators import dag, task
from datetime import datetime, timedelta
from airflow.providers.postgres.operators.postgres import PostgresOperator

default_args = {
    'owner': 'airflow',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

@dag(
    dag_id='dag_with_postgres_operator',
    default_args=default_args,
    start_date=datetime(2024, 10, 30),
    schedule_interval='0 0 * * *'
)
def dag_with_postgres_operator():
    
    task1 = PostgresOperator(
        task_id='create_postgres_table',
        postgres_conn_id='postgres',
        sql="""
            CREATE TABLE IF NOT EXISTS DAG_RUN (
                dt date,
                dag_id character varying
            );
        """
    )

    task1

dag_with_postgres_operator()