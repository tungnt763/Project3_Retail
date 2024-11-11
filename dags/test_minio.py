from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta
import os

@dag(
    dag_id='test_minio',
    start_date=datetime(2024, 10, 15),
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['test']
)
def test_dag():

    @task
    def download_file_from_minio(bucket_name, key, local_dir):
        # Ensure the local directory exists and create it if it doesn't
        if not os.path.exists(local_dir):
            try:
                os.makedirs(local_dir)
                print(f"Created directory: {local_dir}")
            except Exception as e:
                raise Exception(f"Failed to create directory {local_dir}: {str(e)}")
        
        # Use the S3Hook to download the file
        s3 = S3Hook(aws_conn_id='aws_default')
        
        try:
            # Download the file to the correct location
            s3.download_file(
                bucket_name=bucket_name, 
                key=key,
                local_path=local_dir  # Full path to save the file
            )
            print(f"Successfully downloaded {key} from bucket {bucket_name} to {local_dir}")
        except Exception as e:
            raise Exception(f"Failed to download file {key} from bucket {bucket_name}: {str(e)}")

    # Download file task
    download = download_file_from_minio('raw', 'retail_db_json/categories/part-r-00000-ce1d8208-178d-48d3-bfb2-1a97d9c05094', '/opt/airflow/dags/data/categories')

    download

# Instantiate the DAG
test_dag()
