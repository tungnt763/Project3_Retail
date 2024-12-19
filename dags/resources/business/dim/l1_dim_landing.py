import os
import sys
HOME = os.getenv('AIRFLOW_HOME')
sys.path.append(HOME)

import json
from airflow.decorators import task, task_group
from datetime import datetime
from lib.job_control import get_max_timestamp as _get_max_timestamp
from lib.job_control import insert_log 
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

HOME = os.getenv('AIRFLOW_HOME')
TEMPLATE_ROOT_PATH =  os.path.join(HOME, 'dags', 'resources', 'sql_template')
config_path = os.path.join(HOME, 'config', 'pipeline_config.json')

@task_group(group_id="landing_layer")
def landing_layer(**kwargs):
    _bucket_name = kwargs.get("bucket_name")
    _my_project = kwargs.get("project")
    _my_dataset = kwargs.get("landing_dataset")
    _my_table_name = f'dim_{kwargs.get("table_name")}_ld'   
    _temp_table = f'dim_{kwargs.get("table_name")}_ld_temp'

    _columns = kwargs.get("columns_detail")
    _gcp_conn_id = kwargs.get("gcp_conn_id")
    _sql_template = os.path.join('resources', 'sql_template', '1_landing', 'dim_tables_cmn_ld.sql')
    
    _prms_schema_columns = []
    _schema_columns = []
    for col in _columns:
        _prms_schema_columns.append(f'{col} STRING')
        _schema_columns.append({'name': f'{col}', 'type': 'STRING'})

    @task(provide_context=True)
    def get_max_timestamp(**context):
        max_timestamp = _get_max_timestamp(
            gcp_conn_id=_gcp_conn_id,
            dataset_name=_my_dataset,
            table_name=_my_table_name
        )

        if max_timestamp:
            context['ti'].xcom_push(key='max_timestamp', value=max_timestamp)
            print(f">> {_my_table_name}'s max timestamp: {max_timestamp}")
        else:
            raise Exception("GET max timestamp failed, marking task as failed.")

    move_to_landing_temp = GCSToBigQueryOperator(
        task_id='move_to_landing_temp',
        bucket=_bucket_name,
        source_objects=["{{ task_instance.xcom_pull(task_ids='get_" + kwargs.get('table_name') + "_file_name', key='return_value') }}"],
        destination_project_dataset_table=f"{_my_project}.{_my_dataset}.{_temp_table}",
        schema_fields=_schema_columns,
        source_format='CSV',
        write_disposition='WRITE_TRUNCATE', 
        create_disposition='CREATE_IF_NEEDED',
        gcp_conn_id=_gcp_conn_id
    )

    process = BigQueryInsertJobOperator(
        task_id=f'create_{_my_table_name}',
        configuration={
            "query": {
                "query": "{% include '" + _sql_template + "' %}",
                "useLegacySql": False,
            }
        },
        params={
            'source_table': _temp_table,
            'my_project': _my_project,
            'my_dataset': _my_dataset,
            'my_table_name': _my_table_name,
            'schema_columns': ',\n\t'.join(_prms_schema_columns),
        },
        location='US', 
        gcp_conn_id=_gcp_conn_id
    )

    @task(provide_context=True)
    def update_job_control(**context):
        log = insert_log(
            gcp_conn_id=_gcp_conn_id,
            dataset_name=_my_dataset,
            table_name=_my_table_name,
            max_timestamp=datetime.now().strftime('%Y-%m-%d %H:%M:%S'), 
            rundate=int(context['ti'].xcom_pull(task_ids='get_rundate', key='rundate'))
        )

        if log:
            print(f">> Job control updated: {log}")
        else:
            raise Exception("Log insertion failed, marking task as failed.")
        
    get_max_timestamp() >> move_to_landing_temp >> process >> update_job_control()

    