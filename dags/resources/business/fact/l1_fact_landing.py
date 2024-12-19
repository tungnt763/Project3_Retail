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

    _orders_table= 'orders_ld'
    _order_items_table= 'order_items_ld'  
    _orders_columns_detail = kwargs.get("orders_columns_detail")
    _order_items_columns_detail = kwargs.get("order_items_columns_detail")

    _gcp_conn_id = kwargs.get("gcp_conn_id")
    _sql_template = os.path.join('resources', 'sql_template', '1_landing', 'fact_sales_ld.sql')
    
    _schema_orders_columns = []
    _schema_order_items_columns = []
    for col in _orders_columns_detail:
        _schema_orders_columns.append({'name': f'{col}', 'type': 'STRING'})
    for col in _order_items_columns_detail:
        _schema_order_items_columns.append({'name': f'{col}', 'type': 'STRING'})

    @task(provide_context=True)
    def get_max_timestamp_orders(**context):
        get_max_timestamp_orders = _get_max_timestamp(
            gcp_conn_id=_gcp_conn_id,
            dataset_name=_my_dataset,
            table_name=_orders_table
        )

        if get_max_timestamp_orders:
            context['ti'].xcom_push(key='max_timestamp_orders', value=get_max_timestamp_orders)
            print(f">> {_orders_table}'s max timestamp: {get_max_timestamp_orders}")
        else:
            raise Exception("GET max timestamp failed, marking task as failed.")
        
    @task(provide_context=True)
    def get_max_timestamp_order_items(**context):
        get_max_timestamp_order_items = _get_max_timestamp(
            gcp_conn_id=_gcp_conn_id,
            dataset_name=_my_dataset,
            table_name=_order_items_table
        )

        if get_max_timestamp_order_items:
            context['ti'].xcom_push(key='max_timestamp_order_items', value=get_max_timestamp_order_items)
            print(f">> {_order_items_table}'s max timestamp: {get_max_timestamp_order_items}")
        else:
            raise Exception("GET max timestamp failed, marking task as failed.")
        
    move_to_landing_temp_orders = GCSToBigQueryOperator(
        task_id='move_to_landing_temp_orders',
        bucket=_bucket_name,
        source_objects=["{{ task_instance.xcom_pull(task_ids='get_orders_file_name', key='return_value') }}"],
        destination_project_dataset_table=f"{_my_project}.{_my_dataset}.{_orders_table}_temp",
        schema_fields=_schema_orders_columns,
        source_format='CSV',
        write_disposition='WRITE_TRUNCATE', 
        create_disposition='CREATE_IF_NEEDED',
        gcp_conn_id=_gcp_conn_id
    )

    move_to_landing_temp_order_items = GCSToBigQueryOperator(
        task_id='move_to_landing_temp_order_items',
        bucket=_bucket_name,
        source_objects=["{{ task_instance.xcom_pull(task_ids='get_order_items_file_name', key='return_value') }}"], 
        destination_project_dataset_table=f"{_my_project}.{_my_dataset}.{_order_items_table}_temp",
        schema_fields=_schema_order_items_columns,
        source_format='CSV',
        write_disposition='WRITE_TRUNCATE', 
        create_disposition='CREATE_IF_NEEDED',
        gcp_conn_id=_gcp_conn_id
    )

    process = BigQueryInsertJobOperator(
        task_id=f'create_orders_order_items_table',
        configuration={
            "query": {
                "query": "{% include '" + _sql_template + "' %}",
                "useLegacySql": False,
            }
        },
        params={
            'my_project': _my_project,
            'my_dataset': _my_dataset
        },
        location='US', 
        gcp_conn_id=_gcp_conn_id
    )

    @task(provide_context=True)
    def update_job_control_orders(**context):
        log = insert_log(
            gcp_conn_id=_gcp_conn_id,
            dataset_name=_my_dataset,
            table_name=_orders_table,
            max_timestamp=datetime.now().strftime('%Y-%m-%d %H:%M:%S'), 
            rundate=int(context['ti'].xcom_pull(task_ids='get_rundate', key='rundate'))
        )

        if log:
            print(f">> Job control updated: {log}")
        else:
            raise Exception("Log insertion failed, marking task as failed.")
    
    @task(provide_context=True)
    def update_job_control_order_items(**context):
        log = insert_log(
            gcp_conn_id=_gcp_conn_id,
            dataset_name=_my_dataset,
            table_name=_order_items_table,
            max_timestamp=datetime.now().strftime('%Y-%m-%d %H:%M:%S'), 
            rundate=int(context['ti'].xcom_pull(task_ids='get_rundate', key='rundate'))
        )

        if log:
            print(f">> Job control updated: {log}")
        else:
            raise Exception("Log insertion failed, marking task as failed.")
        
    # Get max timestamp tasks
    get_max_timestamp_orders_task = get_max_timestamp_orders()
    get_max_timestamp_order_items_task = get_max_timestamp_order_items()

    # Update job control tasks
    update_job_control_orders_task = update_job_control_orders()
    update_job_control_order_items_task = update_job_control_order_items()

    # Set dependencies
    [get_max_timestamp_orders_task, get_max_timestamp_order_items_task] >> move_to_landing_temp_orders
    [get_max_timestamp_orders_task, get_max_timestamp_order_items_task] >> move_to_landing_temp_order_items
    [move_to_landing_temp_orders, move_to_landing_temp_order_items] >> process
    process >> update_job_control_orders_task
    process >> update_job_control_order_items_task

    