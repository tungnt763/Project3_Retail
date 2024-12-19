import os
import sys
HOME = os.getenv('AIRFLOW_HOME')
TEMPLATE_ROOT_PATH = os.path.join(HOME, 'dags', 'resources', 'sql_template')
sys.path.append(HOME)

import json
from airflow.decorators import dag, task
from datetime import datetime, timedelta
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from dags.resources.business.fact.l1_fact_landing import landing_layer
from dags.resources.business.fact.l2_fact_staging import staging_layer
from dags.resources.business.fact.l3_fact_edw import edw_layer
from lib.utils import get_rundate as _get_rundate
from airflow.providers.google.cloud.sensors.gcs import GCSObjectsWithPrefixExistenceSensor
from airflow.providers.google.cloud.hooks.gcs import GCSHook
_default_args = {
    'owner': 'tungnt',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=15),
    'start_date': datetime.today()
}

def create_dag(_dag_id, _schedule, **kwargs):

    @dag(
        dag_id=_dag_id,
        default_args=_default_args,
        schedule=_schedule,
        tags=['fact_sales'],
        catchup=False
    )
    def get_dag():

        @task(provide_context=True) 
        def get_rundate(**context):
            run_dt = _get_rundate()
            print(f">> Rundate: {run_dt}")
            context['ti'].xcom_push(key="rundate", value=run_dt)

        sensor_orders_task = GCSObjectsWithPrefixExistenceSensor(
            task_id=f'sensor_orders_file',
            bucket=kwargs.get('bucket_name'),
            prefix='raw/orders', 
            google_cloud_conn_id=kwargs.get('gcp_conn_id'),
            timeout=600, 
            poke_interval=30,  
        )

        sensor_order_items_task = GCSObjectsWithPrefixExistenceSensor(
            task_id=f'sensor_order_items_file',
            bucket=kwargs.get('bucket_name'),
            prefix='raw/order_items', 
            google_cloud_conn_id=kwargs.get('gcp_conn_id'),
            timeout=600, 
            poke_interval=30,  
        )

        @task(task_id=f'get_orders_file_name')
        def get_orders_file_name(bucket_name, prefix):
            hook = GCSHook(gcp_conn_id=kwargs.get('gcp_conn_id'))

            blobs = hook.list(bucket_name, prefix=prefix)

            if blobs:
                return blobs[0]  
            return None
        
        return_orders_value = get_orders_file_name(kwargs.get('bucket_name'), 'raw/orders')

        @task(task_id=f'get_order_items_file_name')
        def get_order_items_file_name(bucket_name, prefix):
            hook = GCSHook(gcp_conn_id=kwargs.get('gcp_conn_id'))

            blobs = hook.list(bucket_name, prefix=prefix)

            if blobs:
                return blobs[0]  
            return None
        
        return_order_items_value = get_order_items_file_name(kwargs.get('bucket_name'), 'raw/order_items')

        ld_layer = landing_layer(**kwargs)
        
        stg_layer = staging_layer(**kwargs)

        dw_layer = edw_layer(**kwargs)

        archive_orders_file = GCSToGCSOperator(
            task_id=f'archive_orders_file',
            source_bucket=kwargs.get('bucket_name'),
            source_object="{{ task_instance.xcom_pull(task_ids='get_orders_file_name', key='return_value') }}",
            destination_bucket=kwargs.get('bucket_name'),
            destination_object="archive/{{ task_instance.xcom_pull(task_ids='get_orders_file_name', key='return_value').split('/')[-1].split('.')[0] }}_{{ ts_nodash }}.csv",
            move_object=True,
            gcp_conn_id=kwargs.get('gcp_conn_id'),
        )

        archive_order_items_file = GCSToGCSOperator(
            task_id=f'archive_order_items_file',
            source_bucket=kwargs.get('bucket_name'),
            source_object="{{ task_instance.xcom_pull(task_ids='get_order_items_file_name', key='return_value') }}",
            destination_bucket=kwargs.get('bucket_name'),
            destination_object="archive/{{ task_instance.xcom_pull(task_ids='get_order_items_file_name', key='return_value').split('/')[-1].split('.')[0] }}_{{ ts_nodash }}.csv",
            move_object=True,
            gcp_conn_id=kwargs.get('gcp_conn_id'),
        )

        get_rundate() >> sensor_orders_task >> sensor_order_items_task >> [return_orders_value, return_order_items_value] >> ld_layer >> stg_layer >> dw_layer >> [archive_orders_file, archive_order_items_file]

    return get_dag()

config_path = os.path.join(HOME, 'config', 'pipeline_config.json')

with open(config_path, 'r') as inp:
    config_content = inp.read()
    print('Config_content: ', config_content)
    pipelines = json.loads(config_content)['fact_table']
    db_env = json.loads(config_content)['db_environment']

_project = db_env.get('project')
_landing_dataset = db_env.get('landing_dataset')
_staging_dataset = db_env.get('staging_dataset')
_dw_dataset = db_env.get('dw_dataset')
_bucket_name = db_env.get('bucket_name')
_orders_columns_detail = pipelines[0].get('columns_detail')
_order_items_columns_detail = pipelines[1].get('columns_detail')

cmn_config = {
    "gcp_conn_id": 'gcp',
    "project": _project,
    "landing_dataset": _landing_dataset,
    "staging_dataset": _staging_dataset,
    "dw_dataset": _dw_dataset,
    "bucket_name": _bucket_name,
    "orders_columns_detail": _orders_columns_detail,
    "order_items_columns_detail": _order_items_columns_detail,

    "template_root_path": os.path.join(TEMPLATE_ROOT_PATH),
}

globals()['fact_sales_dag'] = create_dag('fact_sales_dag', None, **cmn_config)

