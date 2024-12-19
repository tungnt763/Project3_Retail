import os
import sys
HOME = os.getenv('AIRFLOW_HOME')
TEMPLATE_ROOT_PATH = os.path.join(HOME, 'dags', 'resources', 'sql_template')
sys.path.append(HOME)

import json
from airflow.decorators import dag, task
from datetime import datetime, timedelta
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from dags.resources.business.dim.l1_dim_landing import landing_layer
from dags.resources.business.dim.l2_dim_staging import staging_layer
from dags.resources.business.dim.l3_dim_edw import edw_layer
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
        tags=[kwargs.get('table_name')],
        catchup=False
    )
    def get_dag():

        @task(provide_context=True) 
        def get_rundate(**context):
            run_dt = _get_rundate()
            print(f">> Rundate: {run_dt}")
            context['ti'].xcom_push(key="rundate", value=run_dt)

        sensor_task = GCSObjectsWithPrefixExistenceSensor(
            task_id=f'sensor_{kwargs.get("table_name")}_file',
            bucket=kwargs.get('bucket_name'),
            prefix=kwargs.get('prefix_name'), 
            google_cloud_conn_id=kwargs.get('gcp_conn_id'),
            timeout=600, 
            poke_interval=30,  
        )

        @task(task_id=f'get_{kwargs.get("table_name")}_file_name')
        def get_file_name(bucket_name, prefix):
            hook = GCSHook(gcp_conn_id=kwargs.get('gcp_conn_id'))

            blobs = hook.list(bucket_name, prefix=prefix)

            if blobs:
                return blobs[0]  
            return None
        
        return_value = get_file_name(kwargs.get('bucket_name'), kwargs.get('prefix_name'))

        ld_layer = landing_layer(**kwargs)
        
        stg_layer = staging_layer(**kwargs)

        dw_layer = edw_layer(**kwargs)

        table_name = kwargs.get("table_name")
        archive_file = GCSToGCSOperator(
            task_id=f'archive_{table_name}_file',
            source_bucket=kwargs.get('bucket_name'),
            source_object="{{ task_instance.xcom_pull(task_ids='get_" + kwargs.get('table_name') + "_file_name', key='return_value') }}",
            destination_bucket=kwargs.get('bucket_name'),
            destination_object="archive/{{ task_instance.xcom_pull(task_ids='get_" + kwargs.get('table_name') + "_file_name', key='return_value').split('/')[-1].split('.')[0] }}_{{ ts_nodash }}.csv",
            move_object=True,
            gcp_conn_id=kwargs.get('gcp_conn_id'),
        )

        get_rundate() >> sensor_task >> return_value >> ld_layer >> stg_layer >> dw_layer >> archive_file

    return get_dag()

config_path = os.path.join(HOME, 'config', 'pipeline_config.json')

with open(config_path, 'r') as inp:
    config_content = inp.read()
    print('Config_content: ', config_content)
    pipelines = json.loads(config_content)['dim_table']
    db_env = json.loads(config_content)['db_environment']

_project = db_env.get('project')
_landing_dataset = db_env.get('landing_dataset')
_staging_dataset = db_env.get('staging_dataset')
_dw_dataset = db_env.get('dw_dataset')
_bucket_name = db_env.get('bucket_name')

for pipeline in pipelines:
    _table_name = pipeline.get('table_name')
    _dag_id = f'{_table_name}_dag'
    _schedule_interval = pipeline.get('schedule_interval')

    _dim_type = pipeline.get('dim_type')
    _columns_detail = pipeline.get('columns_detail')
    _columns_nk = pipeline.get('columns_nk')

    cmn_config = {
        "gcp_conn_id": 'gcp',
        "project": _project,
        "landing_dataset": _landing_dataset,
        "staging_dataset": _staging_dataset,
        "dw_dataset": _dw_dataset,
        "bucket_name": _bucket_name,
        "prefix_name": f'raw/{_table_name}',

        "template_root_path": os.path.join(TEMPLATE_ROOT_PATH),
        "table_name": _table_name,
        "dim_type": _dim_type,
        "columns_nk": _columns_nk,
        "columns_detail": _columns_detail
    }

    globals()[_dag_id] = create_dag(_dag_id, _schedule_interval, **cmn_config)

