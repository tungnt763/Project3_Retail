import os
from airflow.decorators import task, task_group
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from lib.job_control import insert_log, get_max_timestamp as _get_max_timestamp
from datetime import datetime

@task_group(group_id='staging_layer') 
def staging_layer(**kwargs):
    _gcp_conn_id = kwargs.get("gcp_conn_id")
    _my_project = kwargs.get("project")
    _my_dataset = kwargs.get("staging_dataset")
    _my_table_name = f'dim_{kwargs.get("table_name")}_stg'
    _my_serv_dataset = kwargs.get("landing_dataset")
    _serv_table_name = f'dim_{kwargs.get("table_name")}_ld'
    _dim_type = kwargs.get("dim_type")

    _sql_template = os.path.join('resources', 'sql_template', '2_staging', f'{_dim_type}_tables_cmn_stg.sql')
    _columns = kwargs.get("columns_detail").items()
    _prms_nk = kwargs.get("columns_nk")

    _prms_cast_typed_cols = []
    for col, col_type in _columns:
        _prms_cast_typed_cols.append(f'CAST({col} AS {col_type}) AS {col}')

    params = {
        'my_project': _my_project,
        'my_dataset': _my_dataset,
        'my_table_name': _my_table_name,
        'my_serv_dataset': _my_serv_dataset,
        'serv_table_name': _serv_table_name,
        'nk': ', '.join(_prms_nk),
        'cast_typed_cols': ',\n'.join(_prms_cast_typed_cols)
    }

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
            raise Exception("GET  max timestamp failed, marking task as failed.")

    process = BigQueryInsertJobOperator(
        task_id=f'create_{_my_table_name}',
        configuration={
            "query": {
                "query": "{% include '" + _sql_template + "' %}",
                "useLegacySql": False,
            }
        },
        params=params,
        location='US',
        gcp_conn_id=_gcp_conn_id
    )

    @task(provide_context=True) 
    def update_job_control(**context):
        print("rundate: ", context['ti'].xcom_pull(task_ids='get_rundate', key='rundate'))
        log = insert_log(
            gcp_conn_id=_gcp_conn_id, 
            dataset_name=_my_dataset, 
            table_name=_my_table_name, 
            max_timestamp=datetime.now().strftime('%Y-%m-%d %H:%M:%S'), 
            rundate=int(context['ti'].xcom_pull(task_ids='get_rundate', key='rundate'))
        )
        
        if log:
            print(f'>> Job control updated: {log}')
        else:
            raise Exception("Log insertion failed, marking task as failed.")

    get_max_timestamp() >> process >> update_job_control()
    