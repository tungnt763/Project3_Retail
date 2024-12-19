import os
from airflow.decorators import task, task_group
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from lib.job_control import insert_log, get_max_timestamp as _get_max_timestamp
from datetime import datetime

@task_group(group_id='edw_layer')
def edw_layer(**kwargs):
    _gcp_conn_id = kwargs.get("gcp_conn_id")
    _my_project = kwargs.get("project")
    _my_dataset = kwargs.get("dw_dataset")
    _my_table_name = f'dim_{kwargs.get("table_name")}'
    _my_serv_dataset = kwargs.get("staging_dataset")
    _serv_table_name = f'dim_{kwargs.get("table_name")}_stg'
    _dim_type = kwargs.get("dim_type")

    _sql_template = os.path.join('resources', 'sql_template', '3_edw', f'{_dim_type}_tables_cmn_edw.sql')
    _columns = kwargs.get("columns_detail").items()
    _prms_nk = kwargs.get("columns_nk")

    _prms_create_typed_cols = []
    _prms_select_cols = []
    _prms_vcols_4_insert = []
    _prms_lnk_4_update = []
    _prms_nk_4_condition = []

    for col, typed in _columns:
        _prms_create_typed_cols.append(f"{col} {typed}")
        _prms_select_cols.append(f"{col}")
        _prms_vcols_4_insert.append(f"src.{col}")
        if col not in _prms_nk:
            _prms_lnk_4_update.append(f"{col} = src.{col}")
    
    for nk in _prms_nk:
        _prms_nk_4_condition.append(f"tgt.{nk} = src.{nk}")

    params = {
        'my_project': _my_project,
        'my_dataset': _my_dataset,
        'my_table_name': _my_table_name,
        'my_serv_dataset': _my_serv_dataset,
        'serv_table_name': _serv_table_name,
        'create_typed_cols': ',\n'.join(_prms_create_typed_cols),
        'select_cols': ',\n'.join(_prms_select_cols),
        'nk_4_condition': ' AND '.join(_prms_nk_4_condition),
        'lnk_4_update': ',\n'.join(_prms_lnk_4_update),
        'ncols_4_insert': ', '.join(_prms_select_cols),
        'vcols_4_insert': ', '.join(_prms_vcols_4_insert)
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
            print(f">> {_my_table_name}'s max_timestamp: {max_timestamp}")
        else:
            raise Exception("GET max timestamp failed, marking as failed.")

    process = BigQueryInsertJobOperator(
        task_id=f'create_{_my_table_name}',
        configuration={
            "query": {
                "query": "{% include '" + _sql_template + "' %}",
                "useLegacySql": False,
            }
        },
        params=params,
        location='US',  # Thay đổi theo region của bạn
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

    get_max_timestamp() >> process >> update_job_control()