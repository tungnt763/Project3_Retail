import os
import textwrap
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from lib.utils import replace_sql_values

HOME = os.getenv('AIRFLOW_HOME')
TEMPLATE_ROOT_PATH = os.path.join(HOME, 'dags', 'resources', 'sql_template')

# Hàm chung để thực thi truy vấn SQL
def execute_query(hook: BigQueryHook, sql_context: str):
    print('>>>---EXECUTING SQL---<<<')
    print(textwrap.indent(sql_context, '    '))
    print('>>>---END OF SQL---<<<')
    
    # Thực thi truy vấn
    return hook.insert_job(
            configuration={
                "query": {
                    "query": sql_context,
                    "useLegacySql": False  # Ensure Standard SQL
                }
            }
        )

# Thêm log vào bảng job_control
def insert_log(gcp_conn_id: str, dataset_name: str, table_name: str, max_timestamp: str, rundate: int) -> bool:
    try:
        # Tạo BigQueryHook
        hook = BigQueryHook(gcp_conn_id=gcp_conn_id)

        # Path của SQL QUERY
        _path = os.path.join(TEMPLATE_ROOT_PATH, "0_job_control", "insert_log.sql")
        # Chuẩn bị dữ liệu để thay thế
        _data = f"('{dataset_name}', '{table_name}', '{max_timestamp}', {rundate}, CURRENT_TIMESTAMP)"
        _replacements = {"_values": _data}

        # Replace values trong SQL template
        sql_context = replace_sql_values(_path, _replacements)

        # Thực thi truy vấn
        execute_query(hook, sql_context)

        return True
    except Exception as e:
        print(f"An error occurred: {e}")
        return False

# Lấy max timestamp từ bảng
def get_max_timestamp(gcp_conn_id: str, dataset_name: str, table_name: str) -> str:
    try:
        # Tạo BigQueryHook
        hook = BigQueryHook(gcp_conn_id=gcp_conn_id)

        # Path của SQL QUERY
        _path = os.path.join(TEMPLATE_ROOT_PATH, "0_job_control", "get_max_timestamp.sql")
        
        # Chuẩn bị các giá trị để thay thế
        _replacements = {"_dataset_name": dataset_name, "_table_name": table_name}

        # Replace values trong SQL template
        sql_context = replace_sql_values(_path, _replacements)

        # Thực thi truy vấn và lấy kết quả
        result = execute_query(hook, sql_context)  # Wait for the query to complete
        for row in result:
            return row[0]

        return "1900-01-01 00:00:00.000000"
    except Exception as e:
        print(f"An error occurred: {e}")
        return "1900-01-01 00:00:00.000000"

# Xóa log trong bảng
def delete_log(gcp_conn_id: str, dataset_name: str, table_name: str) -> bool:
    try:
        # Tạo BigQueryHook
        hook = BigQueryHook(gcp_conn_id=gcp_conn_id)

        # Path của SQL QUERY
        _path = os.path.join(TEMPLATE_ROOT_PATH, "0_job_control", "delete_log.sql")

        # Chuẩn bị các giá trị để thay thế
        _replacements = {"_dataset_name": dataset_name, "_table_name": table_name}

        # Replace values trong SQL template
        sql_context = replace_sql_values(_path, _replacements)

        # Thực thi truy vấn
        execute_query(hook, sql_context)

        return True
    except Exception as e:
        print(f"An error occurred: {e}")
        return False

# Xóa toàn bộ log
def truncate_logs(gcp_conn_id: str) -> bool:
    try:
        # Tạo BigQueryHook
        hook = BigQueryHook(gcp_conn_id=gcp_conn_id)

        # Path của SQL QUERY
        _path = os.path.join(TEMPLATE_ROOT_PATH, "0_job_control", "truncate_logs.sql")
        sql_context = replace_sql_values(_path)

        # Thực thi truy vấn
        execute_query(hook, sql_context)

        return True
    except Exception as e:
        print(f"An error occurred: {e}")
        return False
