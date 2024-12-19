from datetime import datetime, timedelta
import json 
import os

HOME = os.getenv('AIRFLOW_HOME')

def get_rundate() -> str:
    try:
        with open(os.path.join(HOME, 'config', 'run_config.json'), 'r') as f:
            data = json.load(f)
            f.close()
        return data['rundate']
    except Exception as e:
        print(e)
        return "19000101"

def replace_sql_values(sql_template_path: str, replacements: dict={}) -> str:
    with open(sql_template_path, 'r') as file:
        sql_template = file.read()
    try:
        sql_query = sql_template if not replacements else sql_template.format(**replacements)
        return sql_query
    except KeyError as e:
        print(f'Missing key in replacements: ', e)
        return None
