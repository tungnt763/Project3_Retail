FROM apache/airflow:2.7.1

USER airflow

COPY ./requirements.txt /opt/airflow/requirements.txt

RUN pip install --user --upgrade pip
RUN pip install --no-cache-dir --user -r /opt/airflow/requirements.txt