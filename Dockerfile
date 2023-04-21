FROM apache/airflow:2.3.0-python3.8
COPY airflow.cfg /opt/airflow/airflow.cfg

USER airflow
RUN pip install aiohttp