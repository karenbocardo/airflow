FROM apache/airflow:2.5.1
COPY airflow.cfg /opt/airflow/airflow.cfg

USER airflow