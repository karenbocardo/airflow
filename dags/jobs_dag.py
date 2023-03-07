from datetime import datetime
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator

config = {
    'dag_id_1': {'schedule_interval': "@daily", 
                "start_date": datetime(2018, 11, 11),
                "table_name": "table_name_1"},  
    'dag_id_2': {'schedule_interval': "@daily", 
                "start_date": datetime(2018, 11, 11),
                "table_name": "table_name_2"},  
    'dag_id_3':{'schedule_interval': "@daily", 
                "start_date": datetime(2018, 11, 11),
                "table_name": "table_name_3"}
    }

def log_information(dag_id, database):
    logging.info(f"{dag_id} start processing tables in database: {database}")

for id, dict in config.items():
    with DAG(id, start_date=dict["start_date"]) as dag:
        log_info = PythonOperator(
            task_id="log_info",
            python_callable=log_information,
            op_kwargs={"dag_id": id, "database": "example"}
        )

        insert = DummyOperator(task_id="insert_row")

        query = DummyOperator(task_id="query_the_table")

        log_info >> insert >> query