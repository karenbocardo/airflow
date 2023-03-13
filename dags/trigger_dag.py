from datetime import datetime

from airflow import DAG
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.bash import BashOperator

with DAG('trigger_dag', start_date=datetime(2022, 11, 11), schedule_interval=None) as dag:
    sensor_task = FileSensor(task_id= "file_sensor_task", 
                             poke_interval= 1,  
                             filepath= "/tmp/")
    
    trigger_dagrun = TriggerDagRunOperator(
        task_id='trigger_run',
        trigger_dag_id='dag_id_2'
    )

    rm_file = BashOperator(
        task_id="rm_file",
        bash_command="rm /opt/airflow/tmp/run",
    )

    sensor_task >> trigger_dagrun
    trigger_dagrun >> rm_file