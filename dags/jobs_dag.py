from datetime import datetime
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.bash import BashOperator

config = {
    'dag_id_1': {'schedule_interval': None, 
                'start_date': datetime(2022, 11, 11),
                'table_name': 'table_name_1'},  
    'dag_id_2': {'schedule_interval': None, 
                'start_date': datetime(2018, 11, 11),
                'table_name': 'table_name_2'},  
    'dag_id_3':{'schedule_interval':  None, # changed from '@daily'
                'start_date': datetime(2018, 11, 11),
                'table_name': 'table_name_3'}
    }

# branching function
def check_table_exist():
    ''' method to check that table exists '''
    if True:
        return 'insert_row'
    return 'create_table'

def log_information(dag_id, database):
    logging.info(f'{dag_id} start processing tables in database: {database}')

for id, dict in config.items():
    with DAG(id, start_date=dict['start_date'], schedule_interval=dict['schedule_interval']) as dag:
        start = PythonOperator(
            task_id='print_process_start',
            python_callable=log_information,
            op_kwargs={'dag_id': id, 'database': 'example'}
        )

        get_user = BashOperator(
            task_id="get_current_user",
            bash_command="whoami",
        )

        check = BranchPythonOperator(
            task_id='check_table_exists',
            python_callable=check_table_exist
        )

        create = DummyOperator(task_id='create_table')

        insert = DummyOperator(task_id='insert_row', 
                               trigger_rule='none_failed') # already done

        query = DummyOperator(task_id='query_table')

        start >> get_user
        get_user >> check 
        check >> [create, insert]
        create >> insert
        insert >> query