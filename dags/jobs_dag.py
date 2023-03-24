import uuid
import logging
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.models.xcom import XCom
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.trigger_rule import TriggerRule
from custom_operator.postgres_operator import PostgreSQLCountRows


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
def check_table_exist(sql_to_check_table_exist, table_name):
    """ callable function to get schema name and after that check if table exist """ 
    hook = PostgresHook()
    schema = 'public'

    # check table exist
    query = hook.get_first(sql=sql_to_check_table_exist.format(schema, table_name))
    print(query)

    if query:
        return 'insert_row'
    else:
        logging.info(f"table {table_name} does not exist")
        return 'create_table'    

def log_information(dag_id, database):
    logging.info(f'{dag_id} start processing tables in database: {database}')

for id, dict in config.items():
    with DAG(id, start_date=dict['start_date'], schedule_interval=dict['schedule_interval']) as dag:
        start = PythonOperator(
            task_id='print_process_start',
            python_callable=log_information,
            op_kwargs={'dag_id': id, 'database': 'public'}
        )

        get_user = BashOperator(
            task_id="get_current_user",
            bash_command="whoami"
        )

        table_name = "table_name"
        check = BranchPythonOperator(task_id='check_table_exists',
                                    python_callable=check_table_exist,
                                    op_args=["SELECT * FROM information_schema.tables "
                                            "WHERE table_schema = '{}'"
                                            "AND table_name = '{}';", table_name])
        
        sql_create_table = '''CREATE TABLE table_name(custom_id integer NOT NULL, 
                    user_name VARCHAR (50) NOT NULL, timestamp TIMESTAMP NOT NULL);'''
        create = PostgresOperator(task_id='create_table',
                                        postgres_conn_id='postgres_default',
                                        sql=sql_create_table)

        custom_id_value = uuid.uuid4().int % 123456789
        timestamp_value = datetime.now()
        insert = PostgresOperator(task_id='insert_row',
                                postgres_conn_id='postgres_default',
                                sql='''INSERT INTO table_name VALUES(%s, '{{ ti.xcom_pull(task_ids='get_current_user', key='return_value') }}', %s);''',
                                parameters=(custom_id_value, timestamp_value),
                                trigger_rule='all_done')

        query = PostgreSQLCountRows(task_id='query_table',
                            postgres_conn_id='postgres_default',
                            table_name='table_name'
                            )

        start >> get_user
        get_user >> check 
        check >> [create, insert]
        create >> insert
        insert >> query