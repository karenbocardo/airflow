from datetime import datetime, timedelta
import logging

from airflow import DAG
from airflow.models import Variable
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.bash import BashOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from airflow.operators.python import PythonOperator
from slack import WebClient
from slack.errors import SlackApiError

path = Variable.get('path', default_var='/opt/airflow/temp/run')
SLACK_TOKEN = Variable.get('slack_token')
dag_to_trigger = 'dag_id_3'

def print_result(**context):
    data_received = context['ti'].xcom_pull(dag_id=dag_to_trigger, task_ids='query_table', key='return_value')
    logging.info(f"data recieved: {data_received}")
    logging.info(f"context: {context}")

def pull_logical_date(self, **kwargs):
    try:
        value = kwargs['ti'].xcom_pull(dag_id='trigger_dag', task_ids='trigger_run', key='trigger_execution_date_iso', include_prior_dates=True)
        logging.info(value)
        return datetime.fromisoformat(value)
    except Exception as e:
        return logging.info('Pulling execution date failed')

def send_slack_message(**context):
    # Slack API token
    # SLACK_TOKEN = 'xoxb-5000400506163-5025096779302-g5Ah31axGZiL3B0AGslPjlPf'
    logging.info(SLACK_TOKEN)

    # Create a WebClient instance using the Slack API token
    client = WebClient(token=SLACK_TOKEN)

    # Set the channel ID of the channel you want to send the message to
    channel_id = 'airflow'

    # Get the DAG ID and execution date
    dag_id = context['dag'].dag_id
    execution_date = context['execution_date']

    # Construct the message to send
    message = f"Airflow DAG {dag_id} has started execution on {execution_date}"

    try:
        # Call the chat.postMessage API method using the WebClient instance
        response = client.chat_postMessage(
            channel=channel_id,
            text=message
        )

        # Log the response if successful
        if response['ok']:
            print("Slack message sent successfully!")
    except SlackApiError as e:
        # Log any errors if the API call fails
        print("Error sending Slack message: {}".format(e))

# Define the SubDag
def subdag(parent_dag_id, child_dag_id, start_date, schedule_interval):

    with DAG(dag_id=f'{parent_dag_id}.{child_dag_id}',
                start_date=start_date,
                schedule_interval=schedule_interval,
                catchup=False
                ) as subdag_dag:
        
        # Define the ExternalTaskSensor to wait for the completion of the triggered DAG
        sensor_dag = ExternalTaskSensor(
            task_id='sensor_triggered_dag',
            external_dag_id=dag_to_trigger,
            external_task_id=None,
            allowed_states=['success'],
            execution_date_fn=pull_logical_date,
            poke_interval=10,
            
        )

        '''
         Inside the  “print the result” task: get this Xcom with xcom_pull() and print the read value to the log. n
        '''
        result = PythonOperator(
            task_id='print_result',
            python_callable=print_result,
            provide_context=True
        )

        rm_file = BashOperator(
            task_id='rm_file',
            bash_command=f'rm {path}'
        )

        # Define the BashOperator to create the file
        finished_file = BashOperator(
            task_id='create_finished_file',
            bash_command='touch /opt/airflow/temp/finished_{{ ts_nodash }}'
        )

        alert = PythonOperator(
            task_id='alert_slack',
            python_callable=send_slack_message,
            provide_context=True
        )

        sensor_dag >> result >> rm_file >> finished_file >> alert

        # Return the SubDag
        return subdag_dag

with DAG('trigger_dag', start_date=datetime(2022, 11, 11), schedule_interval=None) as dag:
    sensor_task = FileSensor(task_id= 'file_sensor_task', 
                             poke_interval= 1,  
                             filepath= path, 
                             fs_conn_id= 'fs_default')
    
    trigger_dagrun = TriggerDagRunOperator(
        task_id='trigger_run',
        trigger_dag_id=dag_to_trigger
    )

    # Create the SubDagOperator
    subdag_task = SubDagOperator(
        task_id='subdag_task',
        subdag=subdag('trigger_dag', 'subdag_task', datetime(2023, 3, 13), timedelta(days=1))
    )

    sensor_task >> trigger_dagrun
    trigger_dagrun >> subdag_task