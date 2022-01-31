import pendulum
from datetime import timedelta

from airflow import DAG
from airflow.models.variable import Variable
from airflow.operators.python_operator import PythonOperator

from common.functions import send_job_success
#! Import any other necessary functions

default_args = {
    'owner': 'com_bi',
    'depends_on_past': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email': Variable.get('urgent_emails'),
    'email_on_retry': True,
    'email_on_failure': True,
}


with DAG(
    #! dag_id="dag_name_with_underscores",
    #! description="DAG description",
    #! 'start_date': pendulum.datetime(year, month, day, tzinfo=pendulum.timezone('Asia/Tokyo')), 
    #! schedule_interval='0 0 * * *', # 'minute hour day month day_of_the_week'
    default_args=default_args,
    concurrency=3,
    max_active_runs=1,
    catchup=False,
    #! tags=['dag_tag'],
) as dag:
    
    
    data_task = PythonOperator(
        #! task_id='descriptive_task_id',
        #! python_callable=function_name,
        #! op_kwargs={
        #!     'argument': 'value'   
        #! },
        provide_context=True,
    )

    send_success_email = PythonOperator(
        task_id='send_success_email',
        python_callable=send_job_success,
        provide_context=True,
    )


    data_task >> send_success_email
