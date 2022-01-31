import pendulum
from datetime import timedelta

from common.functions import sfsf_to_ch, get_sfsf_api_auth, send_job_success

from airflow import DAG
from airflow.models.variable import Variable
from airflow.operators.python_operator import PythonOperator


default_args = {
    'owner': 'com_bi',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email': Variable.get('urgent_emails'),
    'email_on_retry': True,
    'email_on_failure': True,
}


with DAG(
    dag_id="master_hr_sfsf_to_ch_v20200201",
    description="Request API to SAP SFSF server and create HR master data, insert to ClickHouse",
    default_args=default_args,
    start_date=pendulum.datetime(2021, 1, 30, tzinfo=pendulum.timezone('Asia/Tokyo')),
    schedule_interval=timedelta(days=1),
    concurrency=1,
    max_active_runs=1,
    catchup=False,
    tags=['master_data', 'sfsf', 'clickhouse'],
) as dag:

    extract_sfsf_to_ch = PythonOperator(
        task_id='extract_sfsf_to_ch',
        op_kwargs={
            'ch_conn_id': 'clickhouse_commercial',
            'ch_table': 'commercial.hr_master_test',
            'truncate': True
            },
        python_callable=sfsf_to_ch,
        provide_context=True,
    )

    send_success_email = PythonOperator(
        task_id='send_success_email',
        python_callable=send_job_success,
        provide_context=True,
    )


extract_sfsf_to_ch >> send_success_email
