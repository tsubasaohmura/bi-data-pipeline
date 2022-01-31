import pendulum
from datetime import timedelta

from airflow import DAG
from airflow.models.variable import Variable
from airflow.operators.python_operator import PythonOperator

from common.functions import hana_to_ch, send_job_success


default_args = {
    'owner': 'com_bi',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email': Variable.get('urgent_emails'),
    'email_on_retry': True,
    'email_on_failure': True,
}


with DAG(
    dag_id="hana_master_zjmate_all_to_ch_v20201201",
    description="Extract partial material master data from HANA and load to ClickHouse",
    default_args=default_args,
    start_date=pendulum.datetime(2021, 5, 18, tzinfo=pendulum.timezone('Asia/Tokyo')),
    schedule_interval='15 7 * * *',
    concurrency=1,
    max_active_runs=1,
    catchup=False,
    tags=['master_data', 'hana', 'clickhouse'],
) as dag:
    

    extract_hana_to_ch = PythonOperator(
        task_id='extract_hana_to_ch',
        op_kwargs={
            'hana_conn_id': 'hana_PJI',
            'hana_sql_file': 'hana_master_zjmate_all.sql',
            'ch_conn_id': 'clickhouse_commercial',
            'ch_target_table': 'commercial.hana_master_zjmate_all',
            'truncate': True
            },
        python_callable=hana_to_ch,
        provide_context=True,
    )

    send_success_email = PythonOperator(
        task_id='send_success_email',
        python_callable=send_job_success,
        provide_context=True,
    )

    send_success_email_external = PythonOperator(
        task_id='send_success_email_external',
        op_kwargs={
            'external': True
            },
        python_callable=send_job_success,
        provide_context=True,
    )



    extract_hana_to_ch >> send_success_email >> send_success_email_external