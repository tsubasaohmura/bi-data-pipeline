import pendulum
from datetime import timedelta, date

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
    dag_id="hana_transaction_sales_detail_to_ch_7am_v20210824",
    description="Extract transaction data from HANA and insert into ClickHouse",
    default_args=default_args,
    start_date=pendulum.datetime(2021, 8, 22, tzinfo=pendulum.timezone('Asia/Tokyo')),
    schedule_interval='01 7 * * *',
    concurrency=1,
    max_active_runs=1,
    catchup=False,
    tags=['transaction_data', 'hana', 'clickhouse'],
) as dag:
    

    extract_hana_to_ch = PythonOperator(
        task_id='extract_hana_to_ch',
        op_kwargs={
            'hana_conn_id': 'hana_PJI',
            'hana_sql_file': 'hana_zjcust_select_filtered.sql',
            'ch_conn_id': 'clickhouse_commercial',
            'ch_target_table': 'commercial.sales_detail_7am',
            'truncate': False,
            'transaction':True,
            'sales_detail': True,
            'target_date':  str(date.today() - timedelta(days=1))
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
