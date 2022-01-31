from datetime import timedelta
from functools import wraps
import json
import logging
import os
from pathlib import Path
import uuid

import pendulum
import requests

from airflow import DAG
from airflow.hooks.base_hook import BaseHook
from airflow.models.variable import Variable
from airflow.operators.python_operator import PythonOperator

from common.functions import run_mlcp, send_job_success

# Main function for calling O365 Management API
def get_daily_data(*, start_datetime, end_datetime, scope, **context):
    # Temporarily enable proxy to the Internet
    os.environ['http_proxy'] = "http://10.139.60.231:8080"
    os.environ['https_proxy'] = "https://10.139.60.231:8080"

    # Initial setup
    c = context
    logger = logging.getLogger("airflow.task")
    conn_info = BaseHook.get_connection("o365_management_api_ccbji")
    conn_extra = conn_info.extra_dejson
    conn_extra.update({"publisher_guid": uuid.uuid4()})
    output_dir = Path('/tmp') / 'o365_analytics' / c['ds_nodash']
    scoped_dir = output_dir / scope.replace('.', '_').lower()
    scoped_dir.mkdir(parents=True, exist_ok=True)  # create dirs if don't exist
    dt_format ="%Y-%m-%dT%H:%M:%SZ"
    start_time = start_datetime.format(dt_format)
    end_time = end_datetime.format(dt_format)

    def auto_retry(func, times=2):
        """Decorator function that allows API call to retry itself.
        Automatically retrieves new token on each retry.
        O365 Management API is flacky and sometimes returns error codes, which is usually solved on retry.
        """
        @wraps(func)
        def wrapper(*args, **kwargs):
            retry = 0
            while True:
                try:
                    token = get_access_token()
                    value = func(token, *args, **kwargs)
                    return value
                except Exception as e:
                    logger.error(e)
                    if retry < times:
                        retry += 1
                        logger.info(f"Retrying {retry} out of {times}")
                        continue
                    else:
                        raise
        return wrapper

    def get_access_token():
        data = {
            "grant_type": "client_credentials",
            "resource": conn_info.host,
            "client_id": conn_info.login,
            "client_secret": conn_info.password,
            }
        r = requests.post(
            f"https://login.windows.net/{conn_extra['tenant_domain']}/oauth2/token?api-version=1.0",
            data=data
            )
        r.raise_for_status()
        content = r.json()
        logger.info("New access token retrieved.")
        return f"{content['token_type']} {content['access_token']}"

    @auto_retry
    def list_available_contents(token, uri=None, start_time=start_time, end_time=end_time, content_type=scope):
        headers = {'Authorization': token}
        params = {
            'startTime': start_time,
            'endTime': end_time,
            'contentType': content_type,
            'PublisherIdentifier': conn_extra['publisher_guid'],
            }
        if uri:
            # Get contents of nextPageUri in pagination
            r = requests.get(
                uri,
                headers=headers,
            )
        else:
            r = requests.get(
                f"https://manage.office.com/api/v1.0/{conn_extra['tenant_domain']}/activity/feed/subscriptions/content",
                headers=headers,
                params=params,
            )
        r.raise_for_status()
        contents = r.json()
        next_page_uri = r.headers.get('nextPageUri', None)
        return contents, next_page_uri

    @auto_retry
    def get_events_in_content(token, content_uri):
        headers = {'Authorization': token}
        params = {'PublisherIdentifier': conn_extra['publisher_guid']}
        r = requests.get(
            content_uri,
            headers=headers,
            params=params,
            )
        r.raise_for_status()
        events = r.json()
        return events

    def save_events_to_json(events, creation_time):
        filename = creation_time.replace(':', '-').replace('.', '-') + '.json'
        formatted_json = {"root":[]}
        for event in events:
            formatted_json["root"].append(event)
        with open(scoped_dir / filename, 'w', encoding='utf-8') as outfile:
            json.dump(formatted_json, outfile, ensure_ascii=False)

    
    # Main process
    logger.info(f"[START] Scope: {scope}  From: {start_time}  To: {end_time}")
    num_events = 0
    next_page = None
    while True:
        contents, next_page = list_available_contents(uri=next_page)
        logger.info(f"List of contents retrieved. nextPageUri: {'Exists' if next_page else 'None'}")
        for content in contents:
            events = get_events_in_content(content['contentUri'])
            num_events += len(events)
            save_events_to_json(events, content['contentCreated'])
        if not next_page:
            break
    logger.info(f"[END] Scope: {scope}  From: {start_time}  To: {end_time}")
    logger.info(f"Number of events: {num_events}")

    c['ti'].xcom_push(key='input_dir', value=output_dir)


# Pipeline definition

default_args = {
    'owner': 'com_bi',
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'email': Variable.get('urgent_emails'),
    'email_on_retry': True,
    'email_on_failure': True,
}


with DAG(
    dag_id="o365_activity_to_ml_v20201211",
    description="Extract CCBJI Office 365 user activity data and import to MarkLogic",
    default_args=default_args,
    start_date=pendulum.datetime(2021, 1, 1, tzinfo=pendulum.timezone('Asia/Tokyo')),
    schedule_interval=None, # '0 9 * * *',
    concurrency=1,
    max_active_runs=1,
    catchup=False,
    tags=['transaction_data', 'marklogic', 'api'],
) as dag:
    
    # ONE scope per operator.
    # Available scopes: Audit.General, Audit.Exchange, Audit.SharePoint
    audit_general = PythonOperator(
        task_id='audit_general',
        op_kwargs={
            'start_datetime': '{{ execution_date.subtract(days=1) }}',
            'end_datetime': '{{ execution_date }}',
            'scope': 'Audit.General'  
        },
        python_callable=get_daily_data,
        provide_context=True,
    )

    audit_exchange = PythonOperator(
        task_id='audit_exchange',
        op_kwargs={
            'start_datetime': '{{ execution_date.subtract(days=1) }}',
            'end_datetime': '{{ execution_date }}',
            'scope': 'Audit.Exchange'
        },
        python_callable=get_daily_data,
        provide_context=True,
    )

    audit_sharepoint = PythonOperator(
        task_id='audit_sharepoint',
        op_kwargs={
            'start_datetime': '{{ execution_date.subtract(days=1) }}',
            'end_datetime': '{{ execution_date }}',
            'scope': 'Audit.SharePoint'
        },
        python_callable=get_daily_data,
        provide_context=True,
    )

    import_to_marklogic = PythonOperator(
        task_id='import_to_marklogic',
        op_kwargs={
            # No '/' at the end of either input_dir or output_dir
            'input_dir': '{{ ti.xcom_pull(key="input_dir") }}',
            'output_dir': '/o365_analytics/{{ ds_nodash }}',
            'collections': 'O365-Analytics,O365-Analytics-{{ ds_nodash }}',
            'conn_id': 'mlcp_test',
        },
        python_callable=run_mlcp,
        provide_context=True,
    )

    send_success_email = PythonOperator(
        task_id='send_success_email',
        python_callable=send_job_success,
        provide_context=True,
    )


    audit_general >> audit_exchange >> audit_sharepoint >> import_to_marklogic >> send_success_email
