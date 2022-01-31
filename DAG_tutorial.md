# How to create DAG
> See default values in `dag_template.py`.

> Always modify any lines marked with `#!`.

### 5 steps to create a DAG

1. **Import necessary modules, operators and functions.**

2. **Define `default_args` that will be passed to DAG and every Task**.  
    Many arguments are optional.  
    Arguments that MUST be defined:
    * `owner`
    * `start_date`

    Full list of default arguments:
    ```python
    default_args = {
        'owner': 'com_bi',
        'depends_on_past': True, # If True and past task failed, new ones won't run on schedule
        'retries': 1,  # How many times retry the task
        'retry_delay': timedelta(minutes=5),  # Ex: If task fails, retry it 1 times after 5 min
        'email': Variable.get('urgent_emails'),  # What emails to send notifications to. Can pass a list
        'email_on_retry': True,  # Send default notification email if Task retries
        'email_on_failure': True, # Send a default notification email if Task fails
        'on_failure_callback': None, # Function to call on failure
        'on_success_callback': None, # Function to call on success
        'sla': timedelta(minutes=30),  # Set default Service Level Agreement time for DAG
        'sla_miss_callback': None  # Function to call when SLA is missed. For example, email function 
        'user_defined_macros': {}, # Can pass your own macros to templates
        'params': {}, # Dictionary of params that can be used in templates in tasks via `params.<param_name>`
    }
    ```

3. **Create a DAG instance using `with` syntax**.  
    Full list of DAG arguments:
    ```python
    with DAG(
        dag_id="dag_name_with_underscores", # Unique name for DAG. Good to match it with file name
        description="DAG description", # Human-readable description of the DAG
        start_date=pendulum.datetime(year, month, date, tzinfo=pendulum.timezone('Asia/Tokyo')), # When to start scheduling the DAG. Airflow will automatically convert JST to UTC if timezone is set.
        end_date=pendulum.datetime(year, month, day, tzinfo=pendulum.timezone('Asia/Tokyo')), # When to stop scheduling DAG
        schedule_interval='0 0 * * *', # Cron schedule in format 'minute hour day month day_of_the_week'. * means "all\every". When timezone in start_date is set, it uses timezone's time to schedule.
        default_args=default_args,
        concurrency=3, # How many tasks can be run at the same time across all scheduled DAG Runs.
        max_active_runs=1, # How many DAG Runs can be active at the same time.
        catchup=False,  # If True, schedule_interval is not empty and start_date is earlier than today - will create DAG Runs for each previous possible run and run them.
        tags=['dag_tag'],  # Tags for sorting and filtering in Airflow UI
    ) as dag:
    ```

4. **Add Tasks using Operators and define them**.  
    All arguments for PythonOperator:
    ```python
    data_task = PythonOperator(
        task_id='descriptive_task_id', # Unique ID for the task
        python_callable=function_name, # Function to be executed by PythonOperatoroperator
        op_args=[], # List of positional arguments passed to the function
        op_kwargs={ # Dict of keyword arguments passed to the function
             'argument': 'value'   
        },
        provide_context=True, # Pass DAG metadata to operator. Very useful. Always set to True and add **context to your function arguments if you create custom function.
        templates_dict={} # pass macros templated values available as context['templates_dict'] variable inside operator
    )
    ```

5. **Define Tasks execution flow**.  
    Flow dependencies are defined using `>>` syntax.  
    Examples:
    ```python
    task_1 >> task_2  # First task_1 will be performed, then task_2

    task_1 >> [task_2, task_3] >> task_4  # First task_1 will be performed,
    # then both task_2 and task_3 will be performed at the same time,
    # and when they are both successfully finished - task_4 will be performed
    ```


Now you're a DAG master!