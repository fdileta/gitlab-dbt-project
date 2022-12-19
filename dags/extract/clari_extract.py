from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

from airflow_utils import (
    DATA_IMAGE,
    clone_and_setup_extraction_cmd,
    gitlab_defaults,
    slack_failed_task,
    gitlab_pod_env_vars,
)

from kube_secrets import (
    SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_LOAD_PASSWORD,
    SNOWFLAKE_LOAD_ROLE,
    SNOWFLAKE_LOAD_USER,
    SNOWFLAKE_LOAD_WAREHOUSE,
    MAILGUN_API_KEY,
)

# Define the default arguments for the DAG
default_args = {
    "catchup": True,
    "depends_on_past": False,
    "on_failure_callback": slack_failed_task,
    "owner": "airflow",
    "retry_delay": timedelta(minutes=1),
    "sla": timedelta(hours=24),
    "sla_miss_callback": slack_failed_task,
    "start_date": datetime(2022, 12, 17),
}


# Define the DAG
dag = DAG(
    'clari_extract',
    default_args=default_args,
    schedule_interval="0 8 * * *"
)


def get_fiscal_quarter(dt):
    fiscal_year = dt.year
    if dt.month in [2, 3, 4]:
        fiscal_quarter = 1
    elif dt.month in [5, 6, 7]:
        fiscal_quarter = 2
    elif dt.month in [8, 9, 10]:
        fiscal_quarter = 3
    else:
        fiscal_quarter = 4

    # Format the fiscal year and quarter as a string
    fiscal_year_quarter = f'{fiscal_year}_Q{fiscal_quarter}'
    return fiscal_year_quarter


def get_current_fiscal_quarter(dt):
    current_fiscal_quarter = get_fiscal_quarter(dt)
    # quarters are expected to be in a list as they may be manually passed in
    return current_fiscal_quarter


def get_previous_fiscal_quarter(dt):
    current_fiscal_quarter = get_fiscal_quarter(dt)
    fiscal_quarter_prefix = current_fiscal_quarter[:-1]
    current_quarter_int = int(current_fiscal_quarter[-1])

    if current_quarter_int == 1:
        return fiscal_quarter_prefix + '4'
    return fiscal_quarter_prefix + f'{current_quarter_int - 1}'


# Define a function that takes an argument
def get_quarters_to_run(task_schedule):
    yesterday = datetime.now() - timedelta(days=1)
    today = datetime.now()
    print(f'Executing task with task_schedule: {task_schedule}')

    if task_schedule == 'daily':
        return [get_current_fiscal_quarter(yesterday)]
        '''
        if {{config.time_periods}}:
            return {{config.time_periods}}
        else:
            return [get_current_fiscal_quarter()]
        '''

    elif task_schedule == 'quarterly':
        return [get_previous_fiscal_quarter(today)]


'''
def dbt_evaluate_run_date(timestamp: datetime, exclude_schedule: str) -> bool:
    """
    Simple function written to exclude a given schedule, currently only checking against dates.
    Designed to exclude the first Sundays of a given month from the schedule as this is the only date
    the full refresh now runs on.
    :param timestamp: Current run date
    :param exclude_schedule: Cron schedule to exclude
    :return: Bool, false if it is the first Sunday of the month.
    """
    next_run = croniter(exclude_schedule).get_next(datetime)
    # Excludes the first sunday of every month, this is captured by the regular full refresh.
    if next_run.date() == timestamp.date():
        return False

    return True

quarterly_short_circuit_task = ShortCircuitOperator(
    task_id="evaluate_dbt_run_date",
    python_callable=lambda: dbt_evaluate_run_date(datetime.now(), "45 8 * * SUN#1"),
    dag=dag,
)

'''

daily_tasks = []
for i, quarter_to_run in enumerate(get_quarters_to_run('daily')):
    daily_task = BashOperator(
        dag=dag,
        task_id=f"{quarter_to_run}",
        bash_command=f"echo {quarter_to_run}",
    )
    if daily_tasks:
        daily_tasks[-1] >> daily_task
    daily_tasks.append(daily_task)

'''
for i, quarter_to_run in enumerate(get_quarter_to_run('daily')):
    clari_extract_command = (
        f"{clone_and_setup_extraction_cmd} && "
        # f"python clari.py --quarter_to_run {quarter_to_run}"
        f"echo {quarter_to_run}"
    )

    daily_task = KubernetesPodOperator(
        **gitlab_defaults,
        image=DATA_IMAGE,
        task_id=f"clari-extract-{e}",
        name=f"clari-extract-{e}",
        secrets=[
            SNOWFLAKE_ACCOUNT,
            SNOWFLAKE_LOAD_ROLE,
            SNOWFLAKE_LOAD_USER,
            SNOWFLAKE_LOAD_WAREHOUSE,
            SNOWFLAKE_LOAD_PASSWORD,
            CLARI_API_KEY,
        ],
        env_vars={
            **pod_env_vars,
            "START_TIME": "{{ execution_date.isoformat() }}",
            "END_TIME": "{{ yesterday_ds }}",
        },
        affinity=get_affinity(False),
        tolerations=get_toleration(False),
        arguments=[clari_extract_command],
        dag=dag,
    )

    if daily_tasks:
        daily_tasks[-1] >> daily_task
    daily_tasks.append(daily_task)
'''

daily_tasks
# daily_task
# daily_tasks[-1] >> quarterly_short_circuit_task >> QuarterlyKubeTask`
