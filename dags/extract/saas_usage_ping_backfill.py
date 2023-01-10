"""
Unit to generate backfill data for namespace
Need a parameter in json ie:
Name: NAMESPACE_BACKFILL_VAR
Content:
{"start_date": "2022-10-01",
 "end_date": "2022-10-25",
 "metrics_backfill": ['metric_x_last_28_days','some_other_metrics','3rd.metrics'] # this is passed as a list of metrics
}
"""

import os
from datetime import date, datetime, timedelta

from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.models import Variable

from airflow_utils import (
    DATA_IMAGE,
    clone_repo_cmd,
    gitlab_defaults,
    slack_failed_task,
    gitlab_pod_env_vars,
)

from kube_secrets import (
    SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_LOAD_ROLE,
    SNOWFLAKE_LOAD_USER,
    SNOWFLAKE_LOAD_PASSWORD,
    SNOWFLAKE_PASSWORD,
    SNOWFLAKE_USER,
)

env = os.environ.copy()

DAG_NAME = "saas_usage_ping_backfill"

DAG_DESCRIPTION = (
    "This DAG runs on demand to do a backfill "
    "for namespace metrics. "
    "In order to have this DAG run properly, "
    "the variable NAMESPACE_BACKFILL_VAR should be filled"
)
BACKFILL_PARAMETERS = Variable.get("NAMESPACE_BACKFILL_VAR", deserialize_json=True, default_var=None)

secrets = [
    SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_LOAD_ROLE,
    SNOWFLAKE_LOAD_USER,
    SNOWFLAKE_LOAD_PASSWORD,
    SNOWFLAKE_PASSWORD,
    SNOWFLAKE_USER,
]

default_args = {
    "catchup": False,
    "depends_on_past": False,
    "on_failure_callback": slack_failed_task,
    "owner": "airflow",
    "retries": 0,
    "start_date": datetime(2019, 1, 1),
}


def get_command():
    """
    Namespace, Group, Project, User Level Usage Ping
    Generate execution command to call Python code
    """
    cmd = f"""
            {clone_repo_cmd} &&
            cd analytics/extract/saas_usage_ping/ &&
            python3 usage_ping.py backfill --ping_date=$RUN_DATE --namespace_metrics_filter=$METRICS_BACKFILL
        """
    return cmd


def date_to_str(input_date: date) -> str:
    """
    Convert date to string to assign it to DAG name
    """
    return input_date.strftime("%Y%m%d")


def get_task_name(start: date) -> str:
    """
    Generate task name
    """
    start_monday = date_to_str(input_date=start)
    return f"namespace-{start_monday}"


def get_pod_env_var(start: date) -> dict:
    """
    Get pod environment variables
    """

    metrics = get_param_value(param="metrics_backfill")

    pod_env_vars = {
        "RUN_DATE": start.isoformat(),
        "METRICS_BACKFILL": metrics,
        "SNOWFLAKE_SYSADMIN_ROLE": "TRANSFORMER",
        "SNOWFLAKE_LOAD_WAREHOUSE": "USAGE_PING",
    }

    pod_env_vars = {**gitlab_pod_env_vars, **pod_env_vars}

    return pod_env_vars


def get_date_range(start: date, end: date) -> list:
    """
    Generate date range for loop to create tasks
    """
    res = []
    curr_date = start

    while curr_date < end:
        res.append(curr_date.date())

        curr_date += timedelta(days=7)

    return res


def get_monday(day: datetime):
    """
    Get Monday from the input day
    """
    res = day - timedelta(days=day.weekday())

    return res


def generate_task(run_date: date) -> None:
    """
    Generate tasks for back-filling DAG start from Monday,
    as the original pipeline run on Monday
    """

    task_id = task_name = get_task_name(start=run_date)

    env_vars = get_pod_env_var(start=run_date)

    command = get_command()

    return KubernetesPodOperator(
        **gitlab_defaults,
        image=DATA_IMAGE,
        task_id=task_id,
        name=task_name,
        secrets=secrets,
        env_vars=env_vars,
        arguments=[command],
        dag=dag,
    )


def get_param_value(param: str) -> str:
    """
    Return value from the parameter
    """
    res = BACKFILL_PARAMETERS.get(param)

    return res


def get_date(date_param: str) -> datetime:
    """
    Get starting date (Monday in this case)
    """
    res = get_param_value(param=date_param)

    return datetime.strptime(res, "%Y-%m-%d")


start_date = get_date(date_param="start_date")
start_date = get_monday(day=start_date)

end_date = get_date(date_param="end_date")


dag = DAG(
    DAG_NAME,
    default_args=default_args,
    schedule_interval=None,
    concurrency=2,
    description=DAG_DESCRIPTION,
)

for run in get_date_range(start=start_date, end=end_date):
    generate_task(run_date=run)
