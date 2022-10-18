import logging
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
    clone_and_setup_extraction_cmd,
    partitions,
)

from kube_secrets import (
    SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_LOAD_ROLE,
    SNOWFLAKE_LOAD_USER,
    SNOWFLAKE_LOAD_WAREHOUSE,
    SNOWFLAKE_LOAD_PASSWORD,
    SNOWFLAKE_PASSWORD,
    SNOWFLAKE_USER,
)

# Load the env vars into a dict and set env vars
env = os.environ.copy()
GIT_BRANCH = env["GIT_BRANCH"]

backfill_param = Variable.get("NAMESPACE_BACKFILL_VAR")

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
    "start_date": datetime(2020, 6, 7),
    "dagrun_timeout": timedelta(hours=48),
}

dag = DAG("saas_usage_ping_backfill", default_args=default_args, schedule_interval=None)


def get_command():
    """
    Namespace, Group, Project, User Level Usage Ping
    Generate execution command to call Python code
    """
    cmd = f"""
            {clone_repo_cmd} &&
            cd analytics/extract/saas_usage_ping/ &&
            python3 usage_ping.py backfill --start_date=$START_DATE --end_date=$END_DATE
        """
    return cmd


def get_task_name(start_date: date, end_date: date) -> str:
    """
    Generate task name
    """

    return f"saas-namespace-usage-ping-{start_date}{end_date}"


def get_pod_env_var(start_date: date, end_date: date) -> dict:
    """
    Get pod environment variables
    """
    pod_env_vars = {
        "START_DATE": start_date,
        "END_DATE": end_date,
        "SNOWFLAKE_SYSADMIN_ROLE": "TRANSFORMER",
        "SNOWFLAKE_LOAD_WAREHOUSE": "USAGE_PING",
    }

    pod_env_vars = {**gitlab_pod_env_vars, **pod_env_vars}

    return pod_env_vars


def generate_task(start_date: date, end_date: date) -> None:
    """
    Generate tasks for backfilling DAG start from Monday,
    as the original pipeline run on Monday
    """

    KubernetesPodOperator(
        **gitlab_defaults,
        image=DATA_IMAGE,
        task_id=get_task_name(start_date=start_date, end_date=end_date),
        name=get_task_name(start_date=start_date, end_date=end_date),
        secrets=secrets,
        env_vars=get_pod_env_var(start_date=start_date, end_date=end_date),
        arguments=[get_command()],
        dag=dag,
    )


def get_monday(day: datetime):
    """
    Get Monday from the input day
    """
    res = day - timedelta(days=day.weekday())

    return res


def get_date_range(start_date: datetime, end_date: datetime) -> list:
    res = []
    date = start_date

    while date < end_date:
        res.append((date, date + timedelta(days=7)))

        date = date + timedelta(days=7)

    return res


start_date = get_monday(backfill_param.get("start_date"))
end_date = backfill_param.get("end_date")

date_range = get_date_range(start_date=start_date, end_date=end_date)

for start, end in date_range:
    get_task_name(start_date=start, end_date=end)
