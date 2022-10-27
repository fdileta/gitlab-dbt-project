"""
Daily Dag for Sales Analytics notebooks
"""


import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow_utils import (
    ANALYST_IMAGE,
    gitlab_defaults,
    gitlab_pod_env_vars,
    slack_failed_task,
    clone_repo_cmd,
    SALES_ANALYTICS_NOTEBOOKS_PATH,
    get_sales_analytics_notebooks,
)
from kube_secrets import (
    SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_LOAD_PASSWORD,
    SNOWFLAKE_DATA_SCIENCE_LOAD_ROLE,
    SNOWFLAKE_LOAD_USER,
    SNOWFLAKE_LOAD_WAREHOUSE,
    GITLAB_ANALYTICS_PRIVATE_TOKEN,
)

# Load the env vars into a dict and set Secrets
env = os.environ.copy()
GIT_BRANCH = env["GIT_BRANCH"]
pod_env_vars = {**gitlab_pod_env_vars, **{}}

# Default arguments for the DAG
default_args = {
    "catchup": False,
    "depends_on_past": False,
    "on_failure_callback": slack_failed_task,
    "owner": "airflow",
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
    "start_date": datetime(2022, 10, 12),
    "dagrun_timeout": timedelta(hours=2),
}

# Create the DAG
# Schedule to run daily at 6AM
dag = DAG(
    "sales_analytics_daily_notebooks",
    default_args=default_args,
    schedule_interval="0 6 * * *",
    concurrency=1,
)

DAILY_NOTEBOOKS_PATH = f'{SALES_ANALYTICS_NOTEBOOKS_PATH}/daily/'
notebooks = get_sales_analytics_notebooks(frequency='daily')

# Task 1
start = DummyOperator(task_id="Start", dag=dag)

for notebook, task_name in notebooks.items():
    # Set the command for the container for loading the data
    container_cmd_load = f"""
        {clone_repo_cmd} &&
        cd {DAILY_NOTEBOOKS_PATH} &&
        papermill {notebook} -p is_local_development False
        """
    task_identifier = f"{task_name}"
    # Task 2
    sales_analytics_daily_notebooks = KubernetesPodOperator(
        **gitlab_defaults,
        image=ANALYST_IMAGE,
        task_id=task_identifier,
        name=task_identifier,
        pool="default_pool",
        secrets=[
            SNOWFLAKE_ACCOUNT,
            SNOWFLAKE_LOAD_PASSWORD,
            SNOWFLAKE_DATA_SCIENCE_LOAD_ROLE,
            SNOWFLAKE_LOAD_USER,
            SNOWFLAKE_LOAD_WAREHOUSE,
            GITLAB_ANALYTICS_PRIVATE_TOKEN,
        ],
        env_vars=pod_env_vars,
        arguments=[container_cmd_load],
        dag=dag,
    )
    start >> sales_analytics_daily_notebooks