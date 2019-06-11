import os
from datetime import datetime, timedelta

from airflow import DAG

from kube_secrets import *
from airflow_utils import slack_failed_task, gitlab_defaults
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator


# Load the env vars into a dict and set Secrets
env = os.environ.copy()
GIT_BRANCH = env["GIT_BRANCH"]
pod_env_vars = {
    "CI_PROJECT_DIR": "/analytics",
    "SNOWFLAKE_LOAD_DATABASE": "RAW" if GIT_BRANCH == "master" else f"{GIT_BRANCH}_RAW",
    "SNOWFLAKE_TRANSFORM_DATABASE": "ANALYTICS"
    if GIT_BRANCH == "master"
    else f"{GIT_BRANCH}_ANALYTICS",
}

# Default arguments for the DAG
default_args = {
    "catchup": False,
    "depends_on_past": False,
    "on_failure_callback": slack_failed_task,
    "owner": "airflow",
    "retries": 0,
    "retry_delay": timedelta(minutes=3),
    "start_date": datetime(2019, 5, 31),
}

# Set the command for the container
sync_cmd = f"""
    git clone -b {env['GIT_BRANCH']} --single-branch https://gitlab.com/gitlab-data/analytics.git --depth 1 &&
    export PYTHONPATH="$CI_PROJECT_DIR/orchestration/:$PYTHONPATH" &&
    cd analytics/extract/postgres/ &&
    python tap_postgres/tap_postgres.py tap manifests/gitlab_com_manifest.yaml --sync
"""

# Create the DAG
dag = DAG(
    "gitlab_com_db_sync", default_args=default_args, schedule_interval="0 2 */1 * *"
)

# Task 1
gitlab_com_db_sync_extract = KubernetesPodOperator(
    **gitlab_defaults,
    image="registry.gitlab.com/gitlab-data/data-image/data-image:latest",
    task_id="gitlab-com-db-sync",
    name="gitlab-com-db-sync",
    secrets=[
        GITLAB_COM_DB_USER,
        GITLAB_COM_DB_PASS,
        GITLAB_COM_DB_HOST,
        GITLAB_COM_DB_NAME,
        PG_PORT,
        SNOWFLAKE_LOAD_USER,
        SNOWFLAKE_LOAD_PASSWORD,
        SNOWFLAKE_ACCOUNT,
        SNOWFLAKE_LOAD_WAREHOUSE,
        SNOWFLAKE_LOAD_ROLE,
    ],
    env_vars=pod_env_vars,
    cmds=["/bin/bash", "-c"],
    arguments=[sync_cmd],
    dag=dag,
)

# Set the command for the task
scd_cmd = f"""
    git clone -b {env['GIT_BRANCH']} --single-branch https://gitlab.com/gitlab-data/analytics.git --depth 1 &&
    export PYTHONPATH="$CI_PROJECT_DIR/orchestration/:$PYTHONPATH" &&
    cd analytics/extract/postgres/ &&
    python tap_postgres/tap_postgres.py tap manifests/gitlab_com_manifest.yaml --scd_only
"""
# Task 2
gitlab_com_db_scd_extract = KubernetesPodOperator(
    **gitlab_defaults,
    image="registry.gitlab.com/gitlab-data/data-image/data-image:latest",
    task_id="gitlab-com-db-scd",
    name="gitlab-com-db-scd",
    secrets=[
        GITLAB_COM_DB_USER,
        GITLAB_COM_DB_PASS,
        GITLAB_COM_DB_HOST,
        GITLAB_COM_DB_NAME,
        PG_PORT,
        SNOWFLAKE_LOAD_USER,
        SNOWFLAKE_LOAD_PASSWORD,
        SNOWFLAKE_ACCOUNT,
        SNOWFLAKE_LOAD_WAREHOUSE,
        SNOWFLAKE_LOAD_ROLE,
    ],
    env_vars=pod_env_vars,
    cmds=["/bin/bash", "-c"],
    arguments=[scd_cmd],
    dag=dag,
)

gitlab_com_db_sync_extract >> gitlab_com_db_scd_extract
