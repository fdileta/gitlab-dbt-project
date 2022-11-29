import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow_utils import (
    DATA_IMAGE,
    clone_repo_cmd,
    gitlab_defaults,
    slack_failed_task,
)

from kubernetes_helpers import get_affinity, get_toleration
from kube_secrets import (
    SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_LOAD_PASSWORD,
    SNOWFLAKE_LOAD_ROLE,
    SNOWFLAKE_LOAD_USER,
    SNOWFLAKE_LOAD_WAREHOUSE,
)

env = os.environ.copy()
GIT_BRANCH = env["GIT_BRANCH"]
pod_env_vars = {
    "SNOWFLAKE_LOAD_DATABASE": "RAW"
    if GIT_BRANCH == "master"
    else f"{GIT_BRANCH.upper()}_RAW",
    "CI_PROJECT_DIR": "/analytics",
}

scd_cmd = f"""
            {clone_repo_cmd} &&
            cd analytics/extract/gitlab_deduplication/ &&
            python main.py deduplication manifest_deduplication/t_gitlab_com_deduplication_table_manifest.yaml 
"""
default_args = {
    "catchup": False,
    "depends_on_past": False,
    "on_failure_callback": slack_failed_task,
    "owner": "airflow",
    "retries": 0,
    "start_date": datetime(2020, 1, 1),
}

scd_deduplication_dag = DAG(
    "scd_deduplication", default_args=default_args, schedule_interval=None
)

scd_deduplication = KubernetesPodOperator(
    **gitlab_defaults,
    image=DATA_IMAGE,
    task_id="scd_table_deduplication_task",
    name="scd_table_deduplication_task",
    secrets=[
        SNOWFLAKE_ACCOUNT,
        SNOWFLAKE_LOAD_PASSWORD,
        SNOWFLAKE_LOAD_ROLE,
        SNOWFLAKE_LOAD_USER,
        SNOWFLAKE_LOAD_WAREHOUSE,
    ],
    env_vars=pod_env_vars,
    arguments=[scd_cmd],
    affinity=get_affinity(False),
    tolerations=get_toleration(False),
    dag=scd_deduplication_dag,
)
