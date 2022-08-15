import os
from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow_utils import (
    DATA_IMAGE,
    clone_repo_cmd,
    gitlab_defaults,
    gitlab_pod_env_vars,
    slack_failed_task,)

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
standard_secrets = [
    SNOWFLAKE_LOAD_USER,
    SNOWFLAKE_LOAD_PASSWORD,
    SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_LOAD_WAREHOUSE,
    SNOWFLAKE_LOAD_ROLE,
]

scd_cmd=f"""
            {clone_repo_cmd} &&
            cd analytics/extract/gitlab_deduplication/ &&
            python main.py deduplication ../extract/gitlab_deduplication/manifest_deduplication/t_gitlab_com_deduplication_table_manifest.yaml 
"""
scd_deduplication = KubernetesPodOperator(
    **gitlab_defaults,
    image=DATA_IMAGE,
    task_id="scd_table_deduplication_task",
    name="scd_table_deduplication_task",
    pool="default",
    secrets=standard_secrets,
    env_vars=gitlab_pod_env_vars,
    arguments=[scd_cmd],
    affinity=get_affinity(True),
    tolerations=get_toleration(True),
)

scd_deduplication

scd_deduplication_dag = DAG(
        "scd_deduplication",
        scd_deduplication,
        default_args={
        "catchup": False,
        "depends_on_past": False,
        "on_failure_callback": slack_failed_task,
        "owner": "airflow",
        "retries": 0,
    },
        schedule_interval='* * * * *',
        concurrency=1,
    )

