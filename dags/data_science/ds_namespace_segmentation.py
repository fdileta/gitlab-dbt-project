"""
Namespace Segmentation DAG
"""

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow_utils import (
    ANALYST_IMAGE,
    gitlab_defaults,
    gitlab_pod_env_vars,
    slack_failed_task,
    get_data_science_project_command,
    DATA_SCIENCE_NAMESPACE_SEG_HTTP_REPO,
    DATA_SCIENCE_NAMESPACE_SEG_SSH_REPO,
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
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
    "start_date": datetime(2022, 8, 9),
    "dagrun_timeout": timedelta(hours=2),
}

# Prep the cmd
clone_data_science_namespace_segmentation_repo_cmd = get_data_science_project_command(
    DATA_SCIENCE_NAMESPACE_SEG_HTTP_REPO,
    DATA_SCIENCE_NAMESPACE_SEG_SSH_REPO,
    "namespace-segmentation",
)

# Create the DAG
# Run on the 3rd day of every month at 4AM
dag = DAG(
    "ds_namespace_segmentation",
    default_args=default_args,
    schedule_interval="0 4 3 * *",
)

# Task 1
namespace_seg_scoring_command = f"""
    {clone_data_science_namespace_segmentation_repo_cmd} &&
    cd namespace-segmentation/prod &&
    papermill scoring_code.ipynb -p is_local_development False
"""
KubernetesPodOperator(
    **gitlab_defaults,
    image=ANALYST_IMAGE,
    task_id="namespace-segmentation",
    name="namespace-segmentation",
    secrets=[
        SNOWFLAKE_ACCOUNT,
        SNOWFLAKE_LOAD_PASSWORD,
        SNOWFLAKE_DATA_SCIENCE_LOAD_ROLE,
        SNOWFLAKE_LOAD_USER,
        SNOWFLAKE_LOAD_WAREHOUSE,
        GITLAB_ANALYTICS_PRIVATE_TOKEN,
    ],
    env_vars=pod_env_vars,
    arguments=[namespace_seg_scoring_command],
    dag=dag,
)
