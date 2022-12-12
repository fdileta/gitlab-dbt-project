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
    data_test_ssh_key_cmd,
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


DATA_SCIENCE_NAMESPACE_SEG_SSH_REPO = "git@gitlab.com:gitlab-data/data-science-projects/namespace-segmentation.git"
DATA_SCIENCE_NAMESPACE_SEG_HTTP_REPO = "https://gitlab_analytics:$GITLAB_ANALYTICS_PRIVATE_TOKEN@gitlab.com/gitlab-data/data-science-projects/namespace-segmentation.git"


clone_data_science_namespace_seg_repo_cmd = f"""
    {data_test_ssh_key_cmd} &&
    if [[ -z "$GIT_COMMIT" ]]; then
        export GIT_COMMIT="HEAD"
    fi
    if [[ -z "$GIT_DATA_TESTS_PRIVATE_KEY" ]]; then
        export REPO="{DATA_SCIENCE_NAMESPACE_SEG_HTTP_REPO}";
        else
        export REPO="{DATA_SCIENCE_NAMESPACE_SEG_SSH_REPO}";
    fi &&
    echo "git clone -b change-cloudpickle --single-branch --depth 1 $REPO" &&
    git clone -b change-cloudpickle --single-branch --depth 1 $REPO &&
    echo "checking out commit $GIT_COMMIT" &&
    cd namespace-segmentation &&
    git checkout $GIT_COMMIT &&
    echo pwd &&
    cd .."""


# Default arguments for the DAG
default_args = {
    "catchup": False,
    "depends_on_past": False,
    "on_failure_callback": slack_failed_task,
    "owner": "airflow",
    "retries": 0,
    "retry_delay": timedelta(minutes=10),
    "start_date": datetime(2022, 8, 9),
    "dagrun_timeout": timedelta(hours=2),
}

# Create the DAG
# Run on the 3rd day of every month at 4AM
dag = DAG(
    "ds_namespace_segmentation",
    default_args=default_args,
    schedule_interval="0 4 3 * *",
)

# Task 1
namespace_seg_scoring_command = f"""
    {clone_data_science_namespace_seg_repo_cmd} &&
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
