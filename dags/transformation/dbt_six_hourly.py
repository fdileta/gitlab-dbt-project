"""
## Info about DAG
This DAG is responsible for running a six-hourly refresh on models tagged with the "six_hourly" label from Monday to Saturday.
"""

import os
from datetime import datetime, timedelta

from croniter import croniter
from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.operators.python_operator import ShortCircuitOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow_utils import (
    DBT_IMAGE,
    dbt_install_deps_cmd,
    gitlab_defaults,
    gitlab_pod_env_vars,
    slack_failed_task,
)
from kube_secrets import (
    GIT_DATA_TESTS_PRIVATE_KEY,
    GIT_DATA_TESTS_CONFIG,
    SALT,
    SALT_EMAIL,
    SALT_IP,
    SALT_NAME,
    SALT_PASSWORD,
    SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_PASSWORD,
    SNOWFLAKE_TRANSFORM_ROLE,
    SNOWFLAKE_TRANSFORM_SCHEMA,
    SNOWFLAKE_TRANSFORM_WAREHOUSE,
    SNOWFLAKE_USER,
    SNOWFLAKE_LOAD_PASSWORD,
    SNOWFLAKE_LOAD_ROLE,
    SNOWFLAKE_LOAD_USER,
    SNOWFLAKE_LOAD_WAREHOUSE,
    MCD_DEFAULT_API_ID,
    MCD_DEFAULT_API_TOKEN,
)

# Load the env vars into a dict and set Secrets
env = os.environ.copy()
GIT_BRANCH = env["GIT_BRANCH"]
pod_env_vars = {**gitlab_pod_env_vars, **{}}

# This value is set based on the commit hash setter task in dbt_snapshot
pull_commit_hash = """export GIT_COMMIT="{{ var.value.dbt_hash }}" """


# Default arguments for the DAG
default_args = {
    "catchup": False,
    "depends_on_past": False,
    "on_failure_callback": slack_failed_task,
    "owner": "airflow",
    "retries": 0,
    "sla": timedelta(hours=8),
    "sla_miss_callback": slack_failed_task,
    "start_date": datetime(2019, 1, 1, 0, 0, 0),
    "trigger_rule": TriggerRule.ALL_DONE,
    "dagrun_timeout": timedelta(hours=6),
}

# Define all the  required secret
secrets_list = [
    GIT_DATA_TESTS_PRIVATE_KEY,
    GIT_DATA_TESTS_CONFIG,
    SALT,
    SALT_EMAIL,
    SALT_IP,
    SALT_NAME,
    SALT_PASSWORD,
    SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_USER,
    SNOWFLAKE_PASSWORD,
    SNOWFLAKE_LOAD_PASSWORD,
    SNOWFLAKE_LOAD_ROLE,
    SNOWFLAKE_LOAD_USER,
    SNOWFLAKE_LOAD_WAREHOUSE,
    SNOWFLAKE_TRANSFORM_ROLE,
    SNOWFLAKE_TRANSFORM_WAREHOUSE,
    SNOWFLAKE_TRANSFORM_SCHEMA,
    MCD_DEFAULT_API_ID,
    MCD_DEFAULT_API_TOKEN,
]

# Create the DAG
dag = DAG(
    "dbt_six_hourly",
    description="This DAG is responsible for refreshing models at minute 0 past every 6th hour.",
    default_args=default_args,
    schedule_interval="0 */6 * * 1-6",
)
dag.doc_md = __doc__


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


dbt_evaluate_run_date_task = ShortCircuitOperator(
    task_id="evaluate_dbt_run_date",
    python_callable=lambda: dbt_evaluate_run_date(datetime.now(), "0 */6 * * 1-6"),
    dag=dag,
)

# run sfdc_opportunity models on large warehouse
dbt_six_hourly_models_command = f"""
    {pull_commit_hash} &&
    {dbt_install_deps_cmd} &&
    export SNOWFLAKE_TRANSFORM_WAREHOUSE="TRANSFORMING_L" &&
    dbt --no-use-colors run --profiles-dir profile --target prod --selector six_hourly_salesforce_opportunity; ret=$?;
    target/run_results.json --project-name gitlab-analysis;
    python ../../orchestration/upload_dbt_file_to_snowflake.py results; exit $ret
"""

dbt_six_hourly_models_task = KubernetesPodOperator(
    **gitlab_defaults,
    image=DBT_IMAGE,
    task_id="dbt_six_hourly_models_command",
    name="dbt-six-hourly-models-run",
    secrets=secrets_list,
    env_vars=pod_env_vars,
    arguments=[dbt_six_hourly_models_command],
    dag=dag,
)

# dbt-results
dbt_results_cmd = f"""
    {pull_commit_hash} &&
    {dbt_install_deps_cmd} &&
    dbt --no-use-colors run --profiles-dir profile --target prod --models sources.dbt+ ; ret=$?;
    target/run_results.json --project-name gitlab-analysis;
    python ../../orchestration/upload_dbt_file_to_snowflake.py results; exit $ret
"""
dbt_results = KubernetesPodOperator(
    **gitlab_defaults,
    image=DBT_IMAGE,
    task_id="dbt-results",
    name="dbt-results",
    trigger_rule="all_done",
    secrets=secrets_list,
    env_vars=pod_env_vars,
    arguments=[dbt_results_cmd],
    dag=dag,
)

(
    dbt_evaluate_run_date_task
    >> dbt_six_hourly_models_task
    >> dbt_results
)
