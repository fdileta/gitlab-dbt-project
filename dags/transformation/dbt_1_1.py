"""
## Info about DAG
This DAG is responsible for doing incremental model refresh for both product, non product model,workspace model followed by dbt-test and dbt-result from Monday to Saturday.
"""

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow_utils import (
    DBT_IMAGE_1_1,
    clone_repo_cmd,
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
)

# Load the env vars into a dict and set Secrets
env = os.environ.copy()
GIT_BRANCH = """{{ var.value.dbt_1_1_branch }}"""
pod_env_vars = {**gitlab_pod_env_vars, **{}}

# This value is set based on the commit hash setter task in dbt_snapshot
pull_commit_hash = """export GIT_COMMIT="{{ var.value.dbt_1_1_hash }}" """

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
]

# Create the DAG
dag = DAG(
    "dbt_1_1",
    description="This DAG is responsible for doing incremental model refresh",
    default_args=default_args,
    schedule_interval="45 8 * * MON-SAT",
)
dag.doc_md = __doc__


# BranchPythonOperator functions
def dbt_run_or_refresh(timestamp: datetime) -> str:
    """
    Use the current date to determine whether to do a full-refresh or a
    normal run.

    If it is a Sunday and the current hour is less than the schedule_interval
    for the DAG, then run a full_refresh. This ensures only one full_refresh is
    run every week.
    """

    ## TODO: make this not hardcoded
    current_weekday = timestamp.isoweekday()

    # run a full-refresh once per week (on sunday early AM)
    if current_weekday == 7:
        return "dbt-full-refresh"
    else:
        return "dbt-non-product-models-run"


# run non-product models on small warehouse
dbt_non_product_models_command = f"""
    {pull_commit_hash} &&
    {dbt_install_deps_cmd} &&
    export SNOWFLAKE_TRANSFORM_WAREHOUSE="TRANSFORMING_L" &&
    dbt --no-use-colors run --profiles-dir profile --target prod --exclude tag:datasiren tag:product legacy.sheetload legacy.snapshots sources.gitlab_dotcom sources.sheetload sources.sfdc sources.zuora sources.dbt workspaces.*; ret=$?;
    python ../../orchestration/upload_dbt_file_to_snowflake.py results; exit $ret
"""

dbt_non_product_models_task = KubernetesPodOperator(
    **gitlab_defaults,
    image=DBT_IMAGE_1_1,
    task_id="dbt-non-product-models-run",
    name="dbt-non-product-models-run",
    secrets=secrets_list,
    env_vars=pod_env_vars,
    arguments=[dbt_non_product_models_command],
    dag=dag,
)


# run product models on large warehouse
dbt_product_models_command = f"""
    {pull_commit_hash} &&
    {dbt_install_deps_cmd} &&
    export SNOWFLAKE_TRANSFORM_WAREHOUSE="TRANSFORMING_XL" &&
    dbt --no-use-colors run --profiles-dir profile --target prod --models tag:product --exclude workspaces.* ; ret=$?;
    python ../../orchestration/upload_dbt_file_to_snowflake.py results; exit $ret
"""

dbt_product_models_task = KubernetesPodOperator(
    **gitlab_defaults,
    image=DBT_IMAGE_1_1,
    task_id="dbt-product-models-run",
    name="dbt-product-models-run",
    secrets=secrets_list,
    env_vars=pod_env_vars,
    arguments=[dbt_product_models_command],
    dag=dag,
)


# dbt-test
dbt_test_cmd = f"""
    {pull_commit_hash} &&
    {dbt_install_deps_cmd} &&
    export SNOWFLAKE_TRANSFORM_WAREHOUSE="TRANSFORMING_S" &&
    dbt --no-use-colors test --profiles-dir profile --target prod --exclude tag:datasiren snowplow legacy.snapshots source:gitlab_dotcom source:salesforce source:zuora workspaces.*; ret=$?;
    python ../../orchestration/upload_dbt_file_to_snowflake.py manifest_reduce; 
    python ../../orchestration/upload_dbt_file_to_snowflake.py test; exit $ret
"""
dbt_test = KubernetesPodOperator(
    **gitlab_defaults,
    image=DBT_IMAGE_1_1,
    task_id="dbt-test",
    name="dbt-test",
    trigger_rule="all_done",
    secrets=secrets_list,
    env_vars=pod_env_vars,
    arguments=[dbt_test_cmd],
    dag=dag,
)

# dbt-results
dbt_results_cmd = f"""
    {pull_commit_hash} &&
    {dbt_install_deps_cmd} &&
    dbt --no-use-colors run --profiles-dir profile --target prod --models sources.dbt+ ; ret=$?;
    python ../../orchestration/upload_dbt_file_to_snowflake.py results; exit $ret
"""
dbt_results = KubernetesPodOperator(
    **gitlab_defaults,
    image=DBT_IMAGE_1_1,
    task_id="dbt-results",
    name="dbt-results",
    trigger_rule="all_done",
    secrets=secrets_list,
    env_vars=pod_env_vars,
    arguments=[dbt_results_cmd],
    dag=dag,
)

# dbt-workspaces
dbt_workspaces_command = f"""
    {pull_commit_hash} &&
    {dbt_install_deps_cmd} &&
    dbt --no-use-colors run --profiles-dir profile --target prod --models workspaces.* --exclude workspaces.workspace_data_science.* workspaces.workspace_data.tdf.*; ret=$?;
    python ../../orchestration/upload_dbt_file_to_snowflake.py results; exit $ret
"""
dbt_workspaces = KubernetesPodOperator(
    **gitlab_defaults,
    image=DBT_IMAGE_1_1,
    task_id="dbt-workspaces",
    name="dbt-workspaces",
    trigger_rule="all_done",
    secrets=secrets_list,
    env_vars=pod_env_vars,
    arguments=[dbt_workspaces_command],
    dag=dag,
)

# dbt-workspaces-xl
dbt_workspaces_xl_command = f"""
    {pull_commit_hash} &&
    {dbt_install_deps_cmd} &&
    export SNOWFLAKE_TRANSFORM_WAREHOUSE="TRANSFORMING_L" &&
    dbt --no-use-colors run --profiles-dir profile --target prod --models workspaces.workspace_data_science.* ; ret=$?;
    python ../../orchestration/upload_dbt_file_to_snowflake.py results; exit $ret
"""
dbt_workspaces_xl = KubernetesPodOperator(
    **gitlab_defaults,
    image=DBT_IMAGE_1_1,
    task_id="dbt-workspaces-xl",
    name="dbt-workspaces-xl",
    trigger_rule="all_done",
    secrets=secrets_list,
    env_vars=pod_env_vars,
    arguments=[dbt_workspaces_xl_command],
    dag=dag,
)

# dbt-workspaces
dbt_workspaces_test_command = f"""
    {pull_commit_hash} &&
    {dbt_install_deps_cmd} &&
    dbt --no-use-colors test --profiles-dir profile --target prod --models workspaces.* ; ret=$?;
    python ../../orchestration/upload_dbt_file_to_snowflake.py test; exit $ret
"""
dbt_workspaces_test = KubernetesPodOperator(
    **gitlab_defaults,
    image=DBT_IMAGE_1_1,
    task_id="dbt-workspaces-test",
    name="dbt-workspaces-test",
    trigger_rule="all_done",
    secrets=secrets_list,
    env_vars=pod_env_vars,
    arguments=[dbt_workspaces_test_command],
    dag=dag,
)

(
    dbt_non_product_models_task
    >> dbt_product_models_task
    >> dbt_test
    >> dbt_workspaces
    >> dbt_workspaces_xl
    >> dbt_workspaces_test
    >> dbt_results
)
