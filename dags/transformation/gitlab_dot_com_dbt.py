"""Gitlab COM DBT DAG """
import os
from datetime import datetime

from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator


from airflow_utils import (
    gitlab_defaults,
    slack_failed_task,
    DBT_IMAGE,
    dbt_install_deps_nosha_cmd,
    gitlab_pod_env_vars,
    run_command_test_exclude,
)
from kubernetes_helpers import get_affinity, get_toleration
from kube_secrets import (
    GIT_DATA_TESTS_CONFIG,
    GIT_DATA_TESTS_PRIVATE_KEY,
    SALT,
    SALT_EMAIL,
    SALT_IP,
    SALT_NAME,
    SALT_PASSWORD,
    SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_LOAD_USER,
    SNOWFLAKE_PASSWORD,
    SNOWFLAKE_TRANSFORM_ROLE,
    SNOWFLAKE_TRANSFORM_SCHEMA,
    SNOWFLAKE_TRANSFORM_WAREHOUSE,
    SNOWFLAKE_USER,
    MCD_DEFAULT_API_ID,
    MCD_DEFAULT_API_TOKEN,
)

# Load the env vars into a dict and set env vars
env = os.environ.copy()
GIT_BRANCH = env["GIT_BRANCH"]

dbt_secrets = [
    GIT_DATA_TESTS_CONFIG,
    GIT_DATA_TESTS_PRIVATE_KEY,
    SALT,
    SALT_EMAIL,
    SALT_IP,
    SALT_NAME,
    SALT_PASSWORD,
    SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_LOAD_USER,
    SNOWFLAKE_PASSWORD,
    SNOWFLAKE_TRANSFORM_ROLE,
    SNOWFLAKE_TRANSFORM_SCHEMA,
    SNOWFLAKE_TRANSFORM_WAREHOUSE,
    SNOWFLAKE_USER,
    MCD_DEFAULT_API_ID,
    MCD_DEFAULT_API_TOKEN,
]


# Dictionary containing the configuration values for the various Postgres
# DBs for which we need to run DBT snapshots,
# De dupe model and DBT test on source and model.
config_dict = {
    "t_gitlab_customers_db": {
        "dag_name": "t_gitlab_customers_db_dbt",
        "dbt_name": "customers",
        "start_date": datetime(2022, 9, 12),
        "dbt_schedule_interval": "0 5 * * *",
        "task_name": "t_customers",
        "description": "This DAG does incremental refresh of the gitlab customer database,run snapshot on source table and DBT test",
    },
    "t_gitlab_com_db": {
        "dag_name": "t_gitlab_com_db_dbt",
        "dbt_name": "gitlab_dotcom",
        "start_date": datetime(2022, 9, 12),
        "dbt_schedule_interval": "0 7 * * *",
        "task_name": "t_gitlab_dotcom",
        "description": "This DAG does Incremental Refresh gitlab.com source table,run snapshot on source table and DBT test ",
    },
    "t_gitlab_ops_db": {
        "dag_name": "t_gitlab_ops_db_dbt",
        "dbt_name": "gitlab_ops",
        "start_date": datetime(2022, 9, 12),
        "dbt_schedule_interval": "30 6 * * *",
        "task_name": "t_gitlab_ops_db",
        "description": "This DAG does Incremental Refresh gitlab ops database source table ,run snapshot on source table and DBT test",
    },
}

# DAG argument for scheduling the DAG
dbt_dag_args = {
    "catchup": False,
    "depends_on_past": False,
    "on_failure_callback": slack_failed_task,
    "owner": "airflow",
    "retries": 0,
    "trigger_rule": "all_success",
}


def dbt_tasks(dbt_module_name, dbt_task_name):
    """Define all the DBT Task"""
    # Snapshot source data
    snapshot_cmd = f"""
        {dbt_install_deps_nosha_cmd} &&
        export SNOWFLAKE_TRANSFORM_WAREHOUSE="TRANSFORMING_L" &&
        dbt snapshot --profiles-dir profile --target prod --select path:snapshots/{dbt_module_name}; ret=$?;
        montecarlo import dbt-run-results \
        target/run_results.json --project-name gitlab-analysis;
        python ../../orchestration/upload_dbt_file_to_snowflake.py snapshots; exit $ret
    """
    snapshot_task = KubernetesPodOperator(
        **gitlab_defaults,
        image=DBT_IMAGE,
        task_id=f"{dbt_task_name}-source-snapshot",
        trigger_rule="all_done",
        name=f"{dbt_task_name}-source-snapshot",
        secrets=dbt_secrets,
        env_vars=gitlab_pod_env_vars,
        arguments=[snapshot_cmd],
        affinity=get_affinity(False),
        tolerations=get_toleration(False),
    )

    # Run de dupe / rename /scd model
    model_run_cmd = f"""
        {dbt_install_deps_nosha_cmd} &&
        export SNOWFLAKE_TRANSFORM_WAREHOUSE="TRANSFORMING_L" &&
        dbt run --profiles-dir profile --target prod --models +sources.{dbt_module_name}; ret=$?;
        montecarlo import dbt-run-results \
        target/run_results.json --project-name gitlab-analysis;
        python ../../orchestration/upload_dbt_file_to_snowflake.py results; exit $ret
    """
    dedupe_dbt_model_task = KubernetesPodOperator(
        **gitlab_defaults,
        image=DBT_IMAGE,
        task_id=f"{dbt_task_name}-source-model-run",
        trigger_rule="all_done",
        name=f"{dbt_task_name}-source-model-run",
        secrets=dbt_secrets,
        env_vars=gitlab_pod_env_vars,
        arguments=[model_run_cmd],
        affinity=get_affinity(False),
        tolerations=get_toleration(False),
    )

    # Test all source models
    model_test_cmd = f"""
        {dbt_install_deps_nosha_cmd} &&
        dbt test --profiles-dir profile --target prod --models +sources.{dbt_module_name} {run_command_test_exclude}; ret=$?;
        montecarlo import dbt-run-results \
        target/run_results.json --project-name gitlab-analysis;
        python ../../orchestration/upload_dbt_file_to_snowflake.py test; exit $ret
    """
    source_schema_model_test = KubernetesPodOperator(
        **gitlab_defaults,
        image=DBT_IMAGE,
        task_id=f"{dbt_task_name}-model-test",
        trigger_rule="all_done",
        name=f"{dbt_task_name}-model-test",
        secrets=dbt_secrets,
        env_vars=gitlab_pod_env_vars,
        arguments=[model_test_cmd],
        affinity=get_affinity(False),
        tolerations=get_toleration(False),
    )

    return snapshot_task, dedupe_dbt_model_task, source_schema_model_test


# Loop through each config_dict and generate a DAG
for source_name, config in config_dict.items():
    dbt_dag_args["start_date"] = config["start_date"]

    dbt_transform_dag = DAG(
        f"{config['dag_name']}",
        default_args=dbt_dag_args,
        schedule_interval=config["dbt_schedule_interval"],
        description=config["description"],
    )

    with dbt_transform_dag:
        dbt_name = f"{config['dbt_name']}"
        dbt_task_identifier = f"{config['task_name']}"
        (
            dbt_snapshot_task,
            dbt_dedupe_model_run,
            dbt_source_schema_model_test,
        ) = dbt_tasks(dbt_name, dbt_task_identifier)

    # DAG flow using 3 DBT steps
    dbt_snapshot_task >> dbt_dedupe_model_run >> dbt_source_schema_model_test

    globals()[f"{config['dag_name']}"] = dbt_transform_dag
