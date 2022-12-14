import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from kubernetes_helpers import get_affinity, get_toleration
from yaml import safe_load, YAMLError
from airflow_utils import (
    clone_and_setup_extraction_cmd,
    DATA_IMAGE,
    DBT_IMAGE,
    dbt_install_deps_nosha_cmd,
    gitlab_defaults,
    gitlab_pod_env_vars,
    slack_failed_task,
)

from kube_secrets import (
    GCP_BILLING_ACCOUNT_CREDENTIALS,
    GIT_DATA_TESTS_CONFIG,
    GIT_DATA_TESTS_PRIVATE_KEY,
    MCD_DEFAULT_API_ID,
    MCD_DEFAULT_API_TOKEN,
    SALT_EMAIL,
    SALT_IP,
    SALT_NAME,
    SALT_PASSWORD,
    SALT,
    SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_LOAD_PASSWORD,
    SNOWFLAKE_LOAD_ROLE,
    SNOWFLAKE_LOAD_USER,
    SNOWFLAKE_LOAD_WAREHOUSE,
    SNOWFLAKE_PASSWORD,
    SNOWFLAKE_TRANSFORM_ROLE,
    SNOWFLAKE_TRANSFORM_SCHEMA,
    SNOWFLAKE_TRANSFORM_WAREHOUSE,
    SNOWFLAKE_USER,
)

dbt_secrets = [
    GIT_DATA_TESTS_CONFIG,
    GIT_DATA_TESTS_PRIVATE_KEY,
    MCD_DEFAULT_API_ID,
    MCD_DEFAULT_API_TOKEN,
    SALT_EMAIL,
    SALT_IP,
    SALT_NAME,
    SALT_PASSWORD,
    SALT,
    SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_LOAD_PASSWORD,
    SNOWFLAKE_LOAD_ROLE,
    SNOWFLAKE_LOAD_USER,
    SNOWFLAKE_LOAD_WAREHOUSE,
    SNOWFLAKE_PASSWORD,
    SNOWFLAKE_TRANSFORM_ROLE,
    SNOWFLAKE_TRANSFORM_SCHEMA,
    SNOWFLAKE_TRANSFORM_WAREHOUSE,
    SNOWFLAKE_USER,
]

env = os.environ.copy()

GIT_BRANCH = env["GIT_BRANCH"]
pod_env_vars = gitlab_pod_env_vars

default_args = {
    "catchup": True,
    "depends_on_past": False,
    "on_failure_callback": slack_failed_task,
    "owner": "airflow",
    # "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "sla": timedelta(hours=24),
    "sla_miss_callback": slack_failed_task,
    # Only has data from March 2018
    "start_date": datetime(2018, 3, 27),
}

dag = DAG(
    "external_gcs_gcp_billing",
    default_args=default_args,
    schedule_interval="0 10 * * *",
    concurrency=1,
)


airflow_home = env["AIRFLOW_HOME"]

external_table_run_cmd = f"""
    {dbt_install_deps_nosha_cmd} &&
    dbt run-operation stage_external_sources \
        --args "select: source gcp_billing" --profiles-dir profile; ret=$?;
"""
dbt_task_name = "dbt-gcp-billing-external-table-refresh"
dbt_external_table_run = KubernetesPodOperator(
    **gitlab_defaults,
    image=DBT_IMAGE,
    task_id=dbt_task_name,
    trigger_rule="all_done",
    name=dbt_task_name,
    secrets=dbt_secrets,
    env_vars=gitlab_pod_env_vars,
    arguments=[external_table_run_cmd],
    dag=dag,
)

with open(
    f"{airflow_home}/analytics/extract/gcs_external/src/gcp_billing/gcs_external.yml",
    "r",
) as yaml_file:
    try:
        stream = safe_load(yaml_file)
    except YAMLError as exc:
        print(exc)

for export in stream["exports"]:

    export_name = export["name"]

    billing_extract_command = f"""
    {clone_and_setup_extraction_cmd} && 
    python gcs_external/src/gcp_billing/gcs_external.py --export_name={export_name}
    """

    task_name = export["name"]

    billing_operator = KubernetesPodOperator(
        **gitlab_defaults,
        image=DATA_IMAGE,
        task_id=export_name,
        name=export_name,
        secrets=[GCP_BILLING_ACCOUNT_CREDENTIALS],
        env_vars={**pod_env_vars, "EXPORT_DATE": "{{ yesterday_ds }}",},
        affinity=get_affinity(False),
        tolerations=get_toleration(False),
        arguments=[billing_extract_command],
        dag=dag,
    )

    billing_operator >> dbt_external_table_run
