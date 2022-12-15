import logging
import os
from datetime import datetime
from numpy import busday_count

from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow_utils import (
    DBT_IMAGE,
    dbt_install_deps_nosha_cmd,
    gitlab_defaults,
    gitlab_pod_env_vars,
    number_of_dbt_threads_argument,
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

# Load the env vars into a dict
env = os.environ.copy()
GIT_BRANCH = env["GIT_BRANCH"]
# schedule : “At minute 0 past hour 5, 11, 17, and 23 on
# every day-of-week from Monday through Friday.”
dag_schedule = "0 5,11,17,23 * * 1-5"

pod_env_vars = {**gitlab_pod_env_vars}

# Default arguments for the DAG
default_args = {
    "catchup": False,
    "depends_on_past": False,
    "on_failure_callback": slack_failed_task,
    "owner": "airflow",
    "start_date": datetime(2020, 7, 30, 0, 0, 0),
}


def return_branch_by_bday(**kwargs):
    """
    Returns name of a task to be triggered by branching operator based on the current business day in the calendar month.
    For business days 1-8 dbt-netsuite-actuals-income-cogs-opex will be triggered,
    otherwise no operation will be performed
    """
    beg_of_month = datetime.today().replace(day=1).date()
    today = datetime.today().date()
    if busday_count(beg_of_month, today) <= 8:
        return "dbt-netsuite-actuals-income-cogs-opex"
    else:
        return "do_nothing"


# Create the DAG
dag = DAG(
    dag_id="dbt_netsuite_actuals_income_cogs_opex",
    default_args=default_args,
    schedule_interval=dag_schedule,
    description="\nThis DAG runs netsuite_actuals_income_cogs_opex model and "
    "all parent models on business days 1-8",
)

dbt_cmd = f"""
    {dbt_install_deps_nosha_cmd} &&
    dbt run --profiles-dir profile --target prod --models +netsuite_actuals_income_cogs_opex {number_of_dbt_threads_argument(4)}; ret=$?;
    montecarlo import dbt-run --run-results \
    target/run_results.json --project-name gitlab-analysis;
    python ../../orchestration/upload_dbt_file_to_snowflake.py results; exit $ret
"""

logging.info(dbt_cmd)

branching = BranchPythonOperator(
    task_id="branching",
    python_callable=return_branch_by_bday,
    provide_context=True,
    dag=dag,
)

dbt_poc = KubernetesPodOperator(
    **gitlab_defaults,
    image=DBT_IMAGE,
    task_id="dbt-netsuite-actuals-income-cogs-opex",
    name="dbt-netsuite-actuals-income-cogs-opex",
    secrets=[
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
        SNOWFLAKE_TRANSFORM_ROLE,
        SNOWFLAKE_TRANSFORM_WAREHOUSE,
        SNOWFLAKE_TRANSFORM_SCHEMA,
        SNOWFLAKE_LOAD_PASSWORD,
        SNOWFLAKE_LOAD_ROLE,
        SNOWFLAKE_LOAD_USER,
        SNOWFLAKE_LOAD_WAREHOUSE,
        MCD_DEFAULT_API_ID,
        MCD_DEFAULT_API_TOKEN,
    ],
    env_vars=pod_env_vars,
    arguments=[dbt_cmd],
    dag=dag,
)

kick_off_dag = DummyOperator(task_id="run_this_first", dag=dag)
do_nothing = DummyOperator(task_id="do_nothing", dag=dag)

kick_off_dag >> branching
branching >> do_nothing
branching >> dbt_poc
