import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow_utils import (
    DATA_IMAGE,
    clone_and_setup_extraction_cmd,
    gitlab_defaults,
    slack_failed_task,
)

from kube_secrets import (
    GCP_BILLING_ACCOUNT_CREDENTIALS,
    SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_LOAD_DATABASE,
    SNOWFLAKE_LOAD_PASSWORD,
    SNOWFLAKE_LOAD_ROLE,
    SNOWFLAKE_LOAD_USER,
    SNOWFLAKE_LOAD_WAREHOUSE,
)

from kubernetes_helpers import get_affinity, get_toleration
from yaml import load, safe_load, YAMLError

env = os.environ.copy()

GIT_BRANCH = env["GIT_BRANCH"]
pod_env_vars = {
    "CI_PROJECT_DIR": "/analytics",
    "SNOWFLAKE_LOAD_DATABASE": "RAW" if GIT_BRANCH == "master" else f"{GIT_BRANCH}_RAW",
}

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
    "gcs_external",
    default_args=default_args,
    schedule_interval="0 12 * * *",
    concurrency=1,
)


# don't add a newline at the end of this because it gets added to in the K8sPodOperator arguments

airflow_home = env["AIRFLOW_HOME"]

with open(f"{airflow_home}/analytics/extract/gcs_external/src/gcp_billing/gcs_external.yml", "r") as file:
    try:
        stream = safe_load(file)
    except YAMLError as exc:
        print(exc)

for export in stream['exports']:

  billing_extract_command = (
      f"{clone_and_setup_extraction_cmd} && python gcs_external/src/gcs_external.py"
  )
  
  billing_operator = KubernetesPodOperator(
      **gitlab_defaults,
      image=DATA_IMAGE,
      task_id=export['name'],
      name=export['name'],
      secrets=[
          GCP_BILLING_ACCOUNT_CREDENTIALS,
          SNOWFLAKE_ACCOUNT,
          SNOWFLAKE_LOAD_DATABASE,
          SNOWFLAKE_LOAD_ROLE,
          SNOWFLAKE_LOAD_USER,
          SNOWFLAKE_LOAD_WAREHOUSE,
          SNOWFLAKE_LOAD_PASSWORD,
      ],
      env_vars={
          **pod_env_vars,
					**export,
          "EXPORT_DATE": "{{ execution_date }}",
      },
      affinity=get_affinity(False),
      tolerations=get_toleration(False),
      arguments=[billing_extract_command],
      dag=dag,
  )