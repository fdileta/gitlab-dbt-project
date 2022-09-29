""" Gitlab.Com Extract and load DAG"""
import os
import string
from tokenize import String
from datetime import datetime, timedelta
import yaml

from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator

from airflow_utils import (
    DATA_IMAGE,
    clone_repo_cmd,
    gitlab_defaults,
    slack_failed_task,
    gitlab_pod_env_vars,
)

from kubernetes_helpers import get_affinity, get_toleration
from kube_secrets import (
    CUSTOMERS_DB_HOST,
    CUSTOMERS_DB_NAME,
    CUSTOMERS_DB_PASS,
    CUSTOMERS_DB_USER,
    GCP_PROJECT,
    GCP_REGION,
    GCP_SERVICE_CREDS,
    GITLAB_COM_DB_HOST,
    GITLAB_COM_DB_NAME,
    GITLAB_COM_DB_PASS,
    GITLAB_COM_DB_USER,
    GITLAB_COM_PG_PORT,
    GITLAB_COM_SCD_PG_PORT,
    GITLAB_COM_CI_DB_NAME,
    GITLAB_COM_CI_DB_HOST,
    GITLAB_COM_CI_DB_PASS,
    GITLAB_COM_CI_DB_PORT,
    GITLAB_COM_CI_DB_USER,
    GITLAB_OPS_DB_USER,
    GITLAB_OPS_DB_PASS,
    GITLAB_OPS_DB_HOST,
    GITLAB_OPS_DB_NAME,
    PG_PORT,
    SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_LOAD_PASSWORD,
    SNOWFLAKE_LOAD_ROLE,
    SNOWFLAKE_LOAD_USER,
    SNOWFLAKE_LOAD_WAREHOUSE,
)

# Load the env vars into a dict and set env vars
env = os.environ.copy()
GIT_BRANCH = env["GIT_BRANCH"]
standard_secrets = [
    GCP_SERVICE_CREDS,
    PG_PORT,
    SNOWFLAKE_LOAD_USER,
    SNOWFLAKE_LOAD_PASSWORD,
    SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_LOAD_WAREHOUSE,
    SNOWFLAKE_LOAD_ROLE,
]


# Dictionary containing the configuration values for the various Postgres DBs
config_dict = {
    "el_customers_scd_db": {
        "cloudsql_instance_name": None,
        "dag_name": "el_customers_scd",
        "env_vars": {"DAYS": "1"},
        "extract_schedule_interval": "0 1 */1 * *",
        "secrets": [
            CUSTOMERS_DB_USER,
            CUSTOMERS_DB_PASS,
            CUSTOMERS_DB_HOST,
            CUSTOMERS_DB_NAME,
        ],
        "start_date": datetime(2019, 5, 30),
        "task_name": "customers",
        "description": "This DAG does full extract & load of customer database(Postgres) to snowflake",
    },
    "el_gitlab_com": {
        "cloudsql_instance_name": None,
        "dag_name": "el_gitlab_com",
        "env_vars": {"HOURS": "96"},
        "extract_schedule_interval": "0 */6 * * *",
        "secrets": [
            GITLAB_COM_DB_USER,
            GITLAB_COM_DB_PASS,
            GITLAB_COM_DB_HOST,
            GITLAB_COM_DB_NAME,
            GITLAB_COM_PG_PORT,
        ],
        "start_date": datetime(2019, 5, 30),
        "task_name": "gitlab-com",
        "description": "This DAG does Incremental extract & load  of gitlab.com database(Postgres) to snowflake",
        "description_incremental": "This DAG does backfill of incremental table extract & load of gitlab.com database(Postgres) to snowflake",
    },
    "el_gitlab_com_ci": {
        "cloudsql_instance_name": None,
        "dag_name": "el_gitlab_com_ci",
        "env_vars": {"HOURS": "96"},
        "extract_schedule_interval": "0 */6 * * *",
        "secrets": [
            GITLAB_COM_CI_DB_NAME,
            GITLAB_COM_CI_DB_HOST,
            GITLAB_COM_CI_DB_PASS,
            GITLAB_COM_CI_DB_PORT,
            GITLAB_COM_CI_DB_USER,
        ],
        "start_date": datetime(2019, 5, 30),
        "task_name": "gitlab-com",
        "description": "This DAG does Incremental extract & load of gitlab.com CI* database(Postgres) to snowflake",
        "description_incremental": "This DAG does backfill of incremental table extract & load of gitlab.com CI* database(Postgres) to snowflake",
    },
    "el_gitlab_com_scd": {
        "cloudsql_instance_name": None,
        "dag_name": "el_gitlab_com_scd",
        "env_vars": {},
        "extract_schedule_interval": "0 1 */1 * *",
        "secrets": [
            GITLAB_COM_DB_USER,
            GITLAB_COM_DB_PASS,
            GITLAB_COM_DB_HOST,
            GITLAB_COM_DB_NAME,
            GITLAB_COM_SCD_PG_PORT,
        ],
        "start_date": datetime(2019, 5, 30),
        "task_name": "gitlab-com-scd",
        "description": "This DAG does Full extract & load of gitlab.com database(Postgres) to snowflake",
    },
    "el_gitlab_com_ci_scd": {
        "cloudsql_instance_name": None,
        "dag_name": "el_gitlab_com_ci_scd",
        "env_vars": {},
        "extract_schedule_interval": "0 3 */1 * *",
        "secrets": [
            GITLAB_COM_CI_DB_NAME,
            GITLAB_COM_CI_DB_HOST,
            GITLAB_COM_CI_DB_PASS,
            GITLAB_COM_CI_DB_PORT,
            GITLAB_COM_CI_DB_USER,
        ],
        "start_date": datetime(2019, 5, 30),
        "task_name": "gitlab-com-scd",
        "description": "This DAG does Full extract & load of gitlab.com database CI* (Postgres) to snowflake",
    },
    "el_gitlab_ops": {
        "cloudsql_instance_name": "ops-db-restore",
        "dag_name": "el_gitlab_ops",
        "env_vars": {"HOURS": "48"},
        "extract_schedule_interval": "0 */6 * * *",
        "secrets": [
            GCP_PROJECT,
            GCP_REGION,
            GITLAB_OPS_DB_USER,
            GITLAB_OPS_DB_PASS,
            GITLAB_OPS_DB_HOST,
            GITLAB_OPS_DB_NAME,
        ],
        "start_date": datetime(2019, 5, 30),
        "task_name": "gitlab-ops",
        "description": "This DAG does Incremental extract & load of Operational database (Postgres) to snowflake",
        "description_incremental": "This DAG does backfill of incrmental table extract & load of Operational database(Postgres) to snowflake",
    },
    "el_gitlab_ops_scd": {
        "cloudsql_instance_name": "ops-db-restore",
        "dag_name": "el_gitlab_ops_scd",
        "env_vars": {"HOURS": "13"},
        "extract_schedule_interval": "0 2 */1 * *",
        "secrets": [
            GCP_PROJECT,
            GCP_REGION,
            GITLAB_OPS_DB_USER,
            GITLAB_OPS_DB_PASS,
            GITLAB_OPS_DB_HOST,
            GITLAB_OPS_DB_NAME,
        ],
        "start_date": datetime(2019, 5, 30),
        "task_name": "gitlab-ops",
        "description": "This DAG does Full extract & load of Operational database (Postgres) to snowflake",
    },
}


def get_task_pool(task_name) -> string:
    """Return airflow pool name"""
    return f"{task_name}-pool"


def is_incremental(raw_query):
    """Determine if the extraction is incremental or full extract i.e. SCD"""
    return "{EXECUTION_DATE}" in raw_query or "{BEGIN_TIMESTAMP}" in raw_query


def use_cloudsql_proxy(dag_name, operation, instance_name):
    """Use cloudsql proxy for connecting to ops Database"""
    return f"""
        {clone_repo_cmd} &&
        cd analytics/orchestration &&
        python ci_helpers.py use_proxy --instance_name {instance_name} --command " \
            python ../extract/postgres_pipeline/postgres_pipeline/main.py tap  \
            ../extract/postgres_pipeline/manifests_decomposed/{dag_name}_db_manifest.yaml {operation}
        "
    """


def get_last_loaded(dag_name: String) -> string:
    """Pull from xcom value  last loaded timestamp for the table"""
    if dag_name == "el_gitlab_ops":
        return None

    return "{{{{ task_instance.xcom_pull('{}', include_prior_dates=True)['max_data_available'] }}}}".format(
        task_identifier + "-pgp-extract"
    )


def generate_cmd(dag_name, operation, cloudsql_instance_name):
    """Generate the command"""
    if cloudsql_instance_name is None:
        return f"""
            {clone_repo_cmd} &&
            cd analytics/extract/postgres_pipeline/postgres_pipeline/ &&
            python main.py tap ../manifests_decomposed/{dag_name}_db_manifest.yaml {operation}
        """

    return use_cloudsql_proxy(dag_name, operation, cloudsql_instance_name)


def extract_manifest(manifest_file_path):
    """Extract postgres pipeline manifest file"""
    with open(manifest_file_path, "r") as file:
        manifest_dict = yaml.load(file, Loader=yaml.FullLoader)
    return manifest_dict


def extract_table_list_from_manifest(manifest_contents):
    """Extract table from the manifest file for which extraction needs to be done"""
    return manifest_contents["tables"].keys()


# Sync DAG
sync_dag_args = {
    "catchup": False,
    "depends_on_past": False,
    "on_failure_callback": slack_failed_task,
    "owner": "airflow",
    "retries": 0,
    "retry_delay": timedelta(minutes=3),
    "dagrun_timeout": timedelta(hours=10),
    "trigger_rule": "all_success",
}
# Extract DAG
extract_dag_args = {
    "catchup": True,
    "depends_on_past": False,
    "on_failure_callback": slack_failed_task,
    "owner": "airflow",
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
    "sla": timedelta(hours=8),
    "sla_miss_callback": slack_failed_task,
    "dagrun_timeout": timedelta(hours=6),
    "trigger_rule": "all_success",
}
# Loop through each config_dict and generate a DAG
for source_name, config in config_dict.items():
    if "scd" not in source_name:
        extract_dag_args["start_date"] = config["start_date"]
        sync_dag_args["start_date"] = config["start_date"]

        extract_dag = DAG(
            f"{config['dag_name']}_db_extract",
            default_args=extract_dag_args,
            schedule_interval=config["extract_schedule_interval"],
            description=config["description"],
        )

        with extract_dag:

            # Actual PGP extract
            file_path = f"analytics/extract/postgres_pipeline/manifests_decomposed/{config['dag_name']}_db_manifest.yaml"
            manifest = extract_manifest(file_path)
            table_list = extract_table_list_from_manifest(manifest)

            for table in table_list:
                # tables that aren't incremental won't be processed by the incremental dag
                if not is_incremental(manifest["tables"][table]["import_query"]):
                    continue

                TASK_TYPE = "db-incremental"
                task_identifier = (
                    f"el-{config['task_name']}-{table.replace('_','-')}-{TASK_TYPE}"
                )

                incremental_cmd = generate_cmd(
                    config["dag_name"],
                    f"--load_type incremental --load_only_table {table}",
                    config["cloudsql_instance_name"],
                )

                incremental_extract = KubernetesPodOperator(
                    **gitlab_defaults,
                    image=DATA_IMAGE,
                    task_id=f"{task_identifier}-pgp-extract",
                    name=f"{task_identifier}-pgp-extract",
                    pool=get_task_pool(config["task_name"]),
                    secrets=standard_secrets + config["secrets"],
                    env_vars={
                        **gitlab_pod_env_vars,
                        **config["env_vars"],
                        "TASK_INSTANCE": "{{ task_instance_key_str }}",
                        "LAST_LOADED": get_last_loaded(config["dag_name"]),
                    },
                    affinity=get_affinity(False),
                    tolerations=get_toleration(False),
                    arguments=[incremental_cmd],
                    do_xcom_push=True,
                )
        globals()[f"{config['dag_name']}_db_extract"] = extract_dag

        incremental_backfill_dag = DAG(
            f"{config['dag_name']}_db_incremental_backfill",
            default_args=sync_dag_args,
            schedule_interval=config["extract_schedule_interval"],
            concurrency=1,
            description=config["description_incremental"],
        )

        with incremental_backfill_dag:

            file_path = f"analytics/extract/postgres_pipeline/manifests_decomposed/{config['dag_name']}_db_manifest.yaml"
            manifest = extract_manifest(file_path)
            table_list = extract_table_list_from_manifest(manifest)
            for table in table_list:
                if is_incremental(manifest["tables"][table]["import_query"]):
                    TASK_TYPE = "backfill"

                    task_identifier = (
                        f"el-{config['task_name']}-{table.replace('_','-')}-{TASK_TYPE}"
                    )

                    sync_cmd = generate_cmd(
                        config["dag_name"],
                        f"--load_type backfill --load_only_table {table}",
                        config["cloudsql_instance_name"],
                    )
                    sync_extract = KubernetesPodOperator(
                        **gitlab_defaults,
                        image=DATA_IMAGE,
                        task_id=task_identifier,
                        name=task_identifier,
                        pool=get_task_pool(config["task_name"]),
                        secrets=standard_secrets + config["secrets"],
                        env_vars={
                            **gitlab_pod_env_vars,
                            **config["env_vars"],
                            "TASK_INSTANCE": "{{ task_instance_key_str }}",
                        },
                        affinity=get_affinity(False),
                        tolerations=get_toleration(False),
                        arguments=[sync_cmd],
                        do_xcom_push=True,
                    )

        globals()[
            f"{config['dag_name']}_db_incremental_backfill"
        ] = incremental_backfill_dag
    else:
        sync_dag_args["start_date"] = config["start_date"]
        sync_dag = DAG(
            f"{config['dag_name']}_db_sync",
            default_args=sync_dag_args,
            schedule_interval=config["extract_schedule_interval"],
            concurrency=8,
            description=config["description"],
        )

        with sync_dag:
            # PGP Extract
            file_path = f"analytics/extract/postgres_pipeline/manifests_decomposed/{config['dag_name']}_db_manifest.yaml"
            manifest = extract_manifest(file_path)
            table_list = extract_table_list_from_manifest(manifest)
            for table in table_list:
                if not is_incremental(manifest["tables"][table]["import_query"]):
                    TASK_TYPE = "db-scd"

                    task_identifier = (
                        f"el-{config['task_name']}-{table.replace('_','-')}-{TASK_TYPE}"
                    )

                    # SCD Task
                    scd_cmd = generate_cmd(
                        config["dag_name"],
                        f"--load_type scd --load_only_table {table}",
                        config["cloudsql_instance_name"],
                    )

                    scd_extract = KubernetesPodOperator(
                        **gitlab_defaults,
                        image=DATA_IMAGE,
                        task_id=task_identifier,
                        name=task_identifier,
                        pool=get_task_pool(config["task_name"]),
                        secrets=standard_secrets + config["secrets"],
                        env_vars={
                            **gitlab_pod_env_vars,
                            **config["env_vars"],
                            "TASK_INSTANCE": "{{ task_instance_key_str }}",
                            "task_id": task_identifier,
                        },
                        arguments=[scd_cmd],
                        affinity=get_affinity(True),
                        tolerations=get_toleration(True),
                        do_xcom_push=True,
                    )
        globals()[f"{config['dag_name']}_db_sync"] = sync_dag
