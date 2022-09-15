import json
import logging
from os import environ as env
from typing import List
from google.cloud import bigquery

from pandas import DataFrame
from big_query_client import BigQueryClient

from gitlabdata.orchestration_utils import (
    snowflake_engine_factory,
    snowflake_stage_load_copy_remove,
)

config_dict = env.copy()


def get_billing_data_query(start_time: str, end_time: str) -> str:
    """
    sql to run in bigquery for daily partition
    """
    date_partition_name = '2022_07_07'
    date_partition_date = '2022-07-07'

    return f"""
        EXPORT DATA OPTIONS(
          uri='gs://gl_gcp_billing_export/test_detail/{date_partition_name}/*.parquet',
          format='PARQUET',
          overwrite=true) AS
        SELECT * 
          , DATE(_PARTITIONTIME) as _partition_date 
        FROM `billing-tools-277316.gitlab_com_detailed_billing.gcp_billing_export_resource_v1_017B02_778F9C_493B83` 
        WHERE DATE(_PARTITIONTIME) = "{date_partition_date}"
    """

if __name__ == "__main__":

    logging.basicConfig(level=20)

    credentials = json.loads(config_dict["GCP_BILLING_ACCOUNT_CREDENTIALS"])

    bq = BigQueryClient(credentials)

    start_time = config_dict["START_TIME"]
    end_time = config_dict["END_TIME"]

    sql_statement = get_billing_data_query(start_time, end_time)

    logging.info(sql_statement)

    result = bq.get_result_from_sql(
        sql_statement,
        project="billing-tools-277316",
        job_config=bigquery.QueryJobConfig(use_legacy_sql=False),
    )
    logging.info("Complete.")
