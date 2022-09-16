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


def get_billing_data_query(export: dict, export_date: str) -> str:
    """
    sql to run in bigquery for daily partition
    """
    

    if export['partition_date_part'] == 'd':
       partition = export_date
    elif export['partition_date_part'] == 'm':
       partition = export_date

    select_string = ', '.join(export['selected_columns'])

    return f"""
        EXPORT DATA OPTIONS(
          uri='{export['bucket_path']}/{partition}/*.parquet',
          format='PARQUET',
          overwrite=true
          ) AS
            SELECT {select_string}
            FROM `{export['table']}`
            WHERE {export['partition_column']} = '{partition}'
    """

if __name__ == "__main__":

    logging.basicConfig(level=20)

    credentials = json.loads(config_dict["GCP_BILLING_ACCOUNT_CREDENTIALS"])

    bq = BigQueryClient(credentials)

    export_date = config_dict["EXPORT_DATE"]

    sql_statement = get_billing_data_query(export, export_date)

    logging.info(sql_statement)

    # result = bq.get_result_from_sql(
    #     sql_statement,
    #     project="billing-tools-277316",
    #     job_config=bigquery.QueryJobConfig(use_legacy_sql=False),
    # )
    logging.info("Complete.")
