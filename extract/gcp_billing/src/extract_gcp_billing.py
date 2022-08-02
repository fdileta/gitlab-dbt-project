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


def get_billing_data_query(start_date: str) -> str:
    """
    sql to run in bigquery for daily partition
    """
    return f"""
        SELECT
          billing_account_id,
          service,
          sku,
          usage_start_time,
          usage_end_time,
          project,
          labels,
          system_labels,
          location,
          export_time,
          cost,
          currency,
          currency_conversion_rate,
          usage,
          credits,
          invoice,
          cost_type,
          DATE(_PARTITIONTIME)
        FROM gitlab_com_billing.gcp_billing_export_v1_017B02_778F9C_493B83
        WHERE DATE(_PARTITIONTIME)= '{start_date}'
    """


def write_date_json(date: str, df: DataFrame) -> List[str]:
    """
    Chunks the dataframe into 10,000 rows each
    then writes each chunk locally.
    Chunks to avoid any possible issue with Snowflake file size constraint.
    """

    logging.info(f"{df.shape[0]} rows to write")

    file_names = []

    row_chunk_size = 10000
    for i in range(0, df.shape[0], row_chunk_size):
        chunk = df[i : i + row_chunk_size]
        file_name = f"gcp_billing_reporting_data_{date}_{i//row_chunk_size}.json"
        logging.info(f"Writing file {file_name}")

        chunk.to_json(file_name, orient="records", date_format="iso")

        logging.info(f"{file_name} written")
        file_names.append(file_name)

    return file_names


if __name__ == "__main__":

    logging.basicConfig(level=20)

    credentials = json.loads(config_dict["GCP_BILLING_ACCOUNT_CREDENTIALS"])

    bq = BigQueryClient(credentials)

    start_time = config_dict["START_TIME"]

    snowflake_engine = snowflake_engine_factory(config_dict, "LOADER")

    sql_statement = get_billing_data_query(start_time)

    df_result = bq.get_dataframe_from_sql(
        sql_statement,
        project="billing-tools-277316",
        job_config=bigquery.QueryJobConfig(use_legacy_sql=False),
    )

    file_names = write_date_json(start_time, df_result)

    for file_name in file_names:
        snowflake_stage_load_copy_remove(
            file_name,
            "gcp_billing.gcp_billing_load",
            "gcp_billing.gcp_billing_export_combined",
            snowflake_engine,
        )

    logging.info("Complete.")
