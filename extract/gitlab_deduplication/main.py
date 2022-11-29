from asyncio.log import logger
import logging
import yaml
from os import environ as env
from datetime import datetime
from fire import Fire
from typing import Dict
from gitlabdata.orchestration_utils import (
    snowflake_engine_factory,
    query_executor,
)

snowflake_engine = snowflake_engine_factory(env, "LOADER")


def manifest_reader(file_path: str) -> Dict[str, Dict]:
    """
    Read a yaml manifest file into a dictionary and return it.
    """

    with open(file_path, "r") as file:
        manifest_dict = yaml.load(file, Loader=yaml.FullLoader)

    return manifest_dict


def build_table_name(
    table_name: str, table_prefix: str = None, table_suffix: str = None
) -> str:
    if table_prefix is None and table_suffix is None:
        return table_name
    elif table_prefix is None and table_suffix is not None:
        return table_name + table_suffix
    elif table_prefix is not None and table_suffix is None:
        return table_prefix + table_name
    else:
        return table_prefix + table_name + table_suffix

def create_table_name(manifest_dict,table_name):
    table_suffix = "_" + datetime.now().strftime("%Y%m%d")
    table_prefix = manifest_dict["generic_info"]["table_prefix"]

    if table_prefix:
        bkp_table_name = build_table_name(table_name, table_prefix, table_suffix)
        original_table_name = build_table_name(table_name, table_prefix)
    else:
        bkp_table_name = build_table_name(table_name, table_suffix)
        original_table_name = build_table_name(table_name)
    return bkp_table_name,original_table_name

def create_backup_table(
    manifest_dict: dict,
    table_name: str,
) -> bool:
    raw_database = manifest_dict["raw_database"]
    backup_schema_name = manifest_dict["generic_info"]["backup_schema"]
    raw_schema = manifest_dict["generic_info"]["raw_schema"]
    bkp_table_name,original_table_name =create_table_name(manifest_dict,table_name)
    create_backup_table = f"CREATE TABLE {raw_database}.{backup_schema_name}.{bkp_table_name} CLONE {raw_database}.{raw_schema}.{original_table_name};"
    logging.info(f"create_backup_table")
    query_executor(snowflake_engine, create_backup_table)
    return True

def get_column_list():
    
    return True

def create_temp_table(
    manifest_dict: dict,
    table_name: str,
):
    get_column_list()
    return True


def deduplicate_scd_tables(manifest_dict: Dict, table_name: str) -> bool:
    """
    Extract the values from the manifest
    """
    table_prefix = manifest_dict["generic_info"]["table_prefix"]
    raw_schema = manifest_dict["generic_info"]["raw_schema"]
    raw_database = manifest_dict["raw_database"]
    # Create backup table
    create_backup_table(
        manifest_dict,  table_name
    )
    if create_backup_table:
        create_temp_table(manifest_dict,table_name)


def main(file_path: str = "t_gitlab_com_scd_advance_metadata_manifest.yml") -> None:
    """
    Read table name from manifest file and decide if the table exist in the database. Check if the advance metadata column `_task_instance`
    is present in the table.
    Check for backup schema is present in snowflake.
    Check for old backup table in snowflake and if present check if the creation date is older than 15 days. If yes drop backup table if not create a new backup table.
    Once the backup is complete, create  table with deduplicate records.
    Swap the table with original table.
    Drop the swapped temp table.
    """
    # Process the manifest
    logging.info(f"Reading manifest at location: {file_path}")
    manifest_dict = manifest_reader(file_path)
    scd_tables_list = manifest_dict["scd_tables"]
    # Add raw database name to the manifest
    manifest_dict.update({"raw_database": env.copy()["SNOWFLAKE_LOAD_DATABASE"]})
    # iterate through each table and check if it exist
    for table in scd_tables_list:
        logging.info(f"Proceeding with table {table} for deduplication")
        deduplicate_scd_tables(manifest_dict, table)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logging.getLogger("snowflake.connector.cursor").disabled = True
    logging.getLogger("snowflake.connector.connection").disabled = True
    config_dict = env.copy()
    snowflake_engine = snowflake_engine_factory(config_dict, "LOADER")
    Fire({"deduplication": main})
