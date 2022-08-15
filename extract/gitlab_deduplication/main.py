import logging
from xmlrpc.client import boolean
import yaml
from fire import Fire
from typing import Dict
from gitlabdata.orchestration_utils import (snowflake_engine_factory,
    query_executor,)

def manifest_reader(file_path: str) -> Dict[str, Dict]:
    """
    Read a yaml manifest file into a dictionary and return it.
    """

    with open(file_path, "r") as file:
        manifest_dict = yaml.load(file, Loader=yaml.FullLoader)

    return manifest_dict

def build_table_name(table_name:str,table_prefix:str = None,table_suffix:str = None) -> str:
        return table_prefix+table_name+table_suffix

def check_backup_table_exist(backup_schema_name:str,table_name:str,table_prefix:str) -> boolean:
    return True


def deduplicate_scd_tables(manifest_dict: Dict,table_name: str) -> boolean:
        backup_schema_name=manifest_dict["generic_info"]["backup_schema"]
        backup_retention_policy=manifest_dict["generic_info"]["backup_retention_policy"]
        table_prefix=manifest_dict["generic_info"]["table_prefix"]
        raw_schema=manifest_dict["generic_info"]["raw_schema"]
        check_backup_table_exist(backup_schema_name,table_name,table_prefix)

def main(file_path: str = 't_gitlab_com_scd_advance_metadata_manifest.yml') -> None:
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
    scd_tables_list=manifest_dict["scd_tables"]

    #iterate through each table and check if it exist 
    for table in scd_tables_list:
        deduplicate_scd_tables(manifest_dict,table)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logging.getLogger("snowflake.connector.cursor").disabled = True
    logging.getLogger("snowflake.connector.connection").disabled = True
    Fire({"deduplication": main})
