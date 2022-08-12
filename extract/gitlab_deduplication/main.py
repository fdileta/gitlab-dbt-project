import logging
import yaml
from fire import Fire
from typing import Dict

def manifest_reader(file_path: str) -> Dict[str, Dict]:
    """
    Read a yaml manifest file into a dictionary and return it.
    """

    with open(file_path, "r") as file:
        manifest_dict = yaml.load(file, Loader=yaml.FullLoader)

    return manifest_dict

def main(file_path: str = 'extract/gitlab_deduplication/manifest_deduplication'
                          '/t_gitlab_com_scd_advance_metadata_manifest.yml') -> None:
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
    print(manifest_dict)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logging.getLogger("snowflake.connector.cursor").disabled = True
    logging.getLogger("snowflake.connector.connection").disabled = True
    Fire({"tap": main})
