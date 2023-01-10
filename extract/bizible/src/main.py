from api import BizibleSnowFlakeExtractor
from os import environ as env
import logging
import datetime
from fire import Fire
from typing import Dict
import yaml
from dateutil import parser as date_parser


def manifest_reader(file_path: str) -> Dict[str, Dict]:
    """
    Read a yaml manifest file into a dictionary and return it.
    """

    with open(file_path, "r") as file:
        manifest_dict = yaml.load(file, Loader=yaml.FullLoader)

    return manifest_dict


def filter_manifest(manifest_dict: Dict, load_only_table: str = None) -> Dict:
    # When load_only_table specified reduce manifest to keep only relevant table config
    if load_only_table and load_only_table in manifest_dict["tables"].keys():
        manifest_dict["tables"] = {
            load_only_table: manifest_dict["tables"][load_only_table]
        }

    return manifest_dict


def main(file_path: str, load_only_table: str = None) -> None:
    config_dict = env.copy()
    start_date = date_parser.parse(config_dict["START_TIME"]) - datetime.timedelta(
        hours=2
    )
    end_date = start_date + datetime.timedelta(hours=13)
    logging.info(f"Running from {start_date} to {end_date}")
    extractor = BizibleSnowFlakeExtractor(config_dict)

    logging.info(f"Reading manifest at location: {file_path}")
    manifest_dict = manifest_reader(file_path)
    # When load_only_table specified reduce manifest to keep only relevant table config
    manifest_dict = filter_manifest(manifest_dict, load_only_table)

    for table in manifest_dict["tables"]:
        logging.info(f"Processing Table: {table}")
        table_dict = manifest_dict["tables"][table]
        if table_dict:
            date_column = table_dict.get("date_column")
        else:
            date_column = ""
        extractor.process_bizible_file(
            start_date, end_date, table, date_column, full_refresh=False
        )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logging.getLogger("snowflake.connector.cursor").disabled = True
    logging.getLogger("snowflake.connector.connection").disabled = True
    Fire({"tap": main})
