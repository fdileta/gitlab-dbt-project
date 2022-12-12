"""
Utils unit for Automated Service ping
"""


import json
from os import environ as env

import pandas as pd
import requests
from gitlabdata.orchestration_utils import dataframe_uploader, snowflake_engine_factory


class EngineFactory:
    """
    Class to manage connection to Snowflake
    """

    def __init__(self):
        self.connected = False
        self.config_vars = env.copy()
        self.loader_engine = None
        self.processing_warehouse = "LOADER"
        self.schema_name = "saas_usage_ping"

    def connect(self):
        """
        Connect to engine factory, return connection object
        """
        self.loader_engine = snowflake_engine_factory(
            self.config_vars, self.processing_warehouse
        )
        self.connected = True

        return self.loader_engine.connect()

    def dispose(self) -> None:
        """
        Dispose from engine factory
        """
        if self.connected:
            self.loader_engine.dispose()

    def upload_to_snowflake(self, table_name: str, data: pd.DataFrame) -> None:
        """
        Upload dataframe to Snowflake
        """
        dataframe_uploader(
            dataframe=data,
            engine=self.loader_engine,
            table_name=table_name,
            schema=self.schema_name,
        )


class Utils:
    """
    Utils class for service ping
    """

    def __init__(self):
        config_dict = env.copy()
        self.headers = {"PRIVATE-TOKEN": config_dict.get("GITLAB_ANALYTICS_PRIVATE_TOKEN", None)}
        self.encoding = "utf8"

        self.meta_api_columns = [
            "recorded_at",
            "version",
            "edition",
            "recording_ce_finished_at",
            "recording_ee_finished_at",
            "uuid",
        ]

    @staticmethod
    def convert_response_to_json(response: requests.Response):
        """
        Convert Response object to json
        """
        return json.loads(response.text)

    def get_response(self, url: str) -> requests.Response:
        """
        get response from the server
        """
        try:
            response = requests.get(url=url, headers=self.headers)
            response.raise_for_status()

            return response
        except requests.ConnectionError as e:
            raise ConnectionError("Error requesting job") from e

    def get_json_response(self, url):
        """
        get prepared response in json format
        """
        response = self.get_response(url=url)

        return self.convert_response_to_json(response=response)

    def keep_meta_data(self, json_data: dict) -> dict:
        """
        Pick up meta-data we want to expose in Snowflake from the original file

        param json_file: json file downloaded from API
        return: dict
        """

        meta_data = {
            meta_api_column: json_data.get(meta_api_column, "")
            for meta_api_column in self.meta_api_columns
        }

        return meta_data

    def save_to_json_file(self, file_name: str, json_data: dict) -> None:
        """
        param file_name: str
        param json_data: dict
        return: None
        """
        with open(file=file_name, mode="w", encoding=self.encoding) as wr_file:
            json.dump(json_data, wr_file)

    def load_from_json_file(self, file_name: str):
        """
        Load from json file
        """
        with open(file=file_name, mode='r', encoding=self.encoding) as file:
            return json.load(file)
