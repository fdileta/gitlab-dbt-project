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
        self.headers = {"PRIVATE-TOKEN": config_dict["GITLAB_ANALYTICS_PRIVATE_TOKEN"]}

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
