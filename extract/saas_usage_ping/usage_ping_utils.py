"""
Utils unit for Automated Service ping
"""

import os

import pandas as pd
import requests
from gitlabdata.orchestration_utils import (dataframe_uploader,
                                            snowflake_engine_factory)

SCHEMA_NAME = "saas_usage_ping"


class EngineFactory:
    """
    Class to manage connection to Snowflake
    """

    def __init__(self):
        self.loader_engine = None
        self.PROCESSING_WAREHOUSE = "LOADER"

    def env_copy(self):
        self.config_vars = os.environ.env.copy()

    def connect(self):
        """
        Connect to engine factory, return connection
        """
        self.loader_engine = snowflake_engine_factory(
            self.config_vars, self.PROCESSING_WAREHOUSE
        )
        return self.loader_engine.connect()

    def dispose(self) -> None:
        """
        Dispose from engine factory
        """
        self.loader_engine.dispose()


def upload_to_snowflake(self, table_name: str, data: pd.DataFrame) -> None:
    """
    Upload dataframe to Snowflake
    """
    dataframe_uploader(
        dataframe=data,
        engine=self.loader_engine,
        table_name=table_name,
        schema=SCHEMA_NAME,
    )


def convert_response_to_json(response: requests.Response):
    """
    Convert Response object to json
    """
    pass


def get_response(url: str, header: dict) -> requests.Response:
    """
    get response from the server
    """

    pass
