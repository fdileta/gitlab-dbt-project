"""
Utils unit for Automated Service ping
"""

import datetime
import json
import re
from hashlib import md5
from os import environ as env

import pandas as pd
import requests
from gitlabdata.orchestration_utils import dataframe_uploader, snowflake_engine_factory

TRANSFORMED_INSTANCE_SQL_QUERIES_FILE = "transformed_instance_queries.json"
META_DATA_INSTANCE_SQL_QUERIES_FILE = "meta_data_instance_queries.json"
NAMESPACE_FILE = "usage_ping_namespace_queries.json"

HAVING_CLAUSE_PATTERN = re.compile(
    "HAVING.*COUNT.*APPROVAL_PROJECT_RULES_USERS.*APPROVALS_REQUIRED", re.IGNORECASE
)

METRICS_EXCEPTION_INSTANCE_SQL = (
    "counts.clusters_platforms_eks",
    "counts.clusters_platforms_gke",
    "usage_activity_by_stage.configure.clusters_platforms_gke",
    "usage_activity_by_stage.configure.clusters_platforms_eks",
    "usage_activity_by_stage_monthly.configure.clusters_platforms_gke",
    "usage_activity_by_stage_monthly.configure.clusters_platforms_eks",
    "usage_activity_by_stage.release.users_creating_deployment_approvals",
    "usage_activity_by_stage_monthly.release.users_creating_deployment_approvals",
)

ENCODING = "utf8"
REDIS_KEY = "redis"
SQL_KEY = "sql"


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

        self.headers = {
            "PRIVATE-TOKEN": config_dict.get("GITLAB_ANALYTICS_PRIVATE_TOKEN", None)
        }
        self.encoding = ENCODING

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
        with open(file=file_name, mode="r", encoding=self.encoding) as file:
            return json.load(file)

    @staticmethod
    def get_loaded_metadata(keys: list, values: list) -> dict:
        """
        Combined metadata from SQL and Redis and export as one json
        Output:
        {sql:{version: 15.1, edition: EEU,...},
         redis: {version: 15.2, edition: CE,...}
         }
        """
        if not keys or not values:
            return {}

        return {keys[i]: values[i] for i in range(len(keys))}

    def get_md5(
        self, input_timestamp: float = datetime.datetime.utcnow().timestamp()
    ) -> str:
        """
        Convert input datetime into md5 hash.
        Result is returned as a string.
        Example:

            Input (datetime): datetime.utcnow().timestamp()
            Output (str): md5 hash

            -----------------------------------------------------------
            current timestamp: 1629986268.131019
            md5 timestamp: 54da37683078de0c1360a8e76d942227
        """
        timestamp_encoded = str(input_timestamp).encode(encoding=self.encoding)

        return md5(timestamp_encoded).hexdigest()
