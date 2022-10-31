"""
Automated Service Ping main unit
"""
from os import environ as env
from typing import Dict
from hashlib import md5

from logging import info
import datetime
import json
import logging
import os
import sys
import requests
import pandas as pd

from fire import Fire

from gitlabdata.orchestration_utils import (
    dataframe_uploader,
    snowflake_engine_factory,
)

from sqlalchemy.exc import SQLAlchemyError

from transform_postgres_to_snowflake import (
    META_API_COLUMNS,
    TRANSFORMED_INSTANCE_QUERIES_FILE,
    META_DATA_INSTANCE_QUERIES_FILE,
    METRICS_EXCEPTION,
)

ENCODING = "utf8"
SCHEMA_NAME = "saas_usage_ping"
NAMESPACE_FILE = "usage_ping_namespace_queries.json"


def get_backfill_filter(filter_list: list):
    """
    Define backfill filter for
    processing a namespace metrics load
    """

    return (
        lambda namespace: namespace.get("time_window_query", False)
        and namespace.get("counter_name") in filter_list
    )


class UsagePing:
    """
    Usage ping class represent as an umbrella
    to sort out service ping data import
    """

    def __init__(self, ping_date=None, namespace_metrics_filter=None):
        self.config_vars = env.copy()
        self.loader_engine = snowflake_engine_factory(self.config_vars, "LOADER")

        if ping_date is not None:
            self.end_date = datetime.datetime.strptime(ping_date, "%Y-%m-%d").date()
        else:
            self.end_date = datetime.datetime.now().date()

        if namespace_metrics_filter is not None:
            self.metrics_backfill = namespace_metrics_filter

        self.start_date_28 = self.end_date - datetime.timedelta(28)
        self.dataframe_api_columns = META_API_COLUMNS

    def set_metrics_filter(self, metrics: list):
        """
        setter for metrics filter
        """
        self.metrics_backfill = metrics

    def get_metrics_filter(self):
        """
        getter for metrics filter
        """
        return self.metrics_backfill

    def _get_instance_queries(self) -> Dict:
        """
        can be updated to query an end point or query other functions
        to generate the {ping_name: sql_query} dictionary
        """
        with open(
            os.path.join(os.path.dirname(__file__), TRANSFORMED_INSTANCE_QUERIES_FILE),
            encoding=ENCODING,
        ) as file:
            saas_queries = json.load(file)

        # exclude metrics we do not want to track
        saas_queries = {
            metrics_name: metrics_sql
            for metrics_name, metrics_sql in saas_queries.items()
            if not metrics_name.lower() in METRICS_EXCEPTION
        }

        return saas_queries

    def _get_dataframe_api_values(self, input_json: dict) -> list:
        """
        pick up values from .json file defined in dataframe_api_columns
        and return them as a list

        param input_json: dict
        return: list
        """
        dataframe_api_value_list = [
            input_json.get(dataframe_api_column, "")
            for dataframe_api_column in self.dataframe_api_columns
        ]

        return dataframe_api_value_list

    def _get_md5(
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
        encoding = "utf-8"
        timestamp_encoded = str(input_timestamp).encode(encoding=encoding)

        return md5(timestamp_encoded).hexdigest()

    def _get_meta_data(self, file_name: str) -> dict:
        """
        Load metadata from .json file from the file system
        param file_name: str
        return: dict
        """
        full_path = os.path.join(os.path.dirname(__file__), file_name)

        with open(full_path, encoding=ENCODING) as file:
            meta_data = json.load(file)

        return meta_data

    def evaluate_saas_queries(self, connection, saas_queries):
        """
        For each 'select statement' in the dict,
        update the dict value to be
        the output of the query when run against Snowflake.

        For example {"key": "SELECT 1"} becomes {"key": "1"}

        The dict vals are updated recursively to preserve its nested structure

        """
        results = {}
        errors = {}

        for key, query in saas_queries.items():
            # if the 'query' is a dictionary, then recursively call
            if isinstance(query, dict):
                results_returned, errors_returned = self.evaluate_saas_queries(
                    connection, query
                )
                if results_returned:
                    results[key] = results_returned
                if errors_returned:
                    errors[key] = errors_returned
            # reached a 'select statement' value, run it in snowflake
            elif isinstance(query, str) and query.startswith("SELECT"):
                logging.info(f"Running ping: {key}...")

                try:
                    data_to_write = error_data_to_write = None
                    query_output = pd.read_sql(sql=query, con=connection)
                    # standardize column case across pandas versions
                    query_output.columns = query_output.columns.str.lower()
                    info(query_output)
                    # convert 'numpy int' to 'int' so json can be written
                    data_to_write = int(query_output.loc[0, "counter_value"])
                except (KeyError, ValueError):
                    data_to_write = 0
                except SQLAlchemyError as err:
                    error_data_to_write = str(err.__dict__["orig"])

                if data_to_write is not None:
                    results[key] = data_to_write

                if error_data_to_write:
                    errors[key] = error_data_to_write

            # else keep the dict as is
            else:
                results[key] = query

        return results, errors

    def saas_instance_ping(self):
        """
        Take a dictionary of {ping_name: sql_query} and run each
        query to then upload to a table in raw.
        """
        connection = self.loader_engine.connect()
        saas_queries = self._get_instance_queries()

        results, errors = self.evaluate_saas_queries(connection, saas_queries)

        info("Processed queries")
        connection.close()
        self.loader_engine.dispose()

        ping_to_upload = pd.DataFrame(
            columns=["query_map", "run_results", "ping_date", "run_id"]
            + self.dataframe_api_columns
        )

        ping_to_upload.loc[0] = [
            saas_queries,
            json.dumps(results),
            self.end_date,
            self._get_md5(datetime.datetime.utcnow().timestamp()),
        ] + self._get_dataframe_api_values(
            self._get_meta_data(META_DATA_INSTANCE_QUERIES_FILE)
        )

        dataframe_uploader(
            ping_to_upload,
            self.loader_engine,
            "instance_sql_metrics",
            "saas_usage_ping",
        )

        # Handling error data part to load data into table:
        # raw.saas_usage_ping.instance_sql_errors
        if errors:
            error_data_to_upload = pd.DataFrame(
                columns=["run_id", "sql_errors", "ping_date"]
            )

            error_data_to_upload.loc[0] = [
                self._get_md5(datetime.datetime.utcnow().timestamp()),
                json.dumps(errors),
                self.end_date,
            ]

            dataframe_uploader(
                error_data_to_upload,
                self.loader_engine,
                "instance_sql_errors",
                "saas_usage_ping",
            )

        self.loader_engine.dispose()

    def saas_instance_redis_metrics(self):

        """
        Call the Non SQL Metrics API and store the results in Snowflake RAW database
        """
        config_dict = env.copy()
        headers = {
            "PRIVATE-TOKEN": config_dict["GITLAB_ANALYTICS_PRIVATE_TOKEN"],
        }

        response = requests.get(
            "https://gitlab.com/api/v4/usage_data/non_sql_metrics", headers=headers
        )
        json_data = json.loads(response.text)

        redis_data_to_upload = pd.DataFrame(
            columns=["jsontext", "ping_date", "run_id"] + self.dataframe_api_columns
        )

        redis_data_to_upload.loc[0] = [
            json.dumps(json_data),
            self.end_date,
            self._get_md5(datetime.datetime.utcnow().timestamp()),
        ] + self._get_dataframe_api_values(json_data)

        dataframe_uploader(
            redis_data_to_upload,
            self.loader_engine,
            "instance_redis_metrics",
            "saas_usage_ping",
        )

    def replace_placeholders(self, sql: str) -> str:
        """
        Replace dates placeholders with proper dates:
        Usage:
        Input: "SELECT 1 FROM TABLE WHERE created_at BETWEEN between_start_date AND between_end_date"
        Output: "SELECT 1 FROM TABLE WHERE created_at BETWEEN '2022-01-01' AND '2022-01-28'"
        """
        res = sql
        res = res.replace("between_end_date", f"'{str(self.end_date)}'")
        res = res.replace("between_start_date", f"'{str(self.start_date_28)}'")

        return res

    def get_prepared_values(self, query: dict) -> tuple:
        """
        Prepare variables for query
        """
        sql_raw = str(query.get("counter_query"))
        prepared_sql = self.replace_placeholders(sql_raw)
        name = query.get("counter_name", "Missing Name")
        level = query.get("level")

        return name, prepared_sql, level

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

    def get_result(self, query_dict: dict, conn) -> pd.DataFrame:
        """
        Try to execute query and return results
        """
        name, sql, level = self.get_prepared_values(query=query_dict)

        try:
            # Expecting [id, namespace_ultimate_parent_id, counter_value]
            res = pd.read_sql(sql=sql, con=conn)
            error = "Success"
        except SQLAlchemyError as err:
            error = str(err.__dict__["orig"])
            res = pd.DataFrame(
                columns=["id", "namespace_ultimate_parent_id", "counter_value"]
            )
            res.loc[0] = [None, None, None]

        res["ping_name"] = name
        res["level"] = level
        res["query_ran"] = sql
        res["error"] = error
        res["ping_date"] = self.end_date

        return res

    def process_namespace_ping(self, query_dict, connection):
        """
        Upload result of namespace ping to Snowflake
        """

        metric_name, _, metric_query = self.get_prepared_values(query=query_dict)

        if "namespace_ultimate_parent_id" not in metric_query:
            logging.info(
                f"Skipping ping {metric_name} due to no namespace information."
            )
            return
        logging.info(f"metric_name: {metric_name}")

        # results = self.get_result(query_dict=query_dict, conn=connection)
        #
        # self.upload_to_snowflake(table_name="gitlab_dotcom_namespace", data=results)

    def saas_namespace_ping(self, metrics_filter=lambda _: True):
        """
        Take a dictionary of the following type and run each
        query to then upload to a table in raw.
        {
            ping_name:
            {
              query_base: sql_query,
              level: namespace,
              between: true
            }
        }
        """
        saas_queries = self._get_meta_data(NAMESPACE_FILE)

        connection = self.loader_engine.connect()

        for query_dict in saas_queries:
            if metrics_filter(query_dict):
                logging.info(f"query_dict: {query_dict}")
                # self.process_namespace_ping(query_dict, connection)

        connection.close()
        self.loader_engine.dispose()

    def backfill(self):
        """
        Routine to back-filling
        data for namespace ping
        """

        # pick up metrics from the parameter list
        # and only if time_window_query == False

        namespace_filter = self.get_metrics_filter()
        logging.info(f"metrics_backfill: {self.metrics_backfill}")
        logging.info(f"namespace_filter: {namespace_filter}")
        logging.info(f"self.config_vars: {str(self.config_vars)}")
        backfill_filter = get_backfill_filter(namespace_filter)

        logging.info(f"backfill_filter: {backfill_filter}")

        self.saas_namespace_ping(metrics_filter=backfill_filter)


if __name__ == "__main__":
    logging.basicConfig(stream=sys.stdout, level=20)
    Fire(UsagePing)
    logging.info("Done with pings.")
