from os import environ as env
from typing import Dict, List, Any, Tuple
from hashlib import md5

from logging import info
import datetime
import json
import logging
import os
import sys
import requests
import pandas as pd
import yaml


from transform_postgres_to_snowflake import (
    META_API_COLUMNS,
    TRANSFORMED_INSTANCE_QUERIES_FILE,
    META_DATA_INSTANCE_QUERIES_FILE,
    METRICS_EXCEPTION,
)
from fire import Fire
from gitlabdata.orchestration_utils import (
    dataframe_uploader,
    snowflake_engine_factory,
)
from sqlalchemy.exc import SQLAlchemyError

REDIS_KEY = "redis"
SQL_KEY = "sql"


class UsagePing(object):
    """
    Usage ping class represent as an umbrella
    to sort out service ping data import
    """

    def __init__(self, ping_date=None):
        self.config_vars = env.copy()
        self.loader_engine = snowflake_engine_factory(self.config_vars, "LOADER")

        if ping_date is not None:
            self.end_date = datetime.datetime.strptime(ping_date, "%Y-%m-%d").date()
        else:
            self.end_date = datetime.datetime.now().date()

        self.start_date_28 = self.end_date - datetime.timedelta(28)
        self.dataframe_api_columns = META_API_COLUMNS
        self.missing_definitions = {SQL_KEY: [], REDIS_KEY: []}
        self.duplicate_keys = []

    def _get_metrics_definition_dict(self) -> Dict[str, Any]:
        """
        Calls api endpoint to get metric_definitions yaml file
        Loads file as list and converts it to a dictionary
        """
        config_dict = env.copy()
        headers = {
            "PRIVATE-TOKEN": config_dict["GITLAB_ANALYTICS_PRIVATE_TOKEN"],
        }

        response = requests.get(
            "http://gitlab.com/api/v4/usage_data/metric_definitions", headers=headers
        )
        if response.status_code == 200:
            metric_definitions = yaml.safe_load(response.text)
        else:
            logging.error(response.json)
            raise ConnectionError("Error requesting job")

        metric_definitions_dict = {
            metric_dict["key_path"]: metric_dict for metric_dict in metric_definitions
        }
        return metric_definitions_dict

    def _get_instance_queries(self) -> Dict:
        """
        can be updated to query an end point or query other functions
        to generate the {ping_name: sql_query} dictionary
        """
        with open(
            os.path.join(os.path.dirname(__file__), TRANSFORMED_INSTANCE_QUERIES_FILE)
        ) as f:
            saas_queries = json.load(f)

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
        Load meta data from .json file from the file system
        param file_name: str
        return: dict
        """
        with open(os.path.join(os.path.dirname(__file__), file_name)) as f:
            meta_data = json.load(f)

        return meta_data

    def check_data_source(
        self,
        payload_source: str,
        metric_definition_dict: Dict,
        concat_metric_name: str,
        prev_concat_metric_name: str,
    ) -> str:
        """
        Determines if payload data source matches the defined data source.

        A metric from the redis payload is valid if the data_source != database
        Conversely, a metric from sql payload is valid if data_source == database

        Both the concatted metric name, i.e key1.key2.key3
        and the previous metric name, i.e key1.key2
        need to be checked because some metrics are only defined by their parent name
        """
        is_match_defined_source = {
            REDIS_KEY: lambda defined_data_source: defined_data_source
            and defined_data_source != "database",
            SQL_KEY: lambda defined_data_source: defined_data_source == "database",
        }
        metric_definition = metric_definition_dict.get(concat_metric_name, {})
        # need to check parent_metric_definition too, because some metrics only have the parent defined, i.e `usage_activity_by_stage.manage.user_auth_by_provider.*`
        parent_metric_definition = metric_definition_dict.get(
            prev_concat_metric_name, {}
        )

        if metric_definition or parent_metric_definition:
            # check if redis or sql payload has the correct corresponding data source in the yaml file
            if is_match_defined_source[payload_source](
                metric_definition.get("data_source")
            ) or is_match_defined_source[payload_source](
                parent_metric_definition.get("data_source")
            ):
                return "valid_source"

            else:
                return "not_matching_source"
        else:
            return "missing_definition"

    def keep_valid_metric_definitions(
        self,
        payload: dict,
        payload_source: str,
        metric_definition_dict: Dict,
        prev_concat_metric_name: str = "",
    ) -> dict:
        """
        For each payload- sourced either from sql or redis- check against
        the metric_definition.yaml file if it is a valid metric.

        A valid metric is one whose payload source matches the defined data_source in the yaml file.

        Do this recursively to keep the structure of the original payload
        """

        valid_metric_dict: Dict[Any, Any] = dict()
        for metric_name, metric_value in payload.items():
            if prev_concat_metric_name:
                concat_metric_name = prev_concat_metric_name + "." + metric_name
            else:
                concat_metric_name = metric_name

            if isinstance(metric_value, dict):
                return_dict = self.keep_valid_metric_definitions(
                    metric_value,
                    payload_source,
                    metric_definition_dict,
                    concat_metric_name,
                )
                if return_dict:
                    valid_metric_dict[metric_name] = return_dict
            else:
                if concat_metric_name.lower() not in METRICS_EXCEPTION:
                    data_source_status = self.check_data_source(
                        payload_source,
                        metric_definition_dict,
                        concat_metric_name,
                        prev_concat_metric_name,
                    )
                    if data_source_status == "valid_source":
                        valid_metric_dict[metric_name] = metric_value
                    elif data_source_status == "not_matching_source":
                        pass  # do nothing, invalid sources are expected
                    elif data_source_status == "missing_definition":
                        self.missing_definitions[payload_source].append(concat_metric_name)

        return valid_metric_dict

    def evaluate_saas_queries(
        self, connection, saas_queries: Dict
    ) -> Tuple[Dict, Dict]:
        """
        For each 'select statement' in the dict,
        update the dict value to be
        the output of the query when run against Snowflake.

        For example {"key": "SELECT 1"} becomes {"key": "1"}

        The dict vals are updated recursively to preserve its nested structure

        """
        results: Dict[Any, Any] = dict()
        errors: Dict[Any, Any] = dict()

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
                except SQLAlchemyError as e:
                    error_data_to_write = str(e.__dict__["orig"])

                if data_to_write is not None:
                    results[key] = data_to_write

                if error_data_to_write:
                    errors[key] = error_data_to_write

            # else keep the dict as is
            else:
                results[key] = query

        return results, errors

    def saas_instance_sql_metrics(
        self, metric_definition_dict: Dict, saas_queries: Dict
    ) -> Tuple[Dict, Dict]:
        """
        Take a dictionary of {ping_name: sql_query} and run each
        query to then upload to a table in raw.
        """
        connection = self.loader_engine.connect()

        payload_source = SQL_KEY
        saas_queries_with_valid_definitions = self.keep_valid_metric_definitions(
            saas_queries, payload_source, metric_definition_dict
        )

        sql_metrics, sql_metric_errors = self.evaluate_saas_queries(
            connection, saas_queries_with_valid_definitions
        )

        info("Processed queries")
        connection.close()
        self.loader_engine.dispose()

        return sql_metrics, sql_metric_errors

    def saas_instance_redis_metrics(self, metric_definition_dict: Dict) -> Dict:

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
        if response.status_code == 200:
            redis_metrics = json.loads(response.text)
        else:
            logging.error(response.json)
            raise ConnectionError("Error requesting job")

        payload_source = REDIS_KEY
        redis_metrics = self.keep_valid_metric_definitions(
            redis_metrics, payload_source, metric_definition_dict
        )

        return redis_metrics

    def upload_combined_metrics(
        self, combined_metrics: Dict, saas_queries: Dict
    ) -> None:
        df_to_upload = pd.DataFrame(
            columns=["query_map", "run_results", "ping_date", "run_id"]
            + self.dataframe_api_columns
            + ["source"]
        )

        df_to_upload.loc[0] = (
            [
                saas_queries,
                json.dumps(combined_metrics),
                self.end_date,
                self._get_md5(datetime.datetime.utcnow().timestamp()),
            ]
            + self._get_dataframe_api_values(
                self._get_meta_data(META_DATA_INSTANCE_QUERIES_FILE)
            )
            + ["combined"]
        )

        dataframe_uploader(
            df_to_upload,
            self.loader_engine,
            "instance_combined_metrics",
            "saas_usage_ping",
        )
        self.loader_engine.dispose()

    def upload_sql_metric_errors(self, sql_metric_errors: Dict) -> None:
        df_to_upload = pd.DataFrame(columns=["run_id", "sql_errors", "ping_date"])

        df_to_upload.loc[0] = [
            self._get_md5(datetime.datetime.utcnow().timestamp()),
            json.dumps(sql_metric_errors),
            self.end_date,
        ]

        dataframe_uploader(
            df_to_upload,
            self.loader_engine,
            "instance_sql_errors",
            "saas_usage_ping",
        )
        self.loader_engine.dispose()

    def run_metric_checks(self) -> None:
        has_error = False
        if self.missing_definitions[SQL_KEY] or self.missing_definitions[REDIS_KEY]:
            logging.warning(
                f"The following payloads have missing definitions in metric_definitions.yaml{self.missing_definitions}. Please open up an issue with product intelligence to add missing definition into the yaml file."
            )
            has_error = True

        if self.duplicate_keys:
            logging.warning(
                f"There is a key collision(s) between the redis and sql payload when merging the 2 payloads together. The redis key with collision is being dropped in favor of the sql one. Full details:\n{self.duplicate_keys}"
            )
            has_error = True

        # only raise error after BOTH errors have been checked for
        if has_error:
            raise ValueError(
                "Raising error to trigger Slack alert. Error is non-critical, but there is inconsistency with source data. Please check above logs for 'missing definitions' and/or 'key collision' warning."
            )

    def _merge_dicts(
        self, redis_metrics: Dict, sql_metrics: Dict, path: List = list()
    ) -> Dict:
        """
        Logic from https://stackoverflow.com/a/7205107
        Combines redis and sql metrics by
        merging sql_metrics into redis_metrics.
        """
        for key in sql_metrics:
            if key in redis_metrics:
                if isinstance(redis_metrics[key], dict) and isinstance(
                    sql_metrics[key], dict
                ):
                    self._merge_dicts(
                        redis_metrics[key], sql_metrics[key], path + [str(key)]
                    )
                elif redis_metrics[key] == sql_metrics[key]:
                    pass  # same leaf value
                else:
                    self.duplicate_keys.append(
                        f'Conflict at {".".join(path + [str(key)])}, Redis sub-value {redis_metrics} to be overriden by sql sub-value {sql_metrics}'
                    )
                    redis_metrics[key] = sql_metrics[key]
                    # raise Exception('Conflict at %s' % '.'.join(path + [str(key)]))
            else:
                redis_metrics[key] = sql_metrics[key]
        return redis_metrics

    def saas_instance_combined_metrics(self) -> None:
        """
        1. Main function to download sql/redis payloads
        2. Compares each metric against metric_definitions file
            - Drops any non-matching data_source metrics
        3. Combines the two payloads
        4. Uploads the combined payload, and any of the sql errors
        """
        metric_definition_dict = self._get_metrics_definition_dict()
        saas_queries = self._get_instance_queries()

        sql_metrics, sql_metric_errors = self.saas_instance_sql_metrics(
            metric_definition_dict, saas_queries
        )
        redis_metrics = self.saas_instance_redis_metrics(metric_definition_dict)

        combined_metrics = self._merge_dicts(redis_metrics, sql_metrics)

        self.upload_combined_metrics(combined_metrics, saas_queries)
        if sql_metric_errors:
            self.upload_sql_metric_errors(sql_metric_errors)

        self.run_metric_checks()

    def _get_namespace_queries(self) -> List[Dict]:
        """
        can be updated to query an end point or query other functions
        to generate:
        {
            { counter_name: ping_name,
              counter_query: sql_query,
              time_window_query: true,
              level: namespace,
            }
        }
        """
        with open(
            os.path.join(os.path.dirname(__file__), "usage_ping_namespace_queries.json")
        ) as namespace_file:
            saas_queries = json.load(namespace_file)

        return saas_queries

    def process_namespace_ping(self, query_dict, connection):
        base_query = query_dict.get("counter_query")
        ping_name = query_dict.get("counter_name", "Missing Name")
        logging.info(f"Running ping {ping_name}...")

        if query_dict.get("time_window_query", False):
            base_query = base_query.replace(
                "between_end_date", f"'{str(self.end_date)}'"
            )
            base_query = base_query.replace(
                "between_start_date", f"'{str(self.start_date_28)}'"
            )

        if "namespace_ultimate_parent_id" not in base_query:
            logging.info(f"Skipping ping {ping_name} due to no namespace information.")
            return

        try:
            # Expecting [id, namespace_ultimate_parent_id, counter_value]
            results = pd.read_sql(sql=base_query, con=connection)
            error = "Success"
        except SQLAlchemyError as e:
            error = str(e.__dict__["orig"])
            results = pd.DataFrame(
                columns=["id", "namespace_ultimate_parent_id", "counter_value"]
            )
            results.loc[0] = [None, None, None]

        results["ping_name"] = ping_name
        results["level"] = query_dict.get("level", None)
        results["query_ran"] = base_query
        results["error"] = error
        results["ping_date"] = self.end_date

        dataframe_uploader(
            results,
            self.loader_engine,
            "gitlab_dotcom_namespace",
            "saas_usage_ping",
        )

    def saas_namespace_ping(self, filter=lambda _: True):
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
        saas_queries = self._get_namespace_queries()

        connection = self.loader_engine.connect()

        for query_dict in saas_queries:
            if filter(query_dict):
                self.process_namespace_ping(query_dict, connection)

        connection.close()
        self.loader_engine.dispose()

    def backfill(self):
        """
        Routine to backfilling data for namespace ping
        """
        filter = lambda query_dict: query_dict.get("time_window_query", False)
        self.saas_namespace_ping(filter)


if __name__ == "__main__":
    logging.basicConfig(stream=sys.stdout, level=20)
    Fire(UsagePing)
    logging.info("Done with pings.")
