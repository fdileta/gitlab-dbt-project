"""
Automated Service Ping main unit

usage_ping.py is responsible for uploading the following into Snowflake:
- usage ping combined metrics (sql + redis)
- usage ping namespace
"""
import datetime
import json
import logging
import os
import sys
from logging import info
from typing import Any, Dict, List, Tuple

import pandas as pd
import yaml
from fire import Fire
from sqlalchemy.exc import SQLAlchemyError
from utils import EngineFactory, Utils


def get_backfill_filter(filter_list: list):
    """
    Define backfill filter for
    processing a namespace metrics load
    """

    return (
        lambda namespace: namespace.get("time_window_query")
        and namespace.get("counter_name") in filter_list
    )


class UsagePing:
    """
    Usage ping class represent as an umbrella
    to sort out service ping data import
    """

    def __init__(self, ping_date=None, namespace_metrics_filter=None):

        self.engine_factory = EngineFactory()
        self.utils = Utils()

        if ping_date is not None:
            self.end_date = datetime.datetime.strptime(ping_date, "%Y-%m-%d").date()
        else:
            self.end_date = datetime.datetime.now().date()

        if namespace_metrics_filter is not None:
            self.metrics_backfill = namespace_metrics_filter
        else:
            self.metrics_backfill = []

        self.start_date_28 = self.end_date - datetime.timedelta(28)
        self.dataframe_api_columns = self.utils.META_API_COLUMNS
        self.missing_definitions = {self.utils.SQL_KEY: [], self.utils.REDIS_KEY: []}
        self.duplicate_keys = []

    def _get_instance_sql_metrics_definition(self) -> Dict[str, Any]:
        """
        Calls api endpoint to get metric_definitions yaml file
        Loads file as list and converts it to a dictionary
        """
        url = "http://gitlab.com/api/v4/usage_data/metric_definitions"

        response = self.utils.get_response(url=url)

        metric_definitions = yaml.safe_load(response.text)

        metric_definitions_dict = {
            metric_dict["key_path"]: metric_dict for metric_dict in metric_definitions
        }
        return metric_definitions_dict

    def set_metrics_filter(self, metrics: list):
        """
        setter for metrics filter
        """
        self.metrics_backfill = metrics

    def get_metrics_filter(self) -> list:
        """
        getter for metrics filter
        """
        return self.metrics_backfill

    def _get_instance_sql_metrics_queries(self) -> Dict:
        """
        can be updated to query an end point or query other functions
        to generate the {ping_name: sql_query} dictionary
        """
        file_name = os.path.join(
            os.path.dirname(__file__), self.utils.TRANSFORMED_INSTANCE_SQL_QUERIES_FILE
        )

        return self.utils.load_from_json_file(file_name=file_name)

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

    def _get_meta_data_from_file(self, file_name: str) -> dict:
        """
        Load metadata from .json file from the file system
        param file_name: str
        return: dict
        """
        full_path = os.path.join(os.path.dirname(__file__), file_name)

        return self.utils.load_from_json_file(file_name=full_path)

    def does_source_match_definition(
        self, payload_source, metric_definition_source
    ) -> dict:
        """
        Determines if payload data source matches the defined data source.

        A metric from the redis payload is valid if the data_source != database
        Conversely, a metric from sql payload is valid if data_source == database
        """
        SQL_DATA_SOURCE_VAL = "database"

        if payload_source == self.utils.REDIS_KEY:
            is_matching = (
                metric_definition_source
                and metric_definition_source != SQL_DATA_SOURCE_VAL
            )

        elif payload_source == self.utils.SQL_KEY:
            is_matching = metric_definition_source == SQL_DATA_SOURCE_VAL

        return is_matching

    def check_data_source(
        self,
        payload_source: str,
        metric_definition_dict: Dict,
        concat_metric_name: str,
        prev_concat_metric_name: str,
    ) -> str:
        """
        Check if an individual metric from the payload matches source
        as defined in the definition yaml file.

        Both the concatted metric name, i.e key1.key2.key3
        and the previous metric name, i.e key1.key2
        need to be checked because some metrics are only defined by their parent name
        i.e `usage_activity_by_stage.manage.user_auth_by_provider.*`
        """
        metric_definition_data_source_key = "data_source"
        metric_definition = metric_definition_dict.get(concat_metric_name, {})

        parent_metric_definition = metric_definition_dict.get(
            prev_concat_metric_name, {}
        )

        if metric_definition or parent_metric_definition:
            # check if redis or sql payload has the correct corresponding data source in the yaml file
            if self.does_source_match_definition(
                payload_source, metric_definition.get(metric_definition_data_source_key)
            ) or self.does_source_match_definition(
                payload_source,
                parent_metric_definition.get(metric_definition_data_source_key),
            ):
                return "valid_source"

            return "not_matching_source"

        return "missing_definition"

    def keep_valid_metric_definitions(
        self,
        payload: dict,
        payload_source: str,
        metric_definition_dict: Dict,
        prev_concat_metric_name: str = "",
    ) -> dict:
        """
        For each payload - sourced either from sql or redis- check against
        the metric_definition.yaml file if it is a valid metric.

        A valid metric is one whose payload source matches the defined data_source in the yaml file.

        Do this recursively to keep the structure of the original payload
        """

        valid_metric_dict: Dict[Any, Any] = {}
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
                if (
                    concat_metric_name.lower()
                    not in self.utils.METRICS_EXCEPTION_INSTANCE_SQL
                    or payload_source != "sql"
                ):
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
                        self.missing_definitions[payload_source].append(
                            concat_metric_name
                        )

        return valid_metric_dict

    def get_instance_sql_data(self, sql_query: str, con) -> tuple:
        """
        Get data to write and error (if any) to write
        for instance_sql_metrics
        """
        try:
            metrics_data = error_data = None
            query_output = pd.read_sql(sql=sql_query, con=con)
            # standardize column case across pandas versions
            query_output.columns = query_output.columns.str.lower()
            # info(query_output)
            # convert 'numpy int' to 'int' so json can be written
            metrics_data = int(query_output.loc[0, "counter_value"])
        except (KeyError, ValueError):
            metrics_data = 0

        except SQLAlchemyError as e:
            error_data = str(e.__dict__["orig"])

        return metrics_data, error_data

    def evaluate_saas_instance_sql_queries(
        self, connection, saas_queries: Dict
    ) -> Tuple[Dict, Dict]:
        """
        For each 'select statement' in the dict,
        update the dict value to be
        the output of the query when run against Snowflake.

        For example {"key": "SELECT 1"} becomes {"key": "1"}

        The dict vals are updated recursively to preserve its nested structure

        """
        results: Dict[Any, Any] = {}
        errors: Dict[Any, Any] = {}

        for key, query in saas_queries.items():
            # if the 'query' is a dictionary, then recursively call
            if isinstance(query, dict):
                results_returned, errors_returned = self.evaluate_saas_instance_sql_queries(
                    connection, query
                )
                if results_returned:
                    results[key] = results_returned
                if errors_returned:
                    errors[key] = errors_returned
            # reached a 'select statement' value, run it in snowflake
            elif isinstance(query, str) and query.startswith("SELECT"):
                # logging.info(f"Running ping: {key}...")
                #
                # try:
                #     data_to_write = error_data_to_write = None
                    # query_output = pd.read_sql(sql=query, con=connection)
                    # # standardize column case across pandas versions
                    # query_output.columns = query_output.columns.str.lower()
                    # info(query_output)
                    # # convert 'numpy int' to 'int' so json can be written
                    # data_to_write = int(query_output.loc[0, "counter_value"])


                # except (KeyError, ValueError):
                #     data_to_write = 0
                # except SQLAlchemyError as e:
                #     error_data_to_write = str(e.__dict__["orig"])

                #
                data_to_write = error_data_to_write = None
                
                data_to_write, error_data_to_write = self.get_instance_sql_data(
                    sql_query=query, con=connection
                )

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

        connection = self.engine_factory.connect()

        payload_source = self.utils.SQL_KEY

        saas_queries_with_valid_definitions = self.keep_valid_metric_definitions(
            saas_queries, payload_source, metric_definition_dict
        )

        sql_metrics, sql_metric_errors = self.evaluate_saas_instance_sql_queries(
            connection, saas_queries_with_valid_definitions
        )

        info("Processed queries")

        connection.close()
        self.engine_factory.dispose()

        return sql_metrics, sql_metric_errors

    def saas_instance_redis_metrics(
        self, metric_definition_dict: Dict
    ) -> Tuple[Dict, Dict]:

        """
        Call the Non SQL Metrics API and store the results in Snowflake RAW database
        """
        url = "https://gitlab.com/api/v4/usage_data/non_sql_metrics"

        redis_metrics = self.utils.get_response_as_dict(url=url)
        redis_metadata = self.utils.keep_meta_data(json_data=redis_metrics)

        payload_source = self.utils.REDIS_KEY
        redis_metrics = self.keep_valid_metric_definitions(
            payload=redis_metrics,
            payload_source=payload_source,
            metric_definition_dict=metric_definition_dict,
        )

        return redis_metrics, redis_metadata

    def upload_combined_metrics(
        self,
        combined_metrics: Dict,
        saas_queries: Dict,
        sql_metadata,
        redis_metadata,
    ) -> None:
        """
        Uploads combined_metrics dictionary to Snowflake
        """

        df_to_upload = pd.DataFrame(
            columns=["query_map", "run_results", "ping_date", "run_id"]
            + self.dataframe_api_columns
            + ["source", "load_metadata"]
        )

        combined_metadata = self.utils.get_loaded_metadata(
            keys=[self.utils.SQL_KEY, self.utils.REDIS_KEY],
            values=[
                sql_metadata,
                redis_metadata,
            ],
        )

        df_to_upload.loc[0] = (
            [
                saas_queries,
                json.dumps(combined_metrics),
                self.end_date,
                self.utils.get_md5(datetime.datetime.utcnow().timestamp()),
            ]
            + self._get_dataframe_api_values(
                self._get_meta_data_from_file(
                    file_name=self.utils.META_DATA_INSTANCE_SQL_QUERIES_FILE
                )
            )
            + ["combined"]
            + [json.dumps(combined_metadata)]
        )

        self.engine_factory.upload_to_snowflake(
            table_name="instance_combined_metrics", data=df_to_upload
        )

        self.engine_factory.dispose()

    def upload_instance_sql_metrics_errors(self, sql_metric_errors: Dict) -> None:
        """
        Uploads sql_metric_errors dictionary to Snowflake
        """
        df_to_upload = pd.DataFrame(columns=["run_id", "sql_errors", "ping_date"])

        df_to_upload.loc[0] = [
            self.utils.get_md5(datetime.datetime.utcnow().timestamp()),
            json.dumps(sql_metric_errors),
            self.end_date,
        ]

        self.engine_factory.upload_to_snowflake(
            table_name="instance_sql_errors", data=df_to_upload
        )

        self.engine_factory.dispose()

    def run_metric_checks(self) -> None:
        """Checks the following:
        - All payload metrics appear in the metric_definitions yaml file
        - The Redis & SQL metrics don't share the same key
            - unlikely unless the duplicate keys are missing from definition file
        """
        has_error = False
        if (
            self.missing_definitions[self.utils.SQL_KEY]
            or self.missing_definitions[self.utils.REDIS_KEY]
        ):
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
        self, redis_metrics: Dict, sql_metrics: Dict, path: List = []
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
                        redis_metrics=redis_metrics[key],
                        sql_metrics=sql_metrics[key],
                        path=path + [str(key)],
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

        metric_definition_dict = self._get_instance_sql_metrics_definition()
        saas_queries = self._get_instance_sql_metrics_queries()

        sql_metrics, sql_metric_errors = self.saas_instance_sql_metrics(
            metric_definition_dict=metric_definition_dict, saas_queries=saas_queries
        )


        redis_metrics, redis_metadata = self.saas_instance_redis_metrics(
            metric_definition_dict=metric_definition_dict
        )

        combined_metrics = self._merge_dicts(
            redis_metrics=redis_metrics, sql_metrics=sql_metrics
        )

        sql_metadata = self._get_meta_data_from_file(
            file_name=self.utils.META_DATA_INSTANCE_SQL_QUERIES_FILE
        )

        self.upload_combined_metrics(
            combined_metrics=combined_metrics,
            saas_queries=saas_queries,
            sql_metadata=sql_metadata,
            redis_metadata=redis_metadata,
        )

        if sql_metric_errors:
            self.upload_instance_sql_metrics_errors(sql_metric_errors=sql_metric_errors)

        # self.run_metric_checks()

    def replace_placeholders(self, sql: str) -> str:
        """
        Replace dates placeholders with proper dates:
        Usage:
        Input: "SELECT 1 FROM TABLE WHERE created_at BETWEEN between_start_date AND between_end_date"
        Output: "SELECT 1 FROM TABLE WHERE created_at BETWEEN '2022-01-01' AND '2022-01-28'"
        """

        base_query = sql
        base_query = base_query.replace("between_end_date", f"'{str(self.end_date)}'")
        base_query = base_query.replace(
            "between_start_date", f"'{str(self.start_date_28)}'"
        )

        return base_query

    def get_prepared_values(self, query: dict) -> tuple:
        """
        Prepare variables for query
        """

        name = query.get("counter_name", "Missing Name")

        sql_raw = str(query.get("counter_query"))
        prepared_sql = self.replace_placeholders(sql_raw)

        level = query.get("level")

        return name, prepared_sql, level

    def get_result(self, query_dict: dict, conn) -> pd.DataFrame:
        """
        Try to execute query and return results
        """
        name, sql, level = self.get_prepared_values(query=query_dict)

        try:
            res = pd.read_sql(sql=sql, con=conn)
            error = "Success"
        except SQLAlchemyError as e:
            error = str(e.__dict__["orig"])
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

    def process_namespace_ping(self, query_dict, connection) -> None:
        """
        Upload result of namespace ping to Snowflake
        """

        metric_name, metric_query, _ = self.get_prepared_values(query=query_dict)

        if "namespace_ultimate_parent_id" not in metric_query:
            logging.info(
                f"Skipping ping {metric_name} due to no namespace information."
            )
            return

        results = self.get_result(query_dict=query_dict, conn=connection)

        self.engine_factory.upload_to_snowflake(
            table_name="gitlab_dotcom_namespace", data=results
        )

        logging.info(f"metric_name loaded: {metric_name}")

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
        connection = self.engine_factory.connect()

        namespace_queries = self._get_meta_data_from_file(
            file_name=self.utils.NAMESPACE_FILE
        )

        for namespace_query in namespace_queries:
            if metrics_filter(namespace_query):
                logging.info(
                    f"Start loading metrics: {namespace_query.get('counter_name')}"
                )
                self.process_namespace_ping(
                    query_dict=namespace_query, connection=connection
                )

        connection.close()
        self.engine_factory.dispose()

    def backfill(self):
        """
        Routine to back-filling
        data for namespace ping
        """

        # pick up metrics from the parameter list
        # and only if time_window_query == False
        namespace_filter_list = self.get_metrics_filter()

        namespace_filter = get_backfill_filter(filter_list=namespace_filter_list)

        self.saas_namespace_ping(metrics_filter=namespace_filter)


if __name__ == "__main__":
    logging.basicConfig(stream=sys.stdout, level=20)
    Fire(UsagePing)
    logging.info("Done with pings.")
