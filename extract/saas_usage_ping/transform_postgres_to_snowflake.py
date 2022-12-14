"""
Module is used to transform SQL syntax from Postgres to Snowflake style
"""
import json
from typing import Any, Dict, List, Callable
from logging import info

from os import environ as env
import re
import sqlparse
from sqlparse.sql import Token, TokenList
from sqlparse.tokens import Whitespace

import requests

META_API_COLUMNS = [
    "recorded_at",
    "version",
    "edition",
    "recording_ce_finished_at",
    "recording_ee_finished_at",
    "uuid",
]

TRANSFORMED_INSTANCE_QUERIES_FILE = "transformed_instance_queries.json"
META_DATA_INSTANCE_QUERIES_FILE = "meta_data_instance_queries.json"
HAVING_CLAUSE_PATTERN = re.compile(
    "HAVING.*COUNT.*APPROVAL_PROJECT_RULES_USERS.*APPROVALS_REQUIRED", re.IGNORECASE
)

METRICS_EXCEPTION = (
    "counts.clusters_platforms_eks",
    "counts.clusters_platforms_gke",
    "usage_activity_by_stage.configure.clusters_platforms_gke",
    "usage_activity_by_stage.configure.clusters_platforms_eks",
    "usage_activity_by_stage_monthly.configure.clusters_platforms_gke",
    "usage_activity_by_stage_monthly.configure.clusters_platforms_eks",
    "usage_activity_by_stage.release.users_creating_deployment_approvals",
    "usage_activity_by_stage_monthly.release.users_creating_deployment_approvals",
)


def get_sql_query_map(private_token: str = None) -> Dict[Any, Any]:
    """
    Routine to get data from RestFUL API and return as a dict
    """
    headers = {
        "PRIVATE-TOKEN": private_token,
    }

    url = "https://gitlab.com/api/v4/usage_data/queries"

    response = requests.get(url=url, headers=headers)

    response.raise_for_status()

    source_json_data = json.loads(response.text)

    return source_json_data


def get_trimmed_token(token: List[str]) -> List[str]:
    """
    Remove empty spaces from the end
    """
    if not token:
        return []

    res = token

    for char in res[::-1]:
        if char == " ":
            res.pop()
        else:
            break

    return res


def get_optimized_token(token: str) -> str:
    """
    Function reduce and optimize the size of the token length
    Primary goal is to:
    - reduce multiple whitespaces:
      from ' ',' ',' ' to ' ' and decrease the size
      (which will speed up the process)
    - remove '\n' (new line) characters as
      they have no any value for a query execution
    """

    if not token:
        return ""

    current_pos = 0
    previous = " "
    optimized = []

    for current in token:
        if (current != previous or previous != " ") and current != "\n":
            optimized.append(token[current_pos])
        previous = current
        current_pos += 1

    trimmed = get_trimmed_token(token=optimized)

    return "".join(trimmed)


def get_translated_postgres_count(token_list: list) -> List[str]:
    """
    Function to support translation of COUNT syntax from Postgres to Snowflake.
    Example:
         - translate count(xx.xx.xx) -> COUNT(xx.xx)
         - translate COUNT(DISTINCT xx.xx.xx) -> COUNT(DISTINCT xx.xx)
    """
    if not token_list:
        return []

    for index, token in enumerate(token_list):
        token = str(token).replace("COUNT( ", "COUNT(")
        postgres_query = str(token).upper()
        if (
            postgres_query.startswith("COUNT")
            and postgres_query.endswith(")")
            and postgres_query.count("(") > 0
            and postgres_query.count("(") == postgres_query.count(")")
            and postgres_query.count(".") == 2
        ):
            # distinguish COUNT(DISTINCT xx.xx.xx) from COUNT(xx.xx.xx)
            if " " in token[6:]:
                index_from = postgres_query.index(" ")
            else:
                index_from = postgres_query.index("(")
            fixed_count = (
                str(token)[: index_from + 1] + str(token)[token.index(".") + 1 :]
            )

            token_list[index] = fixed_count
    return token_list


def get_sql_as_string(token_list: list) -> str:
    """
    Transform token list in a prepared, executable SQL statement
    ready for the execution in Snowflake.
    tokenized list -> str
    """
    if not token_list:
        return ""

    # transform token list in list of strings
    prepared = [str(token) for token in token_list]

    # recreate from the list the SQL query
    res = "".join(prepared)

    return res


def get_keyword_index(token_list: list, defined_keyword: str) -> int:
    """
    Find the index, if any exist for keyword defined
    If no index is found, return 0 as a value
    """
    if not token_list:
        return 0

    for index, keyword_token in enumerate(token_list):
        if (
            keyword_token.is_keyword
            and str(keyword_token).upper() == defined_keyword.upper()
        ):
            return index
    return 0


def get_sql_dict(payload: dict) -> dict:
    """
    Filtered out data and keep real SQLs in
    the sql_dict dict

    The logic:
    - Only keep any 'valid' key:values that have a select value OR
    has a descendent k:v with a select value
    - preserve original JSON structure for 'valid' key:values
    """

    sql_dict: Dict[Any, Any] = {}
    for metric_name, metric_sql in payload.items():
        if isinstance(metric_sql, dict):
            return_dict = get_sql_dict(metric_sql)
            if return_dict:
                sql_dict[metric_name] = return_dict
        elif isinstance(metric_sql, str) and metric_sql.startswith("SELECT"):
            sql_dict[metric_name] = metric_sql
    return sql_dict


def get_trimmed_query(query: str) -> str:
    """
    removing extra " to have an easier query to parse
    """
    return query.replace('"', "")


def get_tokenized_query(raw_query: str):
    """
    Transform string-based query to
    tokenized query
    """
    trimmed = get_trimmed_query(query=raw_query)

    optimized = get_optimized_token(token=trimmed)

    parsed = sqlparse.parse(sql=optimized)

    # split the query in tokens
    # get a list of tokens
    tokenized = parsed[0].tokens

    return tokenized


def get_snowflake_query(
    metrics_name: str, tokenized, select_index: int, from_index: int
):
    """
    Return Snowflake syntax query in the format
    applicable to clculte the metrics
    """

    # Determinate if we have subquery (without FROM clause in the main query),
    # in the format SELECT (SELECT xx FROM);
    # If yes, fit it as a scalar value in counter_value column
    if from_index == 0:
        from_index = len(tokenized) + 1

    # add the counter name column
    tokenized.insert(
        from_index - 1, " AS counter_value, TO_DATE(CURRENT_DATE) AS run_day  "
    )

    tokenized.insert(
        select_index + 1, " '" + metrics_name + "' AS counter_name, ",
    )

    return tokenized


def add_counter_name_as_column(metrics: str, query: str) -> str:
    """
    Transform query from Postgres to Snowflake syntax.
    For this purpose using
    the sqlparse library: https://pypi.org/project/sqlparse/

    Example:
    A query like:

        SELECT COUNT(issues.id)
        FROM issues

    will be changed to:

        SELECT
          "counts.issues" AS counter_name,
          COUNT(issues.id) AS counter_value,
          TO_DATE(CURRENT_DATE) AS run_day
        FROM issues
    """

    tokenized_query = get_tokenized_query(raw_query=query)

    select_index = get_keyword_index(
        token_list=tokenized_query, defined_keyword="SELECT"
    )
    from_index = get_keyword_index(token_list=tokenized_query, defined_keyword="FROM")

    translated = get_translated_postgres_count(token_list=tokenized_query)

    snowflake_query = get_snowflake_query(
        metrics_name=metrics,
        tokenized=translated,
        select_index=select_index,
        from_index=from_index,
    )

    res = get_sql_as_string(token_list=snowflake_query)

    return res


def is_for_renaming(current_token: str, next_token: str) -> bool:
    """
    function as a condition will
    the next position will be renamed or not
    """

    return (
        not next_token.startswith("prep")
        and not next_token.startswith("prod")
        and not (current_token == "FROM" and next_token.startswith("("))
    )


def get_renamed_table_name(
    token: str, index: int, tokenized_list: List[Token], token_list: List[str]
) -> None:
    """
    Replaces the table name in the query -
    represented as the list of tokens,
    to make it able to run in Snowflake

    Does this by prepending `prep.gitlab_dotcom.gitlab_dotcom_`
    to the table name in the query and then appending `_dedupe_source`

    Input:
    SELECT 1 FROM some_table

    Output:
    SELECT 1 FROM prep.gitlab_dotcom.gitlab_dotcom_some_table_dedupe_source
    """

    # comprehensive list of all the keywords that are followed by a table name
    keywords = [
        "FROM",
        "JOIN",
    ]

    if any(token_word in keywords for token_word in token.split(" ")):
        i = 1

        # Whitespaces are considered as tokens and should be skipped
        while tokenized_list[index + i].ttype is Whitespace:
            i += 1

        next_token = str(tokenized_list[index + i])

        if is_for_renaming(current_token=token, next_token=next_token):
            token_list[index + i] = (
                f"prep.gitlab_dotcom.gitlab_dotcom_"
                f"{next_token}"
                f"_dedupe_source AS "
                f"{next_token}"
            )


def get_tokenized_query_list(sql_query: str) -> list:
    """
    Transform string into tokenized list
    """

    parsed = sqlparse.parse(sql_query)

    return list(TokenList(parsed[0].tokens).flatten())


def get_token_list(tokenized_list: list) -> list:
    """
    Return string list from a tokenized list
    """
    return list(map(str, tokenized_list))


def get_prepared_list(token_list: list) -> str:
    """
    Get prepared list as a string,
    transformed from the list.
    """
    raw = "".join(token_list)

    prepared = get_transformed_having_clause(postgres_sql=raw)

    return prepared


def get_transformed_token_list(sql_query: str) -> List[str]:
    """
    Transforming token list
    """

    # start parsing the query and get the token_list
    tokenized = get_tokenized_query_list(sql_query=sql_query)

    res = get_token_list(tokenized_list=tokenized)

    # go through the tokenized to find the tables that should be renamed
    for current_index, current_token in enumerate(tokenized, start=0):
        get_renamed_table_name(
            token=str(current_token),
            index=current_index,
            tokenized_list=tokenized,
            token_list=res,
        )
    return res


def get_renamed_query_tables(sql_query: str) -> str:
    """
    function to rename the table in the sql query
    """

    token_list = get_transformed_token_list(sql_query=sql_query)

    prepared_list = get_prepared_list(token_list=token_list)

    return prepared_list


def get_transformed_having_clause(postgres_sql: str) -> str:
    """
    Algorithm enhancement , need to allow following transformation from:
    (COUNT(approval_project_rules_users) < approvals_required)
    to
    (COUNT(approval_project_rules_users.id) < MAX(approvals_required))
    """
    snowflake_having_clause = postgres_sql

    if HAVING_CLAUSE_PATTERN.findall(snowflake_having_clause):

        snowflake_having_clause = postgres_sql.replace(
            "(approval_project_rules_users)", "(approval_project_rules_users.id)"
        )

        snowflake_having_clause = snowflake_having_clause.replace(
            "approvals_required)", "MAX(approvals_required))"
        )

    return snowflake_having_clause


def perform_action_on_query_str(
    original_dict: Dict[Any, Any],
    action: Callable[..., str],
    action_arg_type: str = "both",
) -> Dict[Any, Any]:
    """
    Iterate over a nested dictionary object.
    If the value is a 'select statement' string...
    then perform some action on the query string.

    If the value is a dictionary, recursively call this function,
    so that eventually all 'child' query strings can be actioned.
    """
    new_dict: Dict[Any, Any] = {}
    for k, v in original_dict.items():
        # if dict, rerun this function recursively
        if isinstance(v, dict):
            return_dict = perform_action_on_query_str(v, action, action_arg_type)
            new_dict[k] = return_dict
        # if string and select statement, apply the action
        elif isinstance(v, str) and v.startswith("SELECT"):
            if action_arg_type == "both":
                new_val = action(k, v)
            elif action_arg_type == "value":
                new_val = action(v)
            else:
                raise ValueError(
                    'Invalid action_arg_type for perform_action_on_query_str(), can choose either "both" or "value"'
                )
            new_dict[k] = new_val
        # else, keep the dict as is
        else:
            new_dict[k] = v
    return new_dict


def transform(json_data: Dict[Any, Any]) -> Dict[Any, Any]:
    """
    Main input point to transform queries
    """

    sql_dict = get_sql_dict(json_data)

    prepared = perform_action_on_query_str(
        original_dict=sql_dict,
        action=add_counter_name_as_column,
        action_arg_type="both",
    )

    transformed = perform_action_on_query_str(
        original_dict=prepared, action=get_renamed_query_tables, action_arg_type="value"
    )

    return transformed


def keep_meta_data(json_data: dict) -> dict:
    """
    Pick up meta data we want to expose in Snowflake from the original file

    param json_file: json file downloaded from API
    return: dict
    """

    meta_data = {
        meta_api_column: json_data.get(meta_api_column, "")
        for meta_api_column in META_API_COLUMNS
    }

    return meta_data


def save_to_json_file(file_name: str, json_data: dict) -> None:
    """
    param file_name: str
    param json_data: dict
    return: None
    """
    with open(file=file_name, mode="w", encoding="utf-8") as wr_file:
        json.dump(json_data, wr_file)


if __name__ == "__main__":
    config_dict = env.copy()
    payload = get_sql_query_map(
        private_token=config_dict["GITLAB_ANALYTICS_PRIVATE_TOKEN"]
    )

    final_sql__dict = transform(payload)
    final_meta_data = keep_meta_data(payload)
    info("Processed final sql queries")

    save_to_json_file(
        file_name=TRANSFORMED_INSTANCE_QUERIES_FILE, json_data=final_sql__dict
    )
    save_to_json_file(
        file_name=META_DATA_INSTANCE_QUERIES_FILE, json_data=final_meta_data
    )
