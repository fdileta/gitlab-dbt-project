import pytest
import sys
import os
from datetime import datetime

# Tweak path as due to script execution way in Airflow, can't touch the original code
abs_path = os.path.dirname(os.path.realpath(__file__))
abs_path = abs_path[: abs_path.find("extract")] + "/extract/saas_usage_ping"
sys.path.append(abs_path)

from extract.saas_usage_ping.usage_ping import UsagePing


def test_get_md5():
    usage_ping_test = UsagePing

    input_timestamps = [
        datetime(2021, 9, 1, 23, 10, 21).timestamp(),
        datetime(2020, 8, 1, 23, 10, 22).timestamp(),
        datetime(2021, 7, 1, 23, 10, 23).timestamp(),
        "test_string",
        "",
        None,
    ]

    """
    Know testing the private method is not aligned with best praxis, but found it is sufficient
    in this implementation.
    """
    for i, check_time in enumerate(input_timestamps):
        res = usage_ping_test._get_md5(None, check_time)
        # Check output data type
        assert isinstance(res, str)
        # Check is len 32 as it is expected length
        assert len(res) == 32  # bytes in hex representation
        # As this is one-way function, can't test it with many things - let see to we have all details with various inputs
        assert res is not None


def test_evaluate_saas_queries():
    """
    Run a series of test queries against Snowflake.
    The queries are designed to elicit both successful snowflake outputs
    and errors.

    The test will check that the expected queries have failed and succeeded.
    The JSON structure is also being implicitly checked based on the ordering of the two lists (expected vs actual) being compared

    Note: The snowflake outputs cannot be compared because they can change over time
    """

    def get_keys_in_nested_dict(nested_dict, keys=list()):
        for key, val in nested_dict.items():
            if isinstance(val, dict):
                get_keys_in_nested_dict(val, keys)
            if isinstance(key, str):
                keys.append(key)
        return keys

    usage_ping_test = UsagePing()
    connection = usage_ping_test.loader_engine.connect()
    saas_queries = {
      "active_user_count": "SELECT 'active_user_count' AS counter_name,  COUNT(users.id) AS counter_value, TO_DATE(CURRENT_DATE) AS run_day   FROM prep.gitlab_dotcom.gitlab_dotcom_users_dedupe_source AS users WHERE (users.state IN ('active')) AND (users.user_type IS NULL OR users.user_type IN (6, 4))",
      "counts": {
        "assignee_lists": "SELECT 'assignee_lists' AS counter_name,  COUNT(lists.id) AS counter_value, TO_DATE(CURRENT_DATE) AS run_day   FROM prep.gitlab_dotcom.gitlab_dotcom_lists_dedupe_source AS lists WHERE lists.list_type = 3",
        "test_failure": {
          "some_key": "SELECT ~"
        }
      },
      "ci_triggers": {
        "arbitrary_key": {
          "arbitrary_key2": {
            "arbitrary_key4": "SELECT ^"
          }
        }
      }
    }

    expected_results = {'active_user_count': 11466893, 'counts': {'assignee_lists': 52316}}
    expected_errors = {'counts': {'test_failure': {'some_key': "Execution failed on sql 'SELECT ~': 001003 (42000): SQL compilation error:\nsyntax error line 1 at position 8 unexpected '<EOF>'."}}, 'ci_triggers': {'arbitrary_key': {'arbitrary_key2': {'arbitrary_key4': "Execution failed on sql 'SELECT ^': 001003 (42000): SQL compilation error:\nsyntax error line 1 at position 7 unexpected '^'."}}}}
    results, errors = usage_ping_test.evaluate_saas_queries(connection, saas_queries)

    # check that the correct queries have suceeded and errored
    assert get_keys_in_nested_dict(results) == get_keys_in_nested_dict(expected_results)
    assert get_keys_in_nested_dict(errors) == get_keys_in_nested_dict(expected_errors)
