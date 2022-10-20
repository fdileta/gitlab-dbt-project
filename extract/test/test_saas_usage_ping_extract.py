"""
The main test routine for Automated Service Ping
"""
from datetime import datetime, timedelta
import pytest

from extract.saas_usage_ping.usage_ping import (
    UsagePing,
    SCHEMA_NAME,
    ENCODING,
    NAMESPACE_FILE,
)


@pytest.fixture(name="usage_ping")
def get_usage_ping():
    """
    Return UsagePing object
    """
    usage_ping = UsagePing
    usage_ping.end_date = datetime.now()
    usage_ping.start_date_28 = usage_ping.end_date - timedelta(days=28)

    return usage_ping


@pytest.fixture(name="namespace_file")
def get_usage_ping_namespace_file(usage_ping):
    """
    Fixture for namespace file
    """

    return usage_ping._get_meta_data(
        usage_ping, file_name="usage_ping_namespace_queries.json"
    )


def test_static_variables():
    """
    Check static variables
    """
    assert SCHEMA_NAME == "saas_usage_ping"
    assert ENCODING == "utf8"
    assert NAMESPACE_FILE == "usage_ping_namespace_queries.json"


def test_get_md5(usage_ping):
    """
    Simple MD5 test.
    Know testing the private method is not aligned
    with the best praxis,
    but found it is sufficient
    in this implementation.
    """

    input_timestamps = [
        datetime(2021, 9, 1, 23, 10, 21).timestamp(),
        datetime(2020, 8, 1, 23, 10, 22).timestamp(),
        datetime(2021, 7, 1, 23, 10, 23).timestamp(),
        "test_string",
        "",
        None,
    ]

    for check_time in input_timestamps:
        res = usage_ping._get_md5(usage_ping, check_time)
        # Check output data type
        assert isinstance(res, str)
        # Check is len 32 as it is expected length
        assert len(res) == 32  # bytes in hex representation
        # As this is one-way function,
        # can't test it with many things
        # let see to we have all details with various inputs
        assert res is not None


# def test_evaluate_saas_queries():
#     """
#     Run a series of test queries against Snowflake.
#     The queries are designed to elicit both successful snowflake outputs
#     and errors.
#
#     The test will check that the expected queries have failed and succeeded.
#     The JSON structure is also being implicitly checked based on the ordering of the two lists (expected vs actual) being compared
#
#     Note: The snowflake outputs cannot be compared because they can change over time
#     """
#
#     def get_keys_in_nested_dict(nested_dict, keys=list()):
#         for key, val in nested_dict.items():
#             if isinstance(val, dict):
#                 get_keys_in_nested_dict(val, keys)
#             if isinstance(key, str):
#                 keys.append(key)
#         return keys
#
#     usage_ping_test = UsagePing()
#     connection = usage_ping_test.loader_engine.connect()
#     saas_queries = {
#         "active_user_count": "SELECT 'active_user_count' AS counter_name,  COUNT(users.id) AS counter_value, TO_DATE(CURRENT_DATE) AS run_day   FROM prep.gitlab_dotcom.gitlab_dotcom_users_dedupe_source AS users WHERE (users.state IN ('active')) AND (users.user_type IS NULL OR users.user_type IN (6, 4))",
#         "counts": {
#             "assignee_lists": "SELECT 'assignee_lists' AS counter_name,  COUNT(lists.id) AS counter_value, TO_DATE(CURRENT_DATE) AS run_day   FROM prep.gitlab_dotcom.gitlab_dotcom_lists_dedupe_source AS lists WHERE lists.list_type = 3",
#             "test_failure": {"some_key": "SELECT ~"},
#         },
#         "ci_triggers": {
#             "arbitrary_key": {"arbitrary_key2": {"arbitrary_key4": "SELECT ^"}}
#         },
#     }
#
#     expected_results = {
#         "active_user_count": 11466893,
#         "counts": {"assignee_lists": 52316},
#     }
#     expected_errors = {
#         "counts": {
#             "test_failure": {
#                 "some_key": "Execution failed on sql 'SELECT ~': 001003 (42000): SQL compilation error:\nsyntax error line 1 at position 8 unexpected '<EOF>'."
#             }
#         },
#         "ci_triggers": {
#             "arbitrary_key": {
#                 "arbitrary_key2": {
#                     "arbitrary_key4": "Execution failed on sql 'SELECT ^': 001003 (42000): SQL compilation error:\nsyntax error line 1 at position 7 unexpected '^'."
#                 }
#             }
#         },
#     }
#     results, errors = usage_ping_test.evaluate_saas_queries(connection, saas_queries)
#
#     # check that the correct queries have suceeded and errored
#     assert get_keys_in_nested_dict(results) == get_keys_in_nested_dict(expected_results)
#     assert get_keys_in_nested_dict(errors) == get_keys_in_nested_dict(expected_errors)


def test_json_file_consistency_time_window_query(namespace_file):
    """
    Test is dictionary is constructed properly in
    the file usage_ping_namespace_queries.json

    If time_window_query=True,
    counter_query should contain ["between_start_date","between_end_date"]
    """

    for metrics in namespace_file:
        counter_query = metrics.get("counter_query")
        time_window_query = bool(metrics.get("time_window_query", False))

        time_window_yes = (
            "between_start_date" in counter_query
            and "between_end_date" in counter_query
            and time_window_query is True
        )
        time_window_no = (
            "between_start_date" not in counter_query
            and "between_end_date" not in counter_query
            and time_window_query is False
        )

        assert time_window_yes or time_window_no


def test_namespace_file(namespace_file):
    """
    Test file loading
    """

    assert namespace_file


def test_namespace_file_error(usage_ping):
    """
    Test file loading
    """
    with pytest.raises(FileNotFoundError):
        usage_ping._get_meta_data(usage_ping, file_name="THIS_DOES_NOT_EXITS.json")


def test_json_file_consistency_level(namespace_file):
    """
    Test is dictionary is constructed properly in
    the file usage_ping_namespace_queries.json

    If level=namespace
    """

    for metrics in namespace_file:
        level = metrics.get("level")

        assert level == "namespace"


# @pytest.mark.parametrize(
#     "test_value, expected_value",
#     [
#         ("active_user_count", False),
#         (
#             "usage_activity_by_stage_monthly.manage.groups_with_event_streaming_destinations",
#             True,
#         ),
#         ("usage_activity_by_stage_monthly.manage.audit_event_destinations", True),
#         ("counts.boards", False),
#         ("usage_activity_by_stage_monthly.configure.instance_clusters_enabled", True),
#         ("counts_monthly.deployments", True),
#     ],
# )
# def test_get_backfill_filter(usage_ping, namespace_file, test_value, expected_value):
#     """
#     test backfill filter accuracy
#     """
#     usage_ping.set_metrics_backfill(usage_ping, test_value)
#
#     for namespace in namespace_file:
#         if BACKFILL_FILTER(namespace):
#             assert namespace.get("time_window_query") == expected_value
#             assert expected_value is True
#             assert namespace.get("counter_name") == test_value


# def test_get_prepared_values(namespace_file, usage_ping):
#     """
#     Test query replacement for dates placeholder
#     """
#
#     filtering = ["counts_monthly.deployments", "counts_monthly.successful_deployments"]
#
#     test_dict = [
#         usage_ping.get_prepared_values(usage_ping, namespace)
#         for namespace in namespace_file
#         if namespace.get("counter_name") in filtering
#     ]
#
#     for name, prepared_query, level in test_dict:
#         assert datetime.strftime(usage_ping.end_date, "%Y-%m-%d") in prepared_query
#         assert datetime.strftime(usage_ping.start_date_28, "%Y-%m-%d") in prepared_query
#         assert "between_start_date" not in prepared_query
#         assert "between_end_date" not in prepared_query
#
#         assert name
#         assert level == "namespace"
#


def test_replace_placeholders(usage_ping):
    """
    Test string replace for query
    """
    sql = "SELECT 1 FROM TABLE WHERE created_at BETWEEN between_start_date AND between_end_date"

    actual = usage_ping.replace_placeholders(usage_ping, sql=sql)

    assert "between_start_date" not in actual
    assert "between_end_date" not in actual

    assert datetime.strftime(usage_ping.end_date, "%Y-%m-%d") in actual
    assert datetime.strftime(usage_ping.start_date_28, "%Y-%m-%d") in actual
