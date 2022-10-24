import pytest
import sys
import os
from datetime import datetime

# Tweak path as due to script execution way in Airflow, can't touch the original code
abs_path = os.path.dirname(os.path.realpath(__file__))
abs_path = abs_path[: abs_path.find("extract")] + "/extract/saas_usage_ping"
sys.path.append(abs_path)

from extract.saas_usage_ping.usage_ping import UsagePing, SQL_KEY, REDIS_KEY


def get_metrics_definition_test_dict():
    return {"counts.productivity_analytics_views": {"data_source": "redis", "instrumentation_class": "RedisMetric"}, "usage_activity_by_stage.secure.user_preferences_group_overview_security_dashboard": {"data_source": "database", "milestone": "<13.9"}, "usage_activity_by_stage.manage.user_auth_by_provider": {"data_source": "database", "value_json_schema": "config/metrics/objects_schemas/user_auth_by_provider.json"}, "recorded_at": {"data_source": "system", "performance_indicator_type": []}, "active_user_count": {"data_source": "database", "performance_indicator_type": []}, "counts.assignee_lists": {"data_source": "database", "milestone": "<13.9"}, "counts.ci_builds": {"data_source": "database", "milestone": "<13.9"}, "counts.ci_internal_pipelines": {"data_source": "database", "milestone": "<13.9"}, "counts.package_events_i_package_delete_package_by_deploy_token": {"data_source": "redis", "milestone": "<13.9"}, "counts.service_usage_data_download_payload_click": {"data_source": "redis", "milestone": "14.9"}, "counts.clusters_platforms_eks": {"data_source": "database", "milestone": "<13.9"}}


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
            "test_failure": {"some_key": "SELECT ~"},
        },
        "ci_triggers": {
            "arbitrary_key": {"arbitrary_key2": {"arbitrary_key4": "SELECT ^"}}
        },
    }

    expected_results = {
        "active_user_count": 11466893,
        "counts": {"assignee_lists": 52316},
    }
    expected_errors = {
        "counts": {
            "test_failure": {
                "some_key": "Execution failed on sql 'SELECT ~': 001003 (42000): SQL compilation error:\nsyntax error line 1 at position 8 unexpected '<EOF>'."
            }
        },
        "ci_triggers": {
            "arbitrary_key": {
                "arbitrary_key2": {
                    "arbitrary_key4": "Execution failed on sql 'SELECT ^': 001003 (42000): SQL compilation error:\nsyntax error line 1 at position 7 unexpected '^'."
                }
            }
        },
    }
    results, errors = usage_ping_test.evaluate_saas_queries(connection, saas_queries)

    # check that the correct queries have suceeded and errored
    assert get_keys_in_nested_dict(results) == get_keys_in_nested_dict(expected_results)
    assert get_keys_in_nested_dict(errors) == get_keys_in_nested_dict(expected_errors)


def test_check_data_source():
    """
    Test the following:
        1. Valid matching source is returned for the current metric, and the parent metric
        2. Non-matching sources are returned correctly
        3. Missing definitions are returned correctly
    """
    usage_ping_test = UsagePing()
    metric_definitions_dict = get_metrics_definition_test_dict()

    # matching redis concat_metric_name
    payload_source = REDIS_KEY
    concat_metric_name = 'counts.productivity_analytics_views'
    prev_concat_metric_name = 'counts'
    res = usage_ping_test.check_data_source(payload_source, metric_definitions_dict, concat_metric_name, prev_concat_metric_name)
    assert res == 'valid_source'

    # matching sql concat_metric_name
    payload_source = SQL_KEY
    concat_metric_name = 'usage_activity_by_stage.secure.user_preferences_group_overview_security_dashboard'
    prev_concat_metric_name = 'usage_activity_by_stage.secure'
    res = usage_ping_test.check_data_source(payload_source, metric_definitions_dict, concat_metric_name, prev_concat_metric_name)
    assert res == 'valid_source'

    # matching sql prev_concat_metric_name
    payload_source = SQL_KEY
    concat_metric_name = 'usage_activity_by_stage.manage.user_auth_by_provider.two-factor'
    prev_concat_metric_name = 'usage_activity_by_stage.manage.user_auth_by_provider'
    res = usage_ping_test.check_data_source(payload_source, metric_definitions_dict, concat_metric_name, prev_concat_metric_name)
    assert res == 'valid_source'

    # NON-MATCHING data source: redis payload, but metric definition shows data source as sql
    payload_source = REDIS_KEY
    concat_metric_name = 'usage_activity_by_stage.manage.user_auth_by_provider.two-factor'
    prev_concat_metric_name = 'usage_activity_by_stage.manage.user_auth_by_provider'
    res = usage_ping_test.check_data_source(payload_source, metric_definitions_dict, concat_metric_name, prev_concat_metric_name)
    assert res == 'not_matching_source'

    # metric in payload is missing in metric_definition yaml file
    payload_source = REDIS_KEY # should be sql
    concat_metric_name = 'some_missing_key.some_missing_key2'
    prev_concat_metric_name = 'some_missing_key'
    res = usage_ping_test.check_data_source(payload_source, metric_definitions_dict, concat_metric_name, prev_concat_metric_name)
    assert res == 'missing_definition'


def test_keep_valid_metric_definitions():
    """
    Test that only the correct metrics as defined by the metric_definitions yaml file are preserved within the payload.

    """
    usage_ping_test = UsagePing()
    payload = {"recorded_at": "2022-10-13T20:23:45.242Z", "active_user_count": "SELECT COUNT(\"users\".\"id\") FROM \"users\" WHERE (\"users\".\"state\" IN ('active')) AND (\"users\".\"user_type\" IS NULL OR \"users\".\"user_type\" IN (6, 4))", "counts": {"assignee_lists": -3, "ci_builds": -3, "ci_internal_pipelines": -1, "package_events_i_package_delete_package_by_deploy_token": 0, "service_usage_data_download_payload_click": 0, "clusters_platforms_eks": 0}}

    payload_source = REDIS_KEY
    metric_definitions_dict = get_metrics_definition_test_dict()
    valid_metric_dict = usage_ping_test.keep_valid_metric_definitions(payload, payload_source, metric_definitions_dict)
    expected_results = {"recorded_at": "2022-10-13T20:23:45.242Z", "counts": {"package_events_i_package_delete_package_by_deploy_token": 0, "service_usage_data_download_payload_click": 0}}
    assert valid_metric_dict == expected_results


def test_metric_exceptions():
    """
    Tests that metrics defined in list(METRICS_EXCEPTION) are removed.
    """
    usage_ping_test = UsagePing()
    payload = {"active_user_count": "SELECT COUNT(\"users\".\"id\") FROM \"users\" WHERE (\"users\".\"state\" IN ('active')) AND (\"users\".\"user_type\" IS NULL OR \"users\".\"user_type\" IN (6, 4))", "counts": {"clusters_platforms_eks": 0}}

    payload_source = SQL_KEY
    metric_definitions_dict = get_metrics_definition_test_dict()
    valid_metric_dict = usage_ping_test.keep_valid_metric_definitions(payload, payload_source, metric_definitions_dict)
    expected_results = {"active_user_count": "SELECT COUNT(\"users\".\"id\") FROM \"users\" WHERE (\"users\".\"state\" IN ('active')) AND (\"users\".\"user_type\" IS NULL OR \"users\".\"user_type\" IN (6, 4))"}
    assert valid_metric_dict == expected_results


def test_run_metric_checks():
    """
    Test that errors are thrown when there are:
        - missing metric definitions
        - key conflicts whe combining the redis & sql payloads
    """
    usage_ping_test = UsagePing()
    usage_ping_test.run_metric_checks() # nothing should happen

    # ensure that an error is raised if there's a missing definition
    usage_ping_test.missing_definitions[SQL_KEY].append('some_missing_definition')
    with pytest.raises(ValueError, match='Raising error to.*'):
        usage_ping_test.run_metric_checks()

    usage_ping_test.missing_definitions[SQL_KEY] = [] # reset
    usage_ping_test.run_metric_checks() # nothing should happen

    # ensure that an error is raised if there's a dup key
    usage_ping_test.duplicate_keys.append('some duplicate key')
    with pytest.raises(ValueError, match='Raising error to.*'):
        usage_ping_test.run_metric_checks()


def test_merge_dicts():
    """
    Check that when merging the redis & sql payloads, that the results are expected
    """
    usage_ping_test = UsagePing()

    # share matching key (counts), non-matching value is a non-dict (30 vs 40), will cause a conflict
    redis_metrics = {'counts': {'events': 40}}
    sql_metrics = {'counts': {'events': 30}}
    res = usage_ping_test._merge_dicts(redis_metrics, sql_metrics)
    assert res == {'counts': {'events': 30}}
    assert len(usage_ping_test.duplicate_keys) == 1

    # share matching key (events), value is a dictionary, the values are merged successfully
    redis_metrics = {'counts': {'events': {'xmau': 40, 'package': 60}}}
    sql_metrics = {'counts': {'events': {'license': 30, 'projects': 90}}}
    res = usage_ping_test._merge_dicts(redis_metrics, sql_metrics)
    assert res == {'counts': {'events': {'xmau': 40, 'package': 60, 'license': 30, 'projects': 90}}}

    # duplicate k:v's (events: 20) become one k:v, and distinct snippets/packages k:v's are merged
    redis_metrics = {'counts': {'events': 20, 'snippets': -3}}
    sql_metrics = {'counts': {'events': 20, 'packages': -5}}
    res = usage_ping_test._merge_dicts(redis_metrics, sql_metrics)
    assert res == {'counts': {'events': 20, 'snippets': -3, 'packages': -5}}

    # still only one dup key from first assert
    assert len(usage_ping_test.duplicate_keys) == 1
