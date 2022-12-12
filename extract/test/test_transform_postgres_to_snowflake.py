"""
Test unit to ensure quality of transformation algorithm
from Postgres to Snowflake
"""
import pytest

import sqlparse

from extract.saas_usage_ping.transform_postgres_to_snowflake import (
    META_API_COLUMNS,
    TRANSFORMED_INSTANCE_QUERIES_FILE,
    META_DATA_INSTANCE_QUERIES_FILE,
    HAVING_CLAUSE_PATTERN,
    METRICS_EXCEPTION,
    transform,
    get_optimized_token,
    get_translated_postgres_count,
    get_keyword_index,
    get_sql_as_string,
    get_token_list,
    get_tokenized_query_list,
    get_transformed_token_list,
    get_renamed_query_tables,
    get_renamed_table_name,
    is_for_renaming,
    perform_action_on_query_str,
)


def test_static_variables():
    """
    Test case: check static variables
    """
    assert META_API_COLUMNS == [
        "recorded_at",
        "version",
        "edition",
        "recording_ce_finished_at",
        "recording_ee_finished_at",
        "uuid",
    ]
    assert TRANSFORMED_INSTANCE_QUERIES_FILE == "transformed_instance_queries.json"
    assert META_DATA_INSTANCE_QUERIES_FILE == "meta_data_instance_queries.json"


def test_constants():
    """
    Test contants to ensure there are in the proper place
    with proper values
    """
    assert TRANSFORMED_INSTANCE_QUERIES_FILE is not None
    assert META_DATA_INSTANCE_QUERIES_FILE is not None
    assert HAVING_CLAUSE_PATTERN is not None
    assert METRICS_EXCEPTION is not None


@pytest.fixture(name="transformed_dict")
def test_cases_dict_transformed():
    """
    Prepare fixture data for testing
    """
    test_cases_dict_subquery = {
        "usage_activity_by_stage_monthly": {
            "create": {
                "approval_project_rules_with_more_approvers_than_required": 'SELECT COUNT(*) FROM (SELECT COUNT("approval_project_rules"."id") FROM "approval_project_rules" INNER JOIN approval_project_rules_users ON approval_project_rules_users.approval_project_rule_id = approval_project_rules.id WHERE "approval_project_rules"."rule_type" = 0 GROUP BY "approval_project_rules"."id" HAVING (COUNT(approval_project_rules_users) > approvals_required)) subquery',
                "approval_project_rules_with_less_approvers_than_required": 'SELECT COUNT(*) FROM (SELECT COUNT("approval_project_rules"."id") FROM "approval_project_rules" INNER JOIN approval_project_rules_users ON approval_project_rules_users.approval_project_rule_id = approval_project_rules.id WHERE "approval_project_rules"."rule_type" = 0 GROUP BY "approval_project_rules"."id" HAVING (COUNT(approval_project_rules_users) < approvals_required)) subquery',
                "approval_project_rules_with_exact_required_approvers": 'SELECT COUNT(*) FROM (SELECT COUNT("approval_project_rules"."id") FROM "approval_project_rules" INNER JOIN approval_project_rules_users ON approval_project_rules_users.approval_project_rule_id = approval_project_rules.id WHERE "approval_project_rules"."rule_type" = 0 GROUP BY "approval_project_rules"."id" HAVING (COUNT(approval_project_rules_users) = approvals_required)) subquery',
            }
        },
        "usage_activity_by_stage": {
            "create": {
                "approval_project_rules_with_more_approvers_than_required": 'SELECT COUNT(*) FROM (SELECT COUNT("approval_project_rules"."id") FROM "approval_project_rules" INNER JOIN approval_project_rules_users ON approval_project_rules_users.approval_project_rule_id = approval_project_rules.id WHERE "approval_project_rules"."rule_type" = 0 GROUP BY "approval_project_rules"."id" HAVING (COUNT(approval_project_rules_users) > approvals_required)) subquery',
                "approval_project_rules_with_less_approvers_than_required": 'SELECT COUNT(*) FROM (SELECT COUNT("approval_project_rules"."id") FROM "approval_project_rules" INNER JOIN approval_project_rules_users ON approval_project_rules_users.approval_project_rule_id = approval_project_rules.id WHERE "approval_project_rules"."rule_type" = 0 GROUP BY "approval_project_rules"."id" HAVING (COUNT(approval_project_rules_users) < approvals_required)) subquery',
                "approval_project_rules_with_exact_required_approvers": 'SELECT COUNT(*) FROM (SELECT COUNT("approval_project_rules"."id") FROM "approval_project_rules" INNER JOIN approval_project_rules_users ON approval_project_rules_users.approval_project_rule_id = approval_project_rules.id WHERE "approval_project_rules"."rule_type" = 0 GROUP BY "approval_project_rules"."id" HAVING (COUNT(approval_project_rules_users) = approvals_required)) subquery',
            }
        },
    }

    return transform(test_cases_dict_subquery)

@pytest.fixture(name="actual_dict")
def prepared_dict(transformed_dict):
    """
    Prepare nested dict for testing
    """
    prepared = [metrics_query for sub_dict in transformed_dict.values() for metrics_query in sub_dict.values()]

    return {metric_name: metrics_query[metric_name] for metrics_query in prepared for metric_name in
              metrics_query.keys()}

@pytest.fixture(name="list_of_metrics")
def metric_list() -> set:
    """
    static list of metrics for testing complex subqueries
    """
    return {
        "approval_project_rules_with_more_approvers_than_required",
        "approval_project_rules_with_less_approvers_than_required",
        "approval_project_rules_with_exact_required_approvers",
        "approval_project_rules_with_more_approvers_than_required",
        "approval_project_rules_with_less_approvers_than_required",
        "approval_project_rules_with_exact_required_approvers",
    }


def test_transform_postgres_snowflake() -> None:
    """
    Check Postgres -> Snowflake query transformation
    :return:
    """

    input_query = {"my_metrics": "SELECT 1 from DUAL"}

    actual = dict(
        my_metrics="SELECT 'my_metrics' AS counter_name,  "
        "1 AS counter_value, "
        "TO_DATE(CURRENT_DATE) AS run_day   "
        "from DUAL"
    )

    assert actual == transform(input_query)


def test_transforming_queries():
    """
    Test case: for transforming queries from Postgres to Snowflake
    """
    test_cases_dict = {
        "counts": {
            "boards": 'SELECT COUNT("boards"."id") FROM "boards"',
            "clusters_applications_cert_managers": 'SELECT COUNT(DISTINCT "clusters_applications_cert_managers"."clusters.user_id") '
            'FROM "clusters_applications_cert_managers" '
            'INNER JOIN "clusters" '
            'ON "clusters"."id" = "clusters_applications_cert_managers"."cluster_id" '
            'WHERE "clusters_applications_cert_managers"."status" IN (11, 3, 5)',
            "clusters_platforms_eks": 'SELECT COUNT("clusters"."id") '
            'FROM "clusters" '
            'INNER JOIN "cluster_providers_aws" '
            'ON "cluster_providers_aws"."cluster_id" = "clusters"."id" '
            'WHERE "clusters"."provider_type" = 2 '
            'AND ("cluster_providers_aws"."status" IN (3)) '
            'AND "clusters"."enabled" = TRUE',
            "clusters_platforms_gke": 'SELECT COUNT("clusters"."id") '
            'FROM "clusters" '
            'INNER JOIN "cluster_providers_gcp" '
            'ON "cluster_providers_gcp"."cluster_id" = "clusters"."id" '
            'WHERE "clusters"."provider_type" = 1 '
            'AND ("cluster_providers_gcp"."status" IN (3)) '
            'AND "clusters"."enabled" = TRUE',
            "clusters_platforms_user": 'SELECT COUNT("clusters"."id") '
            'FROM "clusters" '
            'WHERE "clusters"."provider_type" = 0 '
            'AND "clusters"."enabled" = TRUE',
            "incident_labeled_issues": 'SELECT COUNT("issues"."id") '
            'FROM "issues" '
            'INNER JOIN "label_links" '
            'ON "label_links"."target_type" = \'Issue\' '
            'AND "label_links"."target_id" = "issues"."id" '
            'INNER JOIN "labels" ON "labels"."id" = "label_links"."label_id" '
            'WHERE "labels"."title" = \'incident\' '
            'AND "labels"."color" = \'#CC0033\' '
            'AND "labels"."description" = \'Denotes a disruption'
            " to IT services and the associated"
            " issues require immediate attention'",
        }
    }

    actuals_dict = {
        "counts": {
            "boards": "SELECT 'boards' AS counter_name,  COUNT(boards.id) AS counter_value, TO_DATE(CURRENT_DATE) AS run_day   FROM prep.gitlab_dotcom.gitlab_dotcom_boards_dedupe_source AS boards",
            "clusters_applications_cert_managers": "SELECT 'clusters_applications_cert_managers' AS counter_name,  COUNT(DISTINCT clusters.user_id) AS counter_value, TO_DATE(CURRENT_DATE) AS run_day   FROM prep.gitlab_dotcom.gitlab_dotcom_clusters_applications_cert_managers_dedupe_source AS clusters_applications_cert_managers INNER JOIN prep.gitlab_dotcom.gitlab_dotcom_clusters_dedupe_source AS clusters ON clusters.id = clusters_applications_cert_managers.cluster_id WHERE clusters_applications_cert_managers.status IN (11, 3, 5)",
            "clusters_platforms_eks": "SELECT 'clusters_platforms_eks' AS counter_name,  COUNT(clusters.id) AS counter_value, TO_DATE(CURRENT_DATE) AS run_day   FROM prep.gitlab_dotcom.gitlab_dotcom_clusters_dedupe_source AS clusters INNER JOIN prep.gitlab_dotcom.gitlab_dotcom_cluster_providers_aws_dedupe_source AS cluster_providers_aws ON cluster_providers_aws.cluster_id = clusters.id WHERE clusters.provider_type = 2 AND (cluster_providers_aws.status IN (3)) AND clusters.enabled = TRUE",
            "clusters_platforms_gke": "SELECT 'clusters_platforms_gke' AS counter_name,  COUNT(clusters.id) AS counter_value, TO_DATE(CURRENT_DATE) AS run_day   FROM prep.gitlab_dotcom.gitlab_dotcom_clusters_dedupe_source AS clusters INNER JOIN prep.gitlab_dotcom.gitlab_dotcom_cluster_providers_gcp_dedupe_source AS cluster_providers_gcp ON cluster_providers_gcp.cluster_id = clusters.id WHERE clusters.provider_type = 1 AND (cluster_providers_gcp.status IN (3)) AND clusters.enabled = TRUE",
            "clusters_platforms_user": "SELECT 'clusters_platforms_user' AS counter_name,  COUNT(clusters.id) AS counter_value, TO_DATE(CURRENT_DATE) AS run_day   FROM prep.gitlab_dotcom.gitlab_dotcom_clusters_dedupe_source AS clusters WHERE clusters.provider_type = 0 AND clusters.enabled = TRUE",
            "incident_labeled_issues": "SELECT 'incident_labeled_issues' AS counter_name,  COUNT(issues.id) AS counter_value, TO_DATE(CURRENT_DATE) AS run_day   FROM prep.gitlab_dotcom.gitlab_dotcom_issues_dedupe_source AS issues INNER JOIN prep.gitlab_dotcom.gitlab_dotcom_label_links_dedupe_source AS label_links ON label_links.target_type = 'Issue' AND label_links.target_id = issues.id INNER JOIN prep.gitlab_dotcom.gitlab_dotcom_labels_dedupe_source AS labels ON labels.id = label_links.label_id WHERE labels.title = 'incident' AND labels.color = '#CC0033' AND labels.description = 'Denotes a disruption to IT services and the associated issues require immediate attention'",
        }
    }

    def check_join_prep(sql_metric, sql_query):
        """
        check did we fix the bug with "JOINprep", should be fixed to "JOIN prep."
        """
        final_sql = sql_query.upper()
        print(f"\nfinal_sql: {final_sql}")
        assert "JOINPREP" not in final_sql

        if "JOIN" in final_sql:
            assert "JOIN PREP" in final_sql

        # compare translated query with working SQL
        assert sql_query == actuals_dict["counts"][sql_metric]

    final_sql_query_dict = transform(test_cases_dict)

    perform_action_on_query_str(
        original_dict=final_sql_query_dict,
        action=check_join_prep,
        action_arg_type="both",
    )


def test_scalar_subquery():
    """
    Test case: Scalar subquery :: test SELECT (SELECT 1) -> SELECT (SELECT 1) as counter_value
    """
    test_cases_dict = {
        "counts": {
            "snippets": 'SELECT (SELECT COUNT("snippets"."id") FROM "snippets" WHERE "snippets"."type" = \'PersonalSnippet\') + (SELECT COUNT("snippets"."id") FROM "snippets" WHERE "snippets"."type" = \'ProjectSnippet\')'
        }
    }

    actuals_dict = {
        "counts": {
            "snippets": "SELECT 'snippets' AS counter_name,  (SELECT COUNT(snippets.id) FROM prep.gitlab_dotcom.gitlab_dotcom_snippets_dedupe_source AS snippets WHERE snippets.type = 'PersonalSnippet') + (SELECT COUNT(snippets.id) FROM prep.gitlab_dotcom.gitlab_dotcom_snippets_dedupe_source AS snippets WHERE snippets.type = 'ProjectSnippet') AS counter_value, TO_DATE(CURRENT_DATE) AS run_day  "
        }
    }

    final_sql_query_dict = transform(test_cases_dict)

    for sql_metric, sql_query in final_sql_query_dict["counts"].items():
        # compare translated query with working SQL
        assert sql_query == actuals_dict["counts"][sql_metric]


def test_regular_subquery_transform():
    """
    Test case: regular subquery transform:
    SELECT a
      FROM (SELECT 1 as a
              FROM b) ->
    SELECT 'metric_name', a metric_value
      FROM (SELECT 1 AS a
              FROM b)
    """
    test_cases_dict_subquery = {
        "usage_activity_by_stage_monthly": {
            "create": {
                "merge_requests_with_overridden_project_rules": 'SELECT COUNT(DISTINCT "approval_merge_request_rules"."merge_request_id") '
                'FROM "approval_merge_request_rules" '
                'WHERE "approval_merge_request_rules"."created_at" '
                "BETWEEN '2021-08-14 12:44:36.596707' AND '2021-09-11 12:44:36.596773' "
                "AND ((EXISTS (\n  SELECT\n    1\n  "
                "FROM\n    approval_merge_request_rule_sources\n  "
                "WHERE\n    approval_merge_request_rule_sources.approval_merge_request_rule_id = approval_merge_request_rules.id\n    "
                "AND NOT EXISTS (\n      "
                "SELECT\n        1\n      "
                "FROM\n        approval_project_rules\n      "
                "WHERE\n        approval_project_rules.id = approval_merge_request_rule_sources.approval_project_rule_id\n        "
                'AND EXISTS (\n          SELECT\n            1\n          FROM\n            projects\n          WHERE\n            projects.id = approval_project_rules.project_id\n            AND projects.disable_overriding_approvers_per_merge_request = FALSE))))\n    OR("approval_merge_request_rules"."modified_from_project_rule" = TRUE)\n)'
            }
        }
    }

    actuals_dict_subquery = {
        "usage_activity_by_stage_monthly": {
            "create": {
                "merge_requests_with_overridden_project_rules": "SELECT 'merge_requests_with_overridden_project_rules' AS counter_name,  COUNT(DISTINCT approval_merge_request_rules.merge_request_id) AS counter_value, TO_DATE(CURRENT_DATE) AS run_day   FROM prep.gitlab_dotcom.gitlab_dotcom_approval_merge_request_rules_dedupe_source AS approval_merge_request_rules WHERE approval_merge_request_rules.created_at BETWEEN '2021-08-14 12:44:36.596707' AND '2021-09-11 12:44:36.596773' AND ((EXISTS ( SELECT 1 FROM prep.gitlab_dotcom.gitlab_dotcom_approval_merge_request_rule_sources_dedupe_source AS approval_merge_request_rule_sources WHERE approval_merge_request_rule_sources.approval_merge_request_rule_id = approval_merge_request_rules.id AND NOT EXISTS ( SELECT 1 FROM prep.gitlab_dotcom.gitlab_dotcom_approval_project_rules_dedupe_source AS approval_project_rules WHERE approval_project_rules.id = approval_merge_request_rule_sources.approval_project_rule_id AND EXISTS ( SELECT 1 FROM prep.gitlab_dotcom.gitlab_dotcom_projects_dedupe_source AS projects WHERE projects.id = approval_project_rules.project_id AND projects.disable_overriding_approvers_per_merge_request = FALSE)))) OR(approval_merge_request_rules.modified_from_project_rule = TRUE))"
            }
        }
    }

    final_sql_query_dict = transform(test_cases_dict_subquery)

    for sql_metric, sql_query in final_sql_query_dict[
        "usage_activity_by_stage_monthly"
    ]["create"].items():
        # compare translated query with working SQL
        assert (
            sql_query
            == actuals_dict_subquery["usage_activity_by_stage_monthly"]["create"][
                sql_metric
            ]
        )


def test_optimize_token_size():
    """
    Test case: optimize_token_size
    """
    test_cases_list = [
        "  SELECT  aa.bb  FROM   (SELECT   1 as aa) FROM BB ",
        "   SELECT 1",
        "   SELECT\n a from   bb   ",
        "",
        None,
    ]

    actuals_list = [
        "SELECT aa.bb FROM (SELECT 1 as aa) FROM BB",
        "SELECT 1",
        "SELECT a from bb",
        "",
        "",
    ]
    #

    for i, test_case_list in enumerate(test_cases_list):
        assert get_optimized_token(test_case_list) == actuals_list[i]


def test_count_pg_snowflake():
    """
    Test
    case: COUNT
    from Postgres to
    Snowflake: translate_postgres_snowflake_count
    """
    test_cases_list_count = [
        ["COUNT(DISTINCT aa.bb.cc)"],
        ["COUNT(xx.yy.zz)"],
        ["COUNT( DISTINCT oo.pp.rr)"],
        ["COUNT( xx.yy.zz)"],
        ["COUNT(users.users.id)"],
        None,
        [],
    ]

    actuals_list_count = [
        ["COUNT(DISTINCT bb.cc)"],
        ["COUNT(yy.zz)"],
        ["COUNT(DISTINCT pp.rr)"],
        ["COUNT(yy.zz)"],
        ["COUNT(users.id)"],
        [],
        [],
    ]

    for i, test_case_list_count in enumerate(test_cases_list_count):
        assert (
            get_translated_postgres_count(test_case_list_count) == actuals_list_count[i]
        )


def test_find_keyword_index():
    """
    Test case: find_keyword_index
    """
    test_cases_parse = [
        sqlparse.parse("SELECT FROM")[0].tokens,
        sqlparse.parse("THERE IS NO MY WORD")[0].tokens,
        sqlparse.parse("THIS IS FROM SELECT")[0].tokens,
        sqlparse.parse("MORE SELECT AND ONE MORE FROM KEYWORD")[0].tokens,
    ]

    actuals_parse = [0, 0, 6, 2]

    # 1. looking for SELECT keyword
    for i, test_case_parse in enumerate(test_cases_parse):
        assert get_keyword_index(test_case_parse, "SELECT") == actuals_parse[i]

    actuals_parse = [2, 0, 4, 10]
    # 2. looking for FROM keyword
    for i, test_case_parse in enumerate(test_cases_parse):
        assert get_keyword_index(test_case_parse, "FROM") == actuals_parse[i]


def test_prepare_sql_statement():
    """
    Test case: prepare_sql_statement
    """
    test_cases_prepare = [
        sqlparse.parse("SELECT 1")[0].tokens,
        sqlparse.parse("SELECT abc from def")[0].tokens,
        sqlparse.parse("SELECT (SELECT 1)")[0].tokens,
    ]

    actuals_list_prepare = ["SELECT 1", "SELECT abc from def", "SELECT (SELECT 1)"]

    for i, test_case_prepare in enumerate(test_cases_prepare):
        assert get_sql_as_string(test_case_prepare) == actuals_list_prepare[i]


def test_subquery_complex(transformed_dict, list_of_metrics):
    """
    Test bugs we found for complex subquery with FROM () clause
    """
    expect_value_select, expect_value_from = 2, 2

    final_sql_query_dict = transformed_dict

    for usage_key, create_dict in final_sql_query_dict.items():
        for create_key, metrics_dict in create_dict.items():
            for metric_name, metric_sql in metrics_dict.items():
                assert "approval" in metric_name
                assert metric_name in list_of_metrics
                assert metric_name in metric_sql

                assert (
                    metric_sql.upper().count("SELECT") == expect_value_select
                )  # expect 2 SELECT statement
                assert (
                    metric_sql.upper().count("FROM") == expect_value_from
                )  # expect 2 FROM statements

                assert metric_sql.count("(") == metric_sql.count(
                    ")"
                )  # query parsed properly


def test_transform_having_clause(actual_dict, list_of_metrics):
    """
    Test bugs we found for complex subquery - having clause

    bug fixing, need to move from:
    (COUNT(approval_project_rules_users) < approvals_required)
    to
    (COUNT(approval_project_rules_users.id) < MAX(approvals_required))
    """

    for metric_name, metric_sql in actual_dict.items():
        assert ".id" in metric_sql
        assert "MAX(" in metric_sql
        assert "COUNT(approval_project_rules_users.id)" in metric_sql
        assert "MAX(approvals_required)" in metric_sql
        assert "subquery" in metric_sql
        assert metric_sql.count("(") == metric_sql.count(")")
        assert metric_name in list_of_metrics
        assert metric_name in metric_sql


def test_nested_structure():
    """
    Test that an arbitrarily nested payload will return
    a correctly transformed output, including the following:
        - only select children nodes are kept
        - original json structure of remaining nodes is preserved
    """

    test_cases_dict = {
        "active_user_count": 'SELECT COUNT("users"."id") FROM "users" WHERE ("users"."state" IN (\'active\')) AND ("users"."user_type" IS NULL OR "users"."user_type" IN (6, 4))',
        "counts": {
            "assignee_lists": 'SELECT COUNT("lists"."id") FROM "lists" WHERE "lists"."list_type" = 3',
            "ci_builds": 'SELECT COUNT("ci_builds"."id") FROM "ci_builds" WHERE "ci_builds"."type" = \'Ci::Build\'',
            "ci_triggers": {
                "arbitrary_key": 'SELECT COUNT("ci_triggers"."id") FROM "ci_triggers"'
            },
            "ci_internal_pipelines": -1,
        },
        "counts_weekly": {
            "aggregated_metrics": {
                "compliance_features_track_unique_visits_union": 1135,
                "arbitrary_key": {"arbitrary_key2": {"arbitrary_key3": "SELECT 1"}},
            }
        },
    }

    actuals_dict = {
        "active_user_count": "SELECT 'active_user_count' AS counter_name,  COUNT(users.id) AS counter_value, TO_DATE(CURRENT_DATE) AS run_day   FROM prep.gitlab_dotcom.gitlab_dotcom_users_dedupe_source AS users WHERE (users.state IN ('active')) AND (users.user_type IS NULL OR users.user_type IN (6, 4))",
        "counts": {
            "assignee_lists": "SELECT 'assignee_lists' AS counter_name,  COUNT(lists.id) AS counter_value, TO_DATE(CURRENT_DATE) AS run_day   FROM prep.gitlab_dotcom.gitlab_dotcom_lists_dedupe_source AS lists WHERE lists.list_type = 3",
            "ci_builds": "SELECT 'ci_builds' AS counter_name,  COUNT(ci_builds.id) AS counter_value, TO_DATE(CURRENT_DATE) AS run_day   FROM prep.gitlab_dotcom.gitlab_dotcom_ci_builds_dedupe_source AS ci_builds WHERE ci_builds.type = 'Ci::Build'",
            "ci_triggers": {
                "arbitrary_key": "SELECT 'arbitrary_key' AS counter_name,  COUNT(ci_triggers.id) AS counter_value, TO_DATE(CURRENT_DATE) AS run_day   FROM prep.gitlab_dotcom.gitlab_dotcom_ci_triggers_dedupe_source AS ci_triggers"
            },
        },
        "counts_weekly": {
            "aggregated_metrics": {
                "arbitrary_key": {
                    "arbitrary_key2": {
                        "arbitrary_key3": "SELECT 'arbitrary_key3' AS counter_name,  1 AS counter_value, TO_DATE(CURRENT_DATE) AS run_day  "
                    }
                }
            }
        },
    }

    final_sql_query_dict = transform(test_cases_dict)
    assert actuals_dict == final_sql_query_dict


def test_get_transformed_token_list():
    """
    Check transformation from str to list
    """

    input_value = "SELECT 1 FROM MY_TABLE"

    actual = get_transformed_token_list(sql_query=input_value)

    expected = [
        "SELECT",
        " ",
        "1",
        " ",
        "FROM",
        " ",
        "prep.gitlab_dotcom.gitlab_dotcom_MY_TABLE_dedupe_source AS MY_TABLE",
    ]

    assert actual == expected


def test_get_token_list():
    """
    Check get_token_list
    """

    input_value = "SELECT 1"

    tokenized = get_tokenized_query_list(sql_query=input_value)

    actual = get_token_list(tokenized_list=tokenized)

    expected = ["SELECT", " ", "1"]

    assert actual == expected


def test_get_renamed_query_tables():
    """
    Check get_renamed_query_tables
    """

    input_value = "SELECT 1 FROM my_table"

    actual = get_renamed_query_tables(sql_query=input_value)

    expected = "SELECT 1 FROM prep.gitlab_dotcom.gitlab_dotcom_my_table_dedupe_source AS my_table"

    assert actual == expected


def test_get_renamed_table_name():
    """
    Check get_renamed_table_name
    """

    input_value = "SELECT 1 FROM my_table"

    tokenized = get_tokenized_query_list(sql_query=input_value)

    actual = get_token_list(tokenized_list=tokenized)

    get_renamed_table_name(
        token="SELECT", index=6, tokenized_list=tokenized, token_list=actual
    )

    expected = [
        "SELECT",
        " ",
        "1",
        " ",
        "FROM",
        " ",
        "my_table",
    ]

    assert actual == expected


def test_is_for_renaming_false():
    """
    Check is_for_renaming

    """

    current_token = "FROM"
    next_token = "prod"

    actual = is_for_renaming(current_token=current_token, next_token=next_token)
    expected = False

    assert actual == expected


def test_is_for_renaming_true():
    """
    Check is_for_renaming

    """

    current_token = "SELECT"
    next_token = "1"

    actual = is_for_renaming(current_token=current_token, next_token=next_token)
    expected = True

    assert actual == expected
