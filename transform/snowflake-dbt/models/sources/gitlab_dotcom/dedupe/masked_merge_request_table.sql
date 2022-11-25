{{ config({
    "materialized": "table",
    "unique_key": "id"
    })
}}

SELECT
 rbacovic_prep.HASHED_USER_ID_TEST_TABLES.pseudonymize_attribute(NVL(id,0))                          AS id
,rbacovic_prep.HASHED_USER_ID_TEST_TABLES.pseudonymize_attribute(NVL(iid,0))                          AS iid
,rbacovic_prep.HASHED_USER_ID_TEST_TABLES.pseudonymize_attribute(NVL(title,''))                        AS title
,rbacovic_prep.HASHED_USER_ID_TEST_TABLES.pseudonymize_attribute(NVL(merge_error,''))                  AS merge_error
,rbacovic_prep.HASHED_USER_ID_TEST_TABLES.pseudonymize_attribute(NVL(assignee_id,0))                  AS assignee_id
,rbacovic_prep.HASHED_USER_ID_TEST_TABLES.pseudonymize_attribute(NVL(updated_by_id,0))                AS updated_by_id
,rbacovic_prep.HASHED_USER_ID_TEST_TABLES.pseudonymize_attribute(NVL(merge_user_id,0))                AS merge_user_id
,rbacovic_prep.HASHED_USER_ID_TEST_TABLES.pseudonymize_attribute(NVL(last_edited_by_id,0))            AS last_edited_by_id
,rbacovic_prep.HASHED_USER_ID_TEST_TABLES.pseudonymize_attribute(NVL(milestone_id,0))                 AS milestone_id
,rbacovic_prep.HASHED_USER_ID_TEST_TABLES.pseudonymize_attribute(NVL(head_pipeline_id,0))             AS head_pipeline_id
,rbacovic_prep.HASHED_USER_ID_TEST_TABLES.pseudonymize_attribute(NVL(latest_merge_request_diff_id,0)) AS latest_merge_request_diff_id
,rbacovic_prep.HASHED_USER_ID_TEST_TABLES.pseudonymize_attribute(NVL(approvals_before_merge,0))       AS approvals_before_merge
,rbacovic_prep.HASHED_USER_ID_TEST_TABLES.pseudonymize_attribute(NVL(lock_version,0))                 AS lock_version
,time_estimate                AS time_estimate
,rbacovic_prep.HASHED_USER_ID_TEST_TABLES.pseudonymize_attribute(NVL(source_project_id,0))            AS source_project_id
,rbacovic_prep.HASHED_USER_ID_TEST_TABLES.pseudonymize_attribute(NVL(target_project_id,0))            AS target_project_id
,rbacovic_prep.HASHED_USER_ID_TEST_TABLES.pseudonymize_attribute(NVL(author_id,0))                    AS author_id
,DATE_TRUNC('day',created_at::TIMESTAMP)                   AS created_at
,DATE_TRUNC('day',updated_at::TIMESTAMP)                   AS updated_at
,DATE_TRUNC('day',last_edited_at::TIMESTAMP)               AS last_edited_at
,rbacovic_prep.HASHED_USER_ID_TEST_TABLES.pseudonymize_attribute(NVL(description,''))                  AS description
-- FROM RBACOVIC_PREP.gitlab_dotcom.masked_merge_requests
FROM {{ source('gitlab_dotcom', 'merge_requests') }}

{% if is_incremental() %}

WHERE updated_at >= (SELECT MAX(updated_at) FROM {{this}})

{% endif %}
LIMIT 10000000