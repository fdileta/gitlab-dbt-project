{{
  config(
    materialized='table',
    tags=["mnpi_exception"]
  )
}}

WITH events AS (
  SELECT
    *
  FROM {{ ref('wk_mart_snowplow_events_service_ping_metrics') }}
  -- only include non-aggregated metrics or aggregated metrics of 'OR' aggregate operator.
  -- more details here: https://gitlab.com/gitlab-org/gitlab/-/issues/376244#note_1167575425
  WHERE aggregate_operator = 'OR'
    OR aggregate_operator IS NULL
)

final AS (
  SELECT
    DATE_TRUNC('month', behavior_at) AS date_month,
    ultimate_parent_namespace_id,
    metrics_path,
    COUNT(DISTINCT gsc_pseudonymized_user_id) AS distinct_users
  FROM events
  {{ dbt_utils.group_by(n = 3) }}
)
