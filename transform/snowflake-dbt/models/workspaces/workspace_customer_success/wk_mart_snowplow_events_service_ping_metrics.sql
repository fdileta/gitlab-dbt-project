{{
  config(
    materialized='view',
    tags=["mnpi_exception"]
  )
}}

WITH service_ping_events AS (
  SELECT
    *
  FROM {{ ref('wk_fct_snowplow_events_service_ping') }}
),

metric_bridge AS (
  SELECT
    *
  FROM {{ ref('bdg_metrics_redis_events') }}
),

joined AS (
  SELECT
    service_ping_events.behavior_structured_event_pk,
    service_ping_events.behavior_at,
    service_ping_events.gsc_pseudonymized_user_id,
    service_ping_events.dim_namespace_id,
    service_ping_events.dim_project_id,
    service_ping_events.gsc_plan,
    service_ping_events.ultimate_parent_namespace_id,
    metric_bridge.metrics_path,
    metric_bridge.aggregate_operator,
    metric_bridge.aggregate_attribute,
    metric_bridge.metrics_status
  FROM service_ping_events
  INNER JOIN metric_bridge ON service_ping_events.redis_event_name = metric_bridge.redis_event
)

{{ dbt_audit(
    cte_ref="joined",
    created_by="@mdrussell",
    updated_by="@mdrussell",
    created_date="2022-12-21",
    updated_date="2023-01-11"
) }}