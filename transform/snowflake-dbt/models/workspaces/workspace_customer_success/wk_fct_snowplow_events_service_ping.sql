{{
  config(
    materialized='incremental',
    tags=["mnpi_exception"]
  )
}}

WITH redis_clicks AS (
  SELECT
    behavior_structured_event_pk,
    behavior_at, 
    gsc_pseudonymized_user_id,
    dim_namespace_id,
    dim_project_id,
    gsc_plan
  FROM {{ ref('fct_behavior_structured_event') }}
  WHERE behavior_at >= '2022-11-01' -- no events added to SP context before Nov 2022
),

namespaces AS (
  SELECT
    *
  FROM {{ ref('dim_namespace') }}
),

contexts AS (
  SELECT
    *
  FROM {{ ref('fct_behavior_structured_event_service_ping_context') }}
),

final AS (
  SELECT
    redis_clicks.behavior_structured_event_pk,
    redis_clicks.behavior_at,
    redis_clicks.gsc_pseudonymized_user_id,
    redis_clicks.dim_namespace_id,
    redis_clicks.dim_project_id,
    redis_clicks.gsc_plan,
    namespaces.ultimate_parent_namespace_id,
    contexts.redis_event_name
  FROM redis_clicks
  INNER JOIN contexts ON contexts.behavior_structured_event_pk = redis_clicks.behavior_structured_event_pk
  LEFT JOIN namespaces ON namespaces.dim_namespace_id = redis_clicks.dim_namespace_id
  {% if is_incremental() %}
  
      AND redis_clicks.behavior_at >= (SELECT MAX(behavior_at) FROM {{this}})
  
  {% endif %}
)

{{ dbt_audit(
    cte_ref="final",
    created_by="@mdrussell",
    updated_by="@mdrussell",
    created_date="2022-12-21",
    updated_date="2023-01-11"
) }}