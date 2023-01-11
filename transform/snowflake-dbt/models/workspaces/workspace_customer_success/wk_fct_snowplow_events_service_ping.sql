{{
  config(
    materialized='incremental',
    tags=["mnpi_exception"]
  )
}}

WITH redis_clicks AS (
  SELECT
    -- event_id,
    behavior_at, 
    gsc_pseudonymized_user_id,
    dim_namespace_id,
    dim_project_id,
    gsc_plan,
    contexts
  FROM {{ ref('fct_behavior_structured_event') }}
  WHERE behavior_at >= '2022-11-01' -- no events added to SP context before Nov 2022
),

namespaces AS (
  SELECT
    *
  FROM {{ ref('dim_namespace') }}
),

joined AS (
  SELECT
    redis_clicks.*,
    namespaces.ultimate_parent_namespace_id
  FROM redis_clicks
  LEFT JOIN namespaces ON namespaces.ultimate_parent_namespace_id = redis_clicks.dim_namespace_id
),

final AS (
  SELECT
    -- joined.event_id,
    joined.behavior_at,
    joined.gsc_pseudonymized_user_id,
    joined.dim_namespace_id,
    joined.dim_project_id,
    joined.gsc_plan,
    joined.ultimate_parent_namespace_id,
    flat_contexts.value['data']['event_name']::VARCHAR AS redis_event_name
  FROM joined
  INNER JOIN LATERAL FLATTEN(input => TRY_PARSE_JSON(joined.contexts), path => 'data') AS flat_contexts
  WHERE flat_contexts.value['schema']::VARCHAR = 'iglu:com.gitlab/gitlab_service_ping/jsonschema/1-0-0'
  {% if is_incremental() %}
  
      AND joined.behavior_at >= (SELECT MAX(behavior_at) FROM {{this}})
  
  {% endif %}
)

{{ dbt_audit(
    cte_ref="final",
    created_by="@mdrussell",
    updated_by="@mdrussell",
    created_date="2022-12-21",
    updated_date="2022-12-21"
) }}