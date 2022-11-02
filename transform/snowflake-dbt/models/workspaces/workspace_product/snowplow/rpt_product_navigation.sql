{{ config({
    "materialized": "incremental",
    "unique_key": "event_id"
    })
}}

{{ simple_cte([
    ('fct_behavior_structured_event_without_assignment','fct_behavior_structured_event_without_assignment'),
    ('dim_behavior_event','dim_behavior_event'),
    ('dim_behavior_operating_system', 'dim_behavior_operating_system')
]) }}

, product_navigation AS (

  SELECT
    fct_behavior_structured_event_without_assignment.behavior_at,
    fct_behavior_structured_event_without_assignment.gsc_pseudonymized_user_id,
    dim_behavior_event.event_category,
    dim_behavior_event.event_action,
    dim_behavior_event.event_label,
    dim_behavior_event.event_property,
    fct_behavior_structured_event_without_assignment.gsc_plan,
    dim_behavior_operating_system.device_type,
    dim_behavior_structured_event.behavior_structured_event_pk AS event_id,
    fct_behavior_structured_event_without_assignment.app_id,
    fct_behavior_structured_event_without_assignment.dim_namespace_id,
    fct_behavior_structured_event_without_assignment.session_id
  FROM fct_behavior_structured_event_without_assignment
  LEFT JOIN dim_behavior_event
    ON fct_behavior_structured_event_without_assignment.dim_behavior_event_sk = dim_behavior_event.dim_behavior_event_sk
  LEFT JOIN dim_behavior_operating_system
    ON fct_behavior_structured_event_without_assignment.dim_behavior_operating_system_sk = dim_behavior_operating_system.dim_behavior_operating_system_sk
  WHERE fct_behavior_structured_event_without_assignment.behavior_at >= '2022-01-01'
    AND (
      (
        dim_behavior_event.event_label IN (
          'main_navigation',
          'profile_dropdown',
          'groups_side_navigation',
          'kubernetes_sections_tabs'
        )
      ) OR
      (
        dim_behavior_event.event_action IN (
          'click_whats_new_drawer',
          'click_forum'
        )
      ) OR
      (
        dim_behavior_event.event_label IN (
          'Menu',
          'groups_dropdown',
          'projects_dropdown'
        )
        AND dim_behavior_event.event_action = 'click_dropdown'
      ) OR
      (
        dim_behavior_event.event_action IN (
          'click_menu_item',
          'click_menu'
        )
        AND dim_behavior_event.event_category LIKE 'projects%'
      ) OR
      (
        dim_behavior_event.event_label IN (
          'packages_registry',
          'container_registry',
          'infrastructure_registry',
          'kubernetes',
          'terraform'
        )
        AND dim_behavior_event.event_action = 'click_menu_item'
      ) OR
      (
        dim_behavior_event.event_action = 'render' AND event_label = 'user_side_navigation'
      )
    )
  {% if is_incremental() %}

    AND  fct_behavior_structured_event_without_assignment.behavior_at > (SELECT MAX(fct_behavior_structured_event_without_assignment.behavior_at) FROM {{ this }})

  {% endif %}
)

{{ dbt_audit(
    cte_ref="product_navigation",
    created_by="@michellecooper",
    updated_by="@michellecooper",
    created_date="2022-11-01",
    updated_date="2022-11-01"
) }}