{{ config({
    "materialized": "table"
    })
}}

WITH filtered_snowplow_events AS (

  SELECT
    derived_tstamp,
    gsc_pseudonymized_user_id,
    event_category,
    event_action,
    event_label,
    event_property,
    gsc_plan,
    device_type,
    event_id
  FROM {{ ref('snowplow_structured_events_400') }}
  WHERE derived_tstamp >= '2022-01-01'
    AND (
      (
        event_label IN (
        'main_navigation',
        'click_whats_new_drawer',
        'click_forum',
        'profile_dropdown',
        'groups_side_navigation',
        'kubernetes_sections_tabs'
        )
      ) OR
      (
        event_label IN (
          'Menu',
          'groups_dropdown',
          'projects_dropdown'
        )
        AND event_action = 'click_dropdown'
      ) OR
      (
        event_action IN (
          'click_menu_item',
          'click_menu'
        )
        AND event_category LIKE 'projects%'
      ) OR
      (
        event_label IN (
          'packages_registry',
          'container_registry',
          'infrastructure_registry',
          'kubernetes',
          'terraform'
        )
        AND event_action = 'click_menu_item'
      ) OR
      (
        event_action = 'render' AND event_label = 'user_side_navigation'
      )
    )
)

{{ dbt_audit(
    cte_ref="filtered_snowplow_events",
    created_by="@mdrussell",
    updated_by="@mdrussell",
    created_date="2022-09-30",
    updated_date="2022-09-30"
) }}
