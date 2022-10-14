{{ config(
        materialized = "incremental",
        unique_key = "fct_behavior_unstructured_sk",
        full_refresh = false
) }}

{{ simple_cte([
    ('events', 'fct_behavior_unstructured_event')
    ])
}}

, link_click AS (

    SELECT
      fct_behavior_unstructured_sk,
      behavior_at,
      dim_behavior_event_sk,
      dim_behavior_website_page_sk,
      gsc_pseudonymized_user_id,
      session_id,
      link_click_target_url
    FROM events
    WHERE dim_event.event_name = 'link_click'
      AND behavior_at >= DATEADD(MONTH, -25, CURRENT_DATE)

    {% if is_incremental() %}

      AND events.behavior_at > (SELECT max(behavior_at) FROM {{ this }})

    {% endif %}
)

{{ dbt_audit(
    cte_ref="link_click",
    created_by="@chrissharp",
    updated_by="@chrissharp",
    created_date="2022-09-22",
    updated_date="2022-09-27"
) }}