{{ config(
    
        materialized = "incremental",
        unique_key = "fct_behavior_unstructured_sk",
        full_refresh = true if flags.FULL_REFRESH and var('full_refresh_force', false) else false,
        on_schema_change = 'sync_all_columns'

) }}

{{ simple_cte([
    ('events', 'prep_snowplow_unnested_events_all'),
    ('dim_page', 'dim_behavior_website_page')
    ])
}}

, unstruct_event AS (

    SELECT
      event_id,
      behavior_at,
      event,
      dim_behavior_event_sk,
      platform,
      gsc_pseudonymized_user_id,
      clean_url_path,
      page_url_host,
      page_url,
      page_url_path,
      page_url_scheme,
      app_id,
      session_id,
      link_click_target_url,
      submit_form_id,
      change_form_id,
      change_form_type,
      change_form_element_id,
      focus_form_element_id,
      focus_form_node_name,
      dim_behavior_browser_sk,
      dim_behavior_operating_system_sk,
      environment
    FROM events
    WHERE event = 'unstruct'
    AND behavior_at >= DATEADD(MONTH, -25, CURRENT_DATE)

    {% if is_incremental() %}

    AND behavior_at > (SELECT max(behavior_at) FROM {{ this }})

    {% endif %}

), unstruct_event_with_dims AS (

    SELECT

      -- Primary Key
      {{ dbt_utils.surrogate_key(['event_id','behavior_at']) }} AS fct_behavior_unstructured_sk,

      -- Natural Key
      unstruct_event.app_id,
      unstruct_event.event_id,
      unstruct_event.session_id,

      -- Surrogate Keys
      unstruct_event.dim_behavior_event_sk,
      dim_page.dim_behavior_website_page_sk,
      unstruct_event.dim_behavior_browser_sk,
      unstruct_event.dim_behavior_operating_system_sk,

      --Time Attributes
      behavior_at,

      -- Google Key
      gsc_pseudonymized_user_id,

      -- Attributes
      link_click_target_url,
      submit_form_id,
      change_form_id,
      change_form_type,
      change_form_element_id,
      focus_form_element_id,
      focus_form_node_name
    FROM unstruct_event
    INNER JOIN dim_event 
      ON unstruct_event.event_name = dim_event.event_name
    INNER JOIN dim_page 
      ON unstruct_event.page_url = dim_page.page_url
        AND unstruct_event.app_id = dim_page.app_id
        AND unstruct_event.page_url_scheme = dim_page.page_url_scheme
    LEFT JOIN dim_behavior_browser
      ON unstruct_event.browser_name = dim_behavior_browser.browser_name
        AND unstruct_event.browser_major_version = dim_behavior_browser.browser_major_version
        AND unstruct_event.browser_minor_version = dim_behavior_browser.browser_minor_version
        AND unstruct_event.browser_language = dim_behavior_browser.browser_language
    LEFT JOIN dim_behavior_operating_system
      ON unstruct_event.os_name = dim_behavior_operating_system.os_name
        AND unstruct_event.os_timezone = dim_behavior_operating_system.os_timezone

)

{{ dbt_audit(
    cte_ref="unstruct_event_with_dims",
    created_by="@chrissharp",
    updated_by="@michellecooper",
    created_date="2022-09-27",
    updated_date="2022-12-05"
) }}
