{{config(

    materialized='incremental',
    unique_key='behavior_structured_event_pk',
    full_refresh= only_force_full_refresh(),
    on_schema_change='sync_all_columns',
    post_hook=["{{ rolling_window_delete('behavior_at','month',24) }}"]

  )

}}
{{ 
    simple_cte([
    ('prep_snowplow_structured_event_all_source', 'prep_snowplow_structured_event_all_source')
    ])
}}

, dim_behavior_website_page AS (

    SELECT 
      dim_behavior_website_page.dim_behavior_website_page_sk,
      dim_behavior_website_page.clean_url_path,
      dim_behavior_website_page.page_url_host,
      dim_behavior_website_page.app_id,
      dim_behavior_website_page.page_url,
      dim_behavior_website_page.page_url_path
    FROM {{ ref('dim_behavior_website_page') }}

), dim_behavior_browser AS (

    SELECT 
      dim_behavior_browser.dim_behavior_browser_sk,
      dim_behavior_browser.browser_name,
      dim_behavior_browser.browser_major_version,
      dim_behavior_browser.browser_minor_version,
      dim_behavior_browser.browser_language
    FROM {{ ref('dim_behavior_browser') }}

), dim_behavior_operating_system AS (

    SELECT 
      dim_behavior_operating_system.dim_behavior_operating_system_sk,
      dim_behavior_operating_system.os_name,
      dim_behavior_operating_system.os_timezone
    FROM {{ ref('dim_behavior_operating_system') }}

), dim_behavior_event AS (

    SELECT
      dim_behavior_event_sk,
      event,
      event_name,
      platform,
      environment,
      event_category,
      event_action,
      event_label,
      event_property
    FROM {{ ref('dim_behavior_event') }}
    WHERE event = 'struct'

), structured_events_w_clean_url AS (

    SELECT 
      prep_snowplow_structured_event_all_source.*,
      {{ clean_url('prep_snowplow_structured_event_all_source.page_url_path') }}  AS clean_url_path
    FROM prep_snowplow_structured_event_all_source
    WHERE derived_tstamp > DATEADD(MONTH, -25, CURRENT_DATE)

), structured_events_w_dim AS (

    SELECT

      -- Primary Key
      structured_events_w_clean_url.event_id   AS behavior_structured_event_pk,

      -- Foreign Keys
      dim_behavior_website_page.dim_behavior_website_page_sk,
      dim_behavior_browser.dim_behavior_browser_sk,
      dim_behavior_operating_system.dim_behavior_operating_system_sk,
      structured_events_w_clean_url.gsc_namespace_id AS dim_namespace_id,
      structured_events_w_clean_url.gsc_project_id AS dim_project_id,
      dim_behavior_event.dim_behavior_event_sk,

      -- Time Attributes
      structured_events_w_clean_url.dvce_created_tstamp,
      structured_events_w_clean_url.derived_tstamp AS behavior_at,

      -- Degenerate Dimensions (Event Attributes)
      structured_events_w_clean_url.v_tracker,
      structured_events_w_clean_url.session_index,
      structured_events_w_clean_url.app_id,
      structured_events_w_clean_url.session_id,
      structured_events_w_clean_url.user_snowplow_domain_id,
      structured_events_w_clean_url.contexts,

      -- Degenerate Dimensions (Gitlab Standard Context Attributes)
      structured_events_w_clean_url.gsc_google_analytics_client_id,
      structured_events_w_clean_url.gsc_pseudonymized_user_id,
      structured_events_w_clean_url.gsc_environment,
      structured_events_w_clean_url.gsc_extra,
      structured_events_w_clean_url.gsc_plan,
      structured_events_w_clean_url.gsc_source

    FROM structured_events_w_clean_url
    LEFT JOIN dim_behavior_website_page
      ON structured_events_w_clean_url.page_url = dim_behavior_website_page.page_url
        AND structured_events_w_clean_url.app_id = dim_behavior_website_page.app_id
        AND structured_events_w_clean_url.page_url_scheme = dim_behavior_website_page.page_url_scheme
    LEFT JOIN dim_behavior_browser
      ON structured_events_w_clean_url.browser_name = dim_behavior_browser.browser_name
        AND structured_events_w_clean_url.browser_major_version = dim_behavior_browser.browser_major_version
        AND structured_events_w_clean_url.browser_minor_version = dim_behavior_browser.browser_minor_version
        AND structured_events_w_clean_url.browser_language = dim_behavior_browser.browser_language
    LEFT JOIN dim_behavior_operating_system
      ON structured_events_w_clean_url.os_name = dim_behavior_operating_system.os_name
        AND structured_events_w_clean_url.os_timezone = dim_behavior_operating_system.os_timezone
    LEFT JOIN dim_behavior_event
      ON structured_events_w_clean_url.event_action = dim_behavior_event.event_action
        AND structured_events_w_clean_url.event_category = dim_behavior_event.event_category
        AND structured_events_w_clean_url.event_label = dim_behavior_event.event_label
        AND structured_events_w_clean_url.event_property = dim_behavior_event.event_property
        AND structured_events_w_clean_url.gsc_environment = dim_behavior_event.environment
    WHERE DATE_TRUNC(MONTH, behavior_at) >= DATEADD(MONTH, -24, DATE_TRUNC(MONTH, CURRENT_DATE))
    {% if is_incremental() %}
      AND behavior_at >= (SELECT MAX(behavior_at) FROM {{ this }})
    {% endif %}

)

{{ dbt_audit(
    cte_ref="structured_events_w_dim",
    created_by="@michellecooper",
    updated_by="@chrissharp",
    created_date="2022-09-01",
    updated_date="2022-11-21"
) }}
