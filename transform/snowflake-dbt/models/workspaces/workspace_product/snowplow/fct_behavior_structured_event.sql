{{config({

        "unique_key":"behavior_structured_event_pk",
        "materialized":"vault_insert_by_period",
        "period":"month",
        "timestamp_field":"BEHAVIOR_AT",
        "start_date": "2020-09-01" 
  })

}}
{{ 
    simple_cte([
    ('dim_behavior_page_url_path', 'dim_behavior_page_url_path'),
    ('dim_behavior_website_page', 'dim_behavior_website_page'),
    ('dim_behavior_browser', 'dim_behavior_browser'),
    ('dim_behavior_operating_system', 'dim_behavior_operating_system'),
    ('prep_snowplow_structured_event_all_source', 'prep_snowplow_structured_event_all_source')
    ])
}}

, structured_events_w_dim AS (

    SELECT

      -- Primary Key
      prep_snowplow_structured_event_all_source.event_id   AS behavior_structured_event_pk,

      -- Foreign Keys
      dim_behavior_website_page.dim_behavior_website_page_sk,
      dim_behavior_page_url_path.dim_behavior_page_url_path_sk,
      dim_behavior_browser.dim_behavior_browser_sk,
      dim_behavior_operating_system.dim_behavior_operating_system_sk,
      prep_snowplow_structured_event_all_source.gsc_namespace_id AS dim_namespace_id,
      prep_snowplow_structured_event_all_source.gsc_project_id AS dim_project_id,

      -- Time Attributes
      prep_snowplow_structured_event_all_source.dvce_created_tstamp,
      prep_snowplow_structured_event_all_source.derived_tstamp AS behavior_at,

      -- Degenerate Dimensions (Event Attributes)
      prep_snowplow_structured_event_all_source.event_action,
      prep_snowplow_structured_event_all_source.event_category,
      prep_snowplow_structured_event_all_source.event_label,
      prep_snowplow_structured_event_all_source.event_property,
      prep_snowplow_structured_event_all_source.event_value,
      prep_snowplow_structured_event_all_source.v_tracker,
      prep_snowplow_structured_event_all_source.session_index,
      prep_snowplow_structured_event_all_source.app_id,
      prep_snowplow_structured_event_all_source.session_id,
      prep_snowplow_structured_event_all_source.user_snowplow_domain_id,
      prep_snowplow_structured_event_all_source.page_url,
      prep_snowplow_structured_event_all_source.page_url_scheme,
      prep_snowplow_structured_event_all_source.page_url_host,
      prep_snowplow_structured_event_all_source.page_url_fragment,
      prep_snowplow_structured_event_all_source.contexts,

      -- Degenerate Dimensions (Gitlab Standard Context Attributes)
      prep_snowplow_structured_event_all_source.gsc_google_analytics_client_id,
      prep_snowplow_structured_event_all_source.gsc_pseudonymized_user_id,
      prep_snowplow_structured_event_all_source.gsc_environment,
      prep_snowplow_structured_event_all_source.gsc_extra,
      prep_snowplow_structured_event_all_source.gsc_plan,
      prep_snowplow_structured_event_all_source.gsc_source

    FROM prep_snowplow_structured_event_all_source
    LEFT JOIN  dim_behavior_page_url_path
      ON prep_snowplow_structured_event_all_source.page_url_path = dim_behavior_page_url_path.page_url_path
    LEFT JOIN dim_behavior_website_page
      ON dim_behavior_page_url_path.clean_url_path = dim_behavior_website_page.clean_url_path
        AND prep_snowplow_structured_event_all_source.page_url_host = dim_behavior_website_page.page_url_host
        AND prep_snowplow_structured_event_all_source.app_id = dim_behavior_website_page.app_id
    LEFT JOIN dim_behavior_browser
      ON prep_snowplow_structured_event_all_source.browser_name = dim_behavior_browser.browser_name
        AND prep_snowplow_structured_event_all_source.browser_major_version = dim_behavior_browser.browser_major_version
        AND prep_snowplow_structured_event_all_source.browser_minor_version = dim_behavior_browser.browser_minor_version
        AND prep_snowplow_structured_event_all_source.browser_language = dim_behavior_browser.browser_language
    LEFT JOIN dim_behavior_operating_system
      ON prep_snowplow_structured_event_all_source.os_name = dim_behavior_operating_system.os_name
        AND prep_snowplow_structured_event_all_source.os_timezone = dim_behavior_operating_system.os_timezone
    WHERE __PERIOD_FILTER__
    
    {% if is_incremental() %}

    AND behavior_at > (SELECT MAX(behavior_at) FROM {{this}})

    {% endif %}
)

{{ dbt_audit(
    cte_ref="structured_events_w_dim",
    created_by="@michellecooper",
    updated_by="@michellecooper",
    created_date="2022-09-01",
    updated_date="2022-09-28"
) }}
