{{ config(
        materialized = "incremental",
        unique_key = "behavior_structured_event_pk",
        enabled = false
) }}

-- depends_on: {{ ref('snowplow_structured_events') }}
{{
    simple_cte([
    ('prep_snowplow_structured_event_all_source', 'prep_snowplow_structured_event_all_source'),
    ('dim_behavior_website_page', 'dim_behavior_website_page')
    ])
}}

, structured_events_w_clean_url AS (

    SELECT
      prep_snowplow_structured_event_all_source.*,
      {{ clean_url('page_url_path') }}   AS clean_url_path,
      REGEXP_SUBSTR(page_url_path, 'namespace(\\d+)', 1, 1, 'e', 1)     AS dim_namespace_id,
      REGEXP_SUBSTR(page_url_path, 'project(\\d+)', 1, 1, 'e', 1)   AS dim_project_id,
      derived_tstamp    AS behavior_at
    FROM prep_snowplow_structured_event_all_source

    {% if is_incremental() %}

    WHERE behavior_at > (SELECT MAX(behavior_at) FROM {{this}})

    {% endif %}

)

, structured_events_w_dim AS (

    SELECT

      -- Primary Key
      {{ dbt_utils.surrogate_key(['structured_events_w_clean_url.event_id']) }}   AS behavior_structured_event_pk,

      -- Foreign Keys
      dim_behavior_website_page.dim_behavior_website_page_sk,
      structured_events_w_clean_url.dim_namespace_id,
      structured_events_w_clean_url.dim_project_id,

      -- Natural Keys
      structured_events_w_clean_url.event_id,
      structured_events_w_clean_url.app_id,
      structured_events_w_clean_url.session_id,
      structured_events_w_clean_url.user_snowplow_domain_id,

      -- Time Attributes
      structured_events_w_clean_url.dvce_created_tstamp,
      structured_events_w_clean_url.behavior_at,

      -- Event Attributes
      structured_events_w_clean_url.event_action,
      structured_events_w_clean_url.event_category,
      structured_events_w_clean_url.event_label,
      structured_events_w_clean_url.event_property,
      structured_events_w_clean_url.event_value,
      structured_events_w_clean_url.v_tracker,
      structured_events_w_clean_url.session_index,
      structured_events_w_clean_url.contexts,

      structured_events_w_clean_url.page_url,
      structured_events_w_clean_url.clean_url_path,
      structured_events_w_clean_url.page_url_scheme,
      structured_events_w_clean_url.page_url_host,
      structured_events_w_clean_url.page_url_path,
      structured_events_w_clean_url.page_url_fragment,

      structured_events_w_clean_url.browser_name,
      structured_events_w_clean_url.browser_major_version,
      structured_events_w_clean_url.browser_minor_version,
      structured_events_w_clean_url.browser_language,
      structured_events_w_clean_url.os,
      structured_events_w_clean_url.os_name,
      structured_events_w_clean_url.os_manufacturer,
      structured_events_w_clean_url.os_timezone,
      structured_events_w_clean_url.browser_engine,
      structured_events_w_clean_url.device_type,
      structured_events_w_clean_url.device_is_mobile,

      -- Gitlab Standard Context fields
      structured_events_w_clean_url.gsc_google_analytics_client_id,
      structured_events_w_clean_url.gsc_pseudonymized_user_id,
      structured_events_w_clean_url.gsc_environment,
      structured_events_w_clean_url.gsc_extra,
      structured_events_w_clean_url.gsc_namespace_id,
      structured_events_w_clean_url.gsc_plan,
      structured_events_w_clean_url.gsc_project_id,
      structured_events_w_clean_url.gsc_source

    FROM structured_events_w_clean_url
    LEFT JOIN dim_behavior_website_page
      ON structured_events_w_clean_url.clean_url_path = dim_behavior_website_page.clean_url_path
        AND structured_events_w_clean_url.page_url_host = dim_behavior_website_page.page_url_host
        AND structured_events_w_clean_url.app_id = dim_behavior_website_page.app_id

)

{{ dbt_audit(
    cte_ref="structured_events_w_dim",
    created_by="@michellecooper",
    updated_by="@iweeks",
    created_date="2022-09-01",
    updated_date="2022-09-22"
) }}
