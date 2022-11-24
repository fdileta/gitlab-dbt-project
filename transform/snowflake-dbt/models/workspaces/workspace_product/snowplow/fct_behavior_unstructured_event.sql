{{ config(
    
        materialized = "incremental",
        unique_key = "fct_behavior_unstructured_sk",
        full_refresh = true if flags.FULL_REFRESH and var('full_refresh_force', false) else false,
        on_schema_change = 'sync_all_columns'

) }}

{{ simple_cte([
    ('events', 'prep_snowplow_unnested_events_all'),
    ('dim_page', 'dim_behavior_website_page'),
    ])
}}

, unstruct_event AS (

    SELECT
      event_id,
      derived_tstamp                                    AS behavior_at,
      event                                             AS event,
      event_name                                        AS event_name,
      se_action                                         AS event_action,
      se_category                                       AS event_category,
      se_label                                          AS event_label,
      se_property                                       AS event_property,
      platform                                          AS platform,
      gsc_pseudonymized_user_id,
      {{ clean_url('page_urlpath') }}                   AS clean_url_path,
      page_urlhost                                      AS page_url_host,
      app_id,
      domain_sessionid                                  AS session_id,
      lc_targeturl                                      AS link_click_target_url,
      sf_formid                                         AS submit_form_id,
      cf_formid                                         AS change_form_id,
      cf_type                                           AS change_form_type,
      cf_elementid                                      AS change_form_element_id,
      ff_elementid                                      AS focus_form_element_id,
      ff_nodename                                       AS focus_form_node_name,
      br_family                                         AS browser_name,
      br_name                                           AS browser_major_version,
      br_version                                        AS browser_minor_version,
      br_lang                                           AS browser_language,
      os_name,
      os_timezone,
      gsc_environment                                   AS environment
    FROM events
    WHERE event = 'unstruct'
    AND derived_tstamp >= DATEADD(MONTH, -25, CURRENT_DATE)

    {% if is_incremental() %}

    AND derived_tstamp > (SELECT max(behavior_at) FROM {{ this }})

    {% endif %}
)

, unstruct_event_with_dims AS (

    SELECT

      -- Primary Key
      {{ dbt_utils.surrogate_key(['event_id','behavior_at']) }} AS fct_behavior_unstructured_sk,

      -- Natural Key
      unstruct_event.app_id,
      unstruct_event.event_id,
      unstruct_event.session_id,

      -- Surrogate Keys
      {{ dbt_utils.surrogate_key(['event', 'event_name', 'platform', 'environment', 'event_category', 'event_action', 'event_label', 'event_property']) }} AS dim_behavior_event_sk,
      dim_page.dim_behavior_website_page_sk,
      {{ dbt_utils.surrogate_key(['browser_name', 'browser_major_version', 'browser_minor_version', 'browser_language']) }}                                AS dim_behavior_browser_sk,
      {{ dbt_utils.surrogate_key(['os_name', 'os_timezone']) }}                                                                                            AS dim_behavior_operating_system_sk,

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
    LEFT JOIN dim_page 
      ON unstruct_event.clean_url_path = dim_page.clean_url_path 
        AND unstruct_event.page_url_host = dim_page.page_url_host
        AND unstruct_event.app_id = dim_page.app_id
      
)

{{ dbt_audit(
    cte_ref="unstruct_event_with_dims",
    created_by="@chrissharp",
    updated_by="@chrissharp",
    created_date="2022-09-27",
    updated_date="2022-09-27"
) }}