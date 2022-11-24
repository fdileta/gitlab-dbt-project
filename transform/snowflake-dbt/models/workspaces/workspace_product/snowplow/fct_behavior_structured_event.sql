{{config(

    materialized='incremental',
    unique_key='behavior_structured_event_pk',
    full_refresh= only_force_full_refresh(),
    on_schema_change='sync_all_columns',
    post_hook=["{{ rolling_window_delete('behavior_at','month',25) }}"]

  )

}}

WITH structured_event_renamed AS (
    SELECT
    
      event_id::VARCHAR                         AS event_id,
      v_tracker::VARCHAR                        AS v_tracker,
      se_action::VARCHAR                        AS event_action,
      se_category::VARCHAR                      AS event_category,
      se_label::VARCHAR                         AS event_label,
      se_property::VARCHAR                      AS event_property,
      se_value::VARCHAR                         AS event_value,
      event::VARCHAR                            AS event,
      event_name::VARCHAR                       AS event_name,
      TRY_PARSE_JSON(contexts)::VARIANT         AS contexts,
      dvce_created_tstamp::TIMESTAMP            AS dvce_created_tstamp,
      derived_tstamp::TIMESTAMP                 AS derived_tstamp,
      collector_tstamp::TIMESTAMP               AS collector_tstamp,
      user_id::VARCHAR                          AS user_custom_id,
      domain_userid::VARCHAR                    AS user_snowplow_domain_id,
      network_userid::VARCHAR                   AS user_snowplow_crossdomain_id,
      domain_sessionid::VARCHAR                 AS session_id,
      domain_sessionidx::INT                    AS session_index,
      platform::VARCHAR                         AS platform,
      (page_urlhost || page_urlpath)::VARCHAR   AS page_url,
      page_urlscheme::VARCHAR                   AS page_url_scheme,
      page_urlhost::VARCHAR                     AS page_url_host,
      page_urlpath::VARCHAR                     AS page_url_path,
      page_urlfragment::VARCHAR                 AS page_url_fragment,
      mkt_medium::VARCHAR                       AS marketing_medium,
      mkt_source::VARCHAR                       AS marketing_source,
      mkt_term::VARCHAR                         AS marketing_term,
      mkt_content::VARCHAR                      AS marketing_content,
      mkt_campaign::VARCHAR                     AS marketing_campaign,
      app_id::VARCHAR                           AS app_id,
      br_family::VARCHAR                        AS browser_name,
      br_name::VARCHAR                          AS browser_major_version,
      br_version::VARCHAR                       AS browser_minor_version,
      os_family::VARCHAR                        AS os,
      os_name::VARCHAR                          AS os_name,
      br_lang::VARCHAR                          AS browser_language,
      os_manufacturer::VARCHAR                  AS os_manufacturer,
      os_timezone::VARCHAR                      AS os_timezone,
      br_renderengine::VARCHAR                  AS browser_engine,
      dvce_type::VARCHAR                        AS device_type,
      dvce_ismobile::BOOLEAN                    AS device_is_mobile,
      gsc_environment                           AS gsc_environment,
      gsc_extra                                 AS gsc_extra,
      gsc_namespace_id                          AS gsc_namespace_id,
      gsc_plan                                  AS gsc_plan,
      gsc_google_analytics_client_id            AS gsc_google_analytics_client_id,
      gsc_project_id                            AS gsc_project_id,
      gsc_pseudonymized_user_id                 AS gsc_pseudonymized_user_id,
      gsc_source                                AS gsc_source

    FROM {{ ref('prep_snowplow_unnested_events_all') }}
    WHERE event = 'struct'
      AND derived_tstamp > DATEADD(MONTH, -25, CURRENT_DATE)
    {% if is_incremental() %}

      AND derived_tstamp > (SELECT MAX(behavior_at) FROM {{ this }})

    {% endif %}
)

, dim_behavior_website_page AS (

    SELECT 
      dim_behavior_website_page.dim_behavior_website_page_sk,
      dim_behavior_website_page.clean_url_path,
      dim_behavior_website_page.page_url_host,
      dim_behavior_website_page.app_id
    FROM {{ ref('dim_behavior_website_page') }}

), structured_events_w_clean_url AS (

    SELECT 
      structured_event_renamed.*,
      {{ clean_url('structured_event_renamed.page_url_path') }}  AS clean_url_path
    FROM structured_event_renamed

), structured_events_w_dim AS (

    SELECT

      -- Primary Key
      structured_events_w_clean_url.event_id                                                                                                                   AS behavior_structured_event_pk,

      -- Foreign Keys
      dim_behavior_website_page.dim_behavior_website_page_sk,
      {{ dbt_utils.surrogate_key(['browser_name', 'browser_major_version', 'browser_minor_version', 'browser_language']) }}                                    AS dim_behavior_browser_sk,
      {{ dbt_utils.surrogate_key(['os_name', 'os_timezone']) }}                                                                                                AS dim_behavior_operating_system_sk,
      structured_events_w_clean_url.gsc_namespace_id                                                                                                           AS dim_namespace_id,
      structured_events_w_clean_url.gsc_project_id                                                                                                             AS dim_project_id,
      {{ dbt_utils.surrogate_key(['event', 'event_name', 'platform', 'gsc_environment', 'event_category', 'event_action', 'event_label', 'event_property']) }} AS dim_behavior_event_sk,

      -- Time Attributes
      structured_events_w_clean_url.dvce_created_tstamp,
      structured_events_w_clean_url.derived_tstamp                                                                                                             AS behavior_at,

      -- Degenerate Dimensions (Event Attributes)
      structured_events_w_clean_url.v_tracker,
      structured_events_w_clean_url.session_index,
      structured_events_w_clean_url.app_id,
      structured_events_w_clean_url.session_id,
      structured_events_w_clean_url.user_snowplow_domain_id,
      structured_events_w_clean_url.contexts,
      structured_events_w_clean_url.page_url,
      structured_events_w_clean_url.page_url_path,
      structured_events_w_clean_url.page_url_scheme,
      structured_events_w_clean_url.page_url_host,
      structured_events_w_clean_url.page_url_fragment,

      -- Degenerate Dimensions (Gitlab Standard Context Attributes)
      structured_events_w_clean_url.gsc_google_analytics_client_id,
      structured_events_w_clean_url.gsc_pseudonymized_user_id,
      structured_events_w_clean_url.gsc_environment,
      structured_events_w_clean_url.gsc_extra,
      structured_events_w_clean_url.gsc_plan,
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
    updated_by="@chrissharp",
    created_date="2022-09-01",
    updated_date="2022-11-01"
) }}
