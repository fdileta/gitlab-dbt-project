{{config(

    materialized='incremental',
    unique_key='behavior_structured_event_pk',
    full_refresh= only_force_full_refresh(),
    on_schema_change='sync_all_columns',
    post_hook=["{{ rolling_window_delete('behavior_at','month',25) }}"],
    cluster_by=['behavior_at::DATE']
  )

}}

WITH structured_event_renamed AS (
    SELECT
    
      event_id,
      tracker_version,
      dim_behavior_event_sk,
      contexts,
      dvce_created_tstamp,
      behavior_at,
      user_snowplow_domain_id,
      session_id,
      session_index,
      platform,
      page_url,
      page_url_scheme,
      page_url_host,
      page_url_path,
      clean_url_path,
      page_url_fragment,
      app_id,
      dim_behavior_browser_sk,
      dim_behavior_operating_system_sk,
      gsc_environment,
      gsc_extra,
      gsc_namespace_id,
      gsc_plan,
      gsc_google_analytics_client_id,
      gsc_project_id,
      gsc_pseudonymized_user_id,
      gsc_source

    FROM {{ ref('prep_snowplow_unnested_events_all') }}
    WHERE event = 'struct'
    {% if is_incremental() %}

      AND behavior_at > (SELECT MAX({{ var('incremental_backfill_date', 'behavior_at') }}) FROM {{ this }})
      AND behavior_at <= (SELECT DATEADD(month, 1,  MAX({{ var('incremental_backfill_date', 'behavior_at') }}) )  FROM {{ this }})


    {% endif %}

), dim_behavior_website_page AS (

    SELECT 
      dim_behavior_website_page.dim_behavior_website_page_sk,
      dim_behavior_website_page.clean_url_path,
      dim_behavior_website_page.page_url_host,
      dim_behavior_website_page.app_id
    FROM {{ ref('dim_behavior_website_page') }}

), structured_events_w_dim AS (

    SELECT

      -- Primary Key
      structured_event_renamed.event_id                                                                                                                        AS behavior_structured_event_pk,

      -- Foreign Keys
      dim_behavior_website_page.dim_behavior_website_page_sk,
      structured_event_renamed.dim_behavior_browser_sk,
      structured_event_renamed.dim_behavior_operating_system_sk,
      structured_event_renamed.gsc_namespace_id                                                                                                                AS dim_namespace_id,
      structured_event_renamed.gsc_project_id                                                                                                                  AS dim_project_id,
      structured_event_renamed.dim_behavior_event_sk,

      -- Time Attributes
      structured_event_renamed.dvce_created_tstamp,
      structured_event_renamed.behavior_at,

      -- Degenerate Dimensions (Event Attributes)
      structured_event_renamed.tracker_version,
      structured_event_renamed.session_index,
      structured_event_renamed.app_id,
      structured_event_renamed.session_id,
      structured_event_renamed.user_snowplow_domain_id,
      structured_event_renamed.contexts,
      structured_event_renamed.page_url,
      structured_event_renamed.page_url_path,
      structured_event_renamed.page_url_scheme,
      structured_event_renamed.page_url_host,
      structured_event_renamed.page_url_fragment,

      -- Degenerate Dimensions (Gitlab Standard Context Attributes)
      structured_event_renamed.gsc_google_analytics_client_id,
      structured_event_renamed.gsc_pseudonymized_user_id,
      structured_event_renamed.gsc_environment,
      structured_event_renamed.gsc_extra,
      structured_event_renamed.gsc_plan,
      structured_event_renamed.gsc_source

    FROM structured_event_renamed
    LEFT JOIN dim_behavior_website_page
      ON structured_event_renamed.clean_url_path = dim_behavior_website_page.clean_url_path
        AND structured_event_renamed.page_url_host = dim_behavior_website_page.page_url_host
        AND structured_event_renamed.app_id = dim_behavior_website_page.app_id

)

{{ dbt_audit(
    cte_ref="structured_events_w_dim",
    created_by="@michellecooper",
    updated_by="@chrissharp",
    created_date="2022-09-01",
    updated_date="2022-12-01"
) }}
