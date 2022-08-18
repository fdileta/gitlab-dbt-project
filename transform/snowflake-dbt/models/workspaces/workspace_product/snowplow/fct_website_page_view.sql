{{ config(
        materialized = "incremental",
        unique_key = "website_page_view_pk"
) }}

{{ 
    simple_cte([
    ('page_views', 'snowplow_page_views_all'),
    ('unstruct_events', 'snowplow_unstructured_events_all'),
    ('dim_website_page', 'dim_website_page')
    ])
}}

, page_views_w_clean_url AS (

    SELECT
      {{ clean_url('page_url_path') }}                                              AS clean_url_path,
      page_url_path,
      app_id,
      page_url_host,
      REGEXP_SUBSTR(page_url_path, 'namespace(\\d+)', 1, 1, 'e', 1)                 AS dim_namespace_id,
      REGEXP_SUBSTR(page_url_path, 'project(\\d+)', 1, 1, 'e', 1)                   AS dim_project_id,
      session_id,
      user_snowplow_domain_id,
      page_view_id                                                                  AS event_id,
      'page_view'                                                                   AS event_name,
      gsc_environment,
      gsc_extra,
      gsc_google_analytics_client_id,
      gsc_namespace_id,
      gsc_plan,
      gsc_project_id,
      gsc_pseudonymized_user_id,
      gsc_source,
      min_tstamp                                                                    AS page_view_start_at,
      max_tstamp                                                                    AS page_view_end_at,
      time_engaged_in_s                                                             AS engaged_seconds,
      total_time_in_ms                                                              AS engaged_milliseconds,
      page_view_index,
      page_view_in_session_index
    FROM page_views

    {% if is_incremental() %}

    WHERE max_tstamp > (SELECT max(page_view_end_at) FROM {{ this }})

    {% endif %}

), page_views_w_dim AS (

    SELECT
      -- Primary Key
      {{ dbt_utils.surrogate_key(['event_id','page_view_end_at']) }}                AS website_page_view_pk,

      -- Foreign Keys
      dim_website_page_sk,
      dim_namespace_id,
      dim_project_id,

      --Time Attributes
      page_view_start_at,
      page_view_end_at,
      page_view_start_at                                                            AS behavior_at,

      -- Natural Keys
      session_id,
      event_id,
      user_snowplow_domain_id,

      -- Google Keys
      gsc_environment,
      gsc_extra,
      gsc_google_analytics_client_id,
      gsc_namespace_id,
      gsc_plan,
      gsc_project_id,
      gsc_pseudonymized_user_id,
      gsc_source,

      -- Attributes
      page_url_path,
      event_name,
      NULL                                                                          AS sf_formid,
      engaged_seconds,
      engaged_milliseconds,
      page_view_index,
      page_view_in_session_index
    FROM page_views_w_clean_url
    LEFT JOIN dim_website_page ON page_views_w_clean_url.clean_url_path = dim_website_page.clean_url_path
    AND page_views_w_clean_url.page_url_host = dim_website_page.page_url_host
    AND page_views_w_clean_url.app_id = dim_website_page.app_id

)

, unstruct_w_clean_url AS (

    SELECT
      {{ clean_url('page_url_path') }}                                              AS clean_url_path,
      page_url_path,
      app_id,
      page_url_host,
      REGEXP_SUBSTR(page_url_path, 'namespace(\\d+)', 1, 1, 'e', 1)                 AS dim_namespace_id,
      REGEXP_SUBSTR(page_url_path, 'project(\\d+)', 1, 1, 'e', 1)                   AS dim_project_id,
      session_id,
      event_id,
      user_snowplow_domain_id,
      event_name,
      gsc_environment,
      gsc_extra,
      gsc_google_analytics_client_id,
      gsc_namespace_id,
      gsc_plan,
      gsc_project_id,
      gsc_pseudonymized_user_id,
      sf_formid,
      NULL                                                                          AS gsc_source,
      NULL                                                                          AS page_view_start_at,
      NULL                                                                          AS page_view_end_at,
      NULL                                                                          AS engaged_seconds,
      NULL                                                                          AS engaged_milliseconds,
      derived_tstamp                                                                AS behavior_at,
      NULL                                                                          AS page_view_index,
      NULL                                                                          AS page_view_in_session_index
    FROM unstruct_events

    {% if is_incremental() %}

    WHERE derived_tstamp > (SELECT max(behavior_at) FROM {{ this }})

    {% endif %}

)

, unstruct_w_dim AS (

    SELECT
      -- Primary Key
      {{ dbt_utils.surrogate_key(['event_id','behavior_at']) }}                     AS website_page_view_pk,

      -- Foreign Keys
      dim_website_page_sk,
      dim_namespace_id,
      dim_project_id,

      --Time Attributes
      page_view_start_at,
      page_view_end_at,
      behavior_at,

      -- Natural Keys
      session_id,
      event_id,
      user_snowplow_domain_id,

      -- Google Keys
      gsc_environment,
      gsc_extra,
      gsc_google_analytics_client_id,
      gsc_namespace_id,
      gsc_plan,
      gsc_project_id,
      gsc_pseudonymized_user_id,
      gsc_source,

      -- Attributes
      page_url_path,
      event_name,
      sf_formid,
      engaged_seconds,
      engaged_milliseconds,
      page_view_index,
      page_view_in_session_index
    FROM unstruct_w_clean_url
    LEFT JOIN dim_website_page ON unstruct_w_clean_url.clean_url_path = dim_website_page.clean_url_path
    AND unstruct_w_clean_url.page_url_host = dim_website_page.page_url_host
    AND unstruct_w_clean_url.app_id = dim_website_page.app_id

)

, unioned AS (
    SELECT *
    FROM page_views_w_dim

    UNION ALL

    SELECT  *
    FROM unstruct_w_dim
)

{{ dbt_audit(
    cte_ref="unioned",
    created_by="@chrissharp",
    updated_by="@chrissharp",
    created_date="2022-07-22",
    updated_date="2022-08-05"
) }}
