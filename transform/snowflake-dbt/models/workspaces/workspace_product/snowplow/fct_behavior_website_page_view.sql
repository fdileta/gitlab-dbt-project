{{ config(
        materialized = "incremental",
        unique_key = "fct_behavior_website_page_view_sk"
) }}

{{ 
    simple_cte([
    ('page_views', 'prep_snowplow_page_views_all'),
    ('dim_behavior_website_page', 'dim_behavior_website_page')
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
      total_time_in_ms                                                              AS page_load_time_in_ms,
      page_view_index,
      page_view_in_session_index,
      referer_url_path,
      page_url,
      referer_url,
      page_url_scheme
    FROM page_views

    {% if is_incremental() %}

    WHERE max_tstamp > (SELECT max(page_view_end_at) FROM {{ this }})

    {% endif %}

), page_views_w_dim AS (

    SELECT
      -- Primary Key
      {{ dbt_utils.surrogate_key(['event_id','page_view_end_at']) }}                AS fct_behavior_website_page_view_sk,

      -- Foreign Keys
      dim_behavior_website_page.dim_behavior_website_page_sk,
      referrer_website_page.dim_behavior_website_page_sk AS dim_behavior_website_page_sk_referer,
      page_views_w_clean_url.gsc_project_id AS dim_namespace_id,
      page_views_w_clean_url.gsc_project_id AS dim_project_id,

      --Time Attributes
      page_views_w_clean_url.page_view_start_at,
      page_views_w_clean_url.page_view_end_at,
      page_views_w_clean_url.page_view_start_at                                     AS behavior_at,

      -- Natural Keys
      page_views_w_clean_url.session_id,
      page_views_w_clean_url.event_id,
      page_views_w_clean_url.user_snowplow_domain_id,

      -- GitLab Standard Context
      page_views_w_clean_url.gsc_environment,
      page_views_w_clean_url.gsc_extra,
      page_views_w_clean_url.gsc_google_analytics_client_id,
      page_views_w_clean_url.gsc_plan,
      page_views_w_clean_url.gsc_pseudonymized_user_id,
      page_views_w_clean_url.gsc_source,

      -- Attributes
      dim_behavior_website_page.page_url_path,
      referrer_website_page.page_url_path AS referer_url_path,
      page_views_w_clean_url.event_name,
      NULL                                                                          AS sf_formid,
      page_views_w_clean_url.engaged_seconds,
      page_views_w_clean_url.page_load_time_in_ms,
      page_views_w_clean_url.page_view_index,
      page_views_w_clean_url.page_view_in_session_index
    FROM page_views_w_clean_url
    LEFT JOIN dim_behavior_website_page 
      ON page_views_w_clean_url.page_url = dim_behavior_website_page.page_url
        AND page_views_w_clean_url.app_id = dim_behavior_website_page.app_id
        AND page_views_w_clean_url.page_url_scheme = dim_behavior_website_page.page_url_scheme
    LEFT JOIN dim_behavior_website_page AS referrer_website_page
      ON page_views_w_clean_url.referer_url = referrer_website_page.page_url
        AND page_views_w_clean_url.app_id = referrer_website_page.app_id
        AND page_views_w_clean_url.page_url_scheme = referrer_website_page.page_url_scheme

)

{{ dbt_audit(
    cte_ref="page_views_w_dim",
    created_by="@chrissharp",
    updated_by="@michellecooper",
    created_date="2022-07-22",
    updated_date="2022-11-21"
) }}
