{{ config(
        materialized = "incremental",
        unique_key = "fct_behavior_website_link_click_sk"
) }}

{{ simple_cte([
    ('events', 'prep_snowplow_unstructured_events_all'),
    ('dim_event', 'dim_behavior_website_event'),
    ('dim_page', 'dim_behavior_website_page')
    ])
}}

, link_click AS (

    SELECT
      event_id,
      derived_tstamp                                    AS behavior_at,
      event_name,
      gsc_environment                                   AS environment,
      gsc_pseudonymized_user_id,
      {{ clean_url('page_url_path') }}                  AS clean_url_path,
      page_url_host,
      app_id,
      session_id,
      lc_targeturl                                      AS link_click_target_url
    FROM events
    WHERE event_name = 'link_click'
    AND derived_tstamp >= DATEADD(MONTH, -25, CURRENT_DATE)

    {% if is_incremental() %}

    AND derived_tstamp > (SELECT max(behavior_at) FROM {{ this }})

    {% endif %}
)

, link_click_with_dims AS (

    SELECT

      -- Primary Key
      {{ dbt_utils.surrogate_key(['event_id','behavior_at']) }} AS fct_behavior_website_link_click_sk,

      -- Natural Key
      event_id,

      -- Surrogate Keys
      dim_event.dim_behavior_website_event_sk,
      dim_page.dim_behavior_website_page_sk,

      --Time Attributes
      behavior_at,

      -- Google Key
      gsc_pseudonymized_user_id,

      -- Attributes
      session_id,
      link_click_target_url
    FROM link_click
    INNER JOIN dim_event ON link_click.event_name = dim_event.event_name
    INNER JOIN dim_page ON link_click.clean_url_path = dim_page.clean_url_path 
    AND link_click.page_url_host = dim_page.page_url_host
    AND link_click.app_id = dim_page.app_id
      
)

{{ dbt_audit(
    cte_ref="link_click_with_dims",
    created_by="@chrissharp",
    updated_by="@chrissharp",
    created_date="2022-09-22",
    updated_date="2022-09-22"
) }}