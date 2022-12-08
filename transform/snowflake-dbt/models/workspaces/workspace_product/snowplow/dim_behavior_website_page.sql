{{ config(
        materialized = "incremental",
        unique_key = "dim_behavior_website_page_sk"
) }}

{{ simple_cte([
    ('events', 'prep_snowplow_unnested_events_all')
    ])
}}

, page AS (

    SELECT
      app_id,
      page_url_host,
      clean_url_path,
      SPLIT_PART(clean_url_path, '/' ,1)                                            AS page_group,
      SPLIT_PART(clean_url_path, '/' ,2)                                            AS page_type,
      SPLIT_PART(clean_url_path, '/' ,3)                                            AS page_sub_type,
      referrer_medium,
      min(behavior_at)                                                              AS min_event_timestamp,
      max(behavior_at)                                                              AS max_event_timestamp
    FROM events
    WHERE event IN ('struct', 'page_view', 'unstruct')
    AND page_url_path IS NOT NULL

    {% if is_incremental() %}

    AND behavior_at > (SELECT max(max_event_timestamp) FROM {{ this }})

    {% endif %}

    {{ dbt_utils.group_by(n=7) }}

), dim_with_sk AS (

    SELECT DISTINCT
      -- Surrogate Key
      {{ dbt_utils.surrogate_key(['app_id','page_url_host','clean_url_path']) }}    AS dim_behavior_website_page_sk,

      -- Natural Keys
      app_id,
      page_url_host,
      clean_url_path,

      -- Attributes
      page_group,
      page_type,
      page_sub_type,
      referrer_medium,
      min_event_timestamp,
      max_event_timestamp
    FROM page

)

{{ dbt_audit(
    cte_ref="dim_with_sk",
    created_by="@chrissharp",
    updated_by="@chrissharp",
    created_date="2022-07-22",
    updated_date="2022-12-01"
) }}
