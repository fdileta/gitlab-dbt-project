{{ config({
    "materialized": "incremental",
    "unique_key": "event_surrogate_key"
    })
}}

{%- set event_ctes = ["audit_events_viewed",
                      "code_analytics_viewed",
                      "cycle_analytics_viewed",
                      "insights_viewed", 
                      "group_analytics_viewed",
                      "group_created",
                      "productivity_analytics_viewed",
                      "user_authenticated"
                      ]
-%}

WITH snowplow_page_views AS (

  SELECT
    user_snowplow_domain_id,
    user_custom_id,
    page_view_start,
    page_url_path,
    page_view_id,
    referer_url_path
  FROM {{ ref('snowplow_page_views_all')}}
  WHERE TRUE
    AND app_id = 'gitlab'
  {% if is_incremental() %}
    AND page_view_start >= (SELECT MAX(event_date) FROM {{this}})
  {% endif %}

)

, audit_events_viewed AS (

  SELECT
    user_snowplow_domain_id,
    user_custom_id,
    TO_DATE(page_view_start)    AS event_date,
    page_url_path,
    'audit_events_viewed'       AS event_type,
    page_view_id,
    {{ dbt_utils.surrogate_key('page_view_id', 'event_type') }}
                                AS event_surrogate_key
  FROM snowplow_page_views
  WHERE page_url_path REGEXP '(\/([0-9A-Za-z_.-])*){1,}\/audit_events'

)

, code_analytics_viewed AS (

  SELECT
    user_snowplow_domain_id,
    user_custom_id,
    TO_DATE(page_view_start)    AS event_date,
    page_url_path,
    'code_analytics_viewed'     AS event_type,
    page_view_id,
    {{ dbt_utils.surrogate_key('page_view_id', 'event_type') }}
                                AS event_surrogate_key
  FROM snowplow_page_views
  WHERE page_url_path REGEXP '(\/([0-9A-Za-z_.-])*){1,}\/code_analytics'

)

, cycle_analytics_viewed AS (

  SELECT
    user_snowplow_domain_id,
    user_custom_id,
    TO_DATE(page_view_start)   AS event_date,
    page_url_path,
    'cycle_analytics_viewed'   AS event_type,
    page_view_id,
    {{ dbt_utils.surrogate_key('page_view_id', 'event_type') }}
                               AS event_surrogate_key
  FROM snowplow_page_views
  WHERE page_url_path REGEXP '(\/([0-9A-Za-z_.-])*){2,}\/cycle_analytics'

)

, group_analytics_viewed AS (

  SELECT
    user_snowplow_domain_id,
    user_custom_id,
    TO_DATE(page_view_start)   AS event_date,
    page_url_path,
    'group_analytics_viewed'   AS event_type,
    page_view_id,
    {{ dbt_utils.surrogate_key('page_view_id', 'event_type') }}
                               AS event_surrogate_key
  FROM snowplow_page_views
  WHERE page_url_path REGEXP '(\/([0-9A-Za-z_.-])*){1,}\/analytics'

)

, group_created AS (

  SELECT
    user_snowplow_domain_id,
    user_custom_id,
    TO_DATE(page_view_start)   AS event_date,
    page_url_path,
    'group_created'            AS event_type,
    page_view_id,
    {{ dbt_utils.surrogate_key('page_view_id', 'event_type') }}
                               AS event_surrogate_key
  FROM snowplow_page_views
  WHERE page_url_path REGEXP '\/groups\/new'

)

, insights_viewed AS (

  SELECT
    user_snowplow_domain_id,
    user_custom_id,
    TO_DATE(page_view_start)   AS event_date,
    page_url_path,
    'insights_viewed'          AS event_type,
    page_view_id,
    {{ dbt_utils.surrogate_key('page_view_id', 'event_type') }}
                               AS event_surrogate_key
  FROM snowplow_page_views
  WHERE page_url_path REGEXP '(\/([0-9A-Za-z_.-])*){1,}\/insights'

)

, productivity_analytics_viewed AS (

  SELECT
    user_snowplow_domain_id,
    user_custom_id,
    TO_DATE(page_view_start)          AS event_date,
    page_url_path,
    'productivity_analytics_viewed'   AS event_type,
    page_view_id,
    {{ dbt_utils.surrogate_key('page_view_id', 'event_type') }}
                                AS event_surrogate_key
  FROM snowplow_page_views
  WHERE page_url_path REGEXP '(\/([0-9A-Za-z_.-])*){1,}\/productivity_analytics'

)

  /*
    Looks at referrer_url in addition to page_url.
    Regex matches for successful sign-in authentications,
    meaning /sign_in redirects to a real GitLab page.
  */
, user_authenticated AS (

  SELECT
    user_snowplow_domain_id,
    user_custom_id,
    TO_DATE(page_view_start)   AS event_date,
    page_url_path,
    'user_authenticated'       AS event_type,
    page_view_id,
    {{ dbt_utils.surrogate_key('page_view_id', 'event_type') }}
                               AS event_surrogate_key
  FROM snowplow_page_views
  WHERE referer_url_path REGEXP '\/users\/sign_in'
    AND page_url_path NOT REGEXP '\/users\/sign_in'

)

, unioned AS (
  {% for event_cte in event_ctes %}

    SELECT *
    FROM {{ event_cte }}

    {%- if not loop.last %}
      UNION
    {%- endif %}

  {% endfor -%}

)

SELECT *
FROM unioned
