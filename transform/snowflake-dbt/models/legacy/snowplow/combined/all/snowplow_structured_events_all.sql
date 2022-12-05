{{config({
    "materialized":"incremental",
    "unique_key":"event_id",
    "full_refresh":false,
    "on_schema_change":"sync_all_columns",
    "post-hook": ["DELETE FROM {{ this }} WHERE DATE_TRUNC(MONTH, derived_tstamp::DATE) < DATEADD(MONTH, -24, DATE_TRUNC(MONTH,CURRENT_DATE))"]
  })
}}

-- depends_on: {{ ref('snowplow_structured_events') }}

{% if is_incremental() %}

  {% set day_limit = 30 %}

{% else %}

  {% set day_limit = 730 %}
  
{% endif %}

WITH 

{% if is_incremental() %}

filtered_table AS (

  SELECT event_id
  FROM {{ this }}
  WHERE derived_tstamp::DATE >= DATEADD(DAY, -{{day_limit}}, CURRENT_DATE::DATE)

),

{% endif %}


unioned_table AS (

{{ schema_union_all('snowplow_', 'snowplow_structured_events', database_name=env_var('SNOWFLAKE_PREP_DATABASE'), day_limit = day_limit) }}

)

SELECT *
FROM unioned_table

{% if is_incremental() %}
WHERE derived_tstamp::DATE >= DATEADD(DAY, -{{day_limit}}, CURRENT_DATE::DATE)
  AND event_id NOT IN (SELECT event_id FROM filtered_table)

{% else %}
--filter to the last rolling 24 months of data for query performance tuning
WHERE DATE_TRUNC(MONTH, derived_tstamp::DATE) >= DATEADD(MONTH, -24, DATE_TRUNC(MONTH,CURRENT_DATE)) 

{% endif %}
