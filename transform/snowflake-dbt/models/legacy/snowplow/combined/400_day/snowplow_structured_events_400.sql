{{config({
    "materialized":"incremental",
    "unique_key":"event_id",
    "post-hook": ["DELETE FROM {{ this }} WHERE derived_tstamp::DATE < DATEADD('DAY', -400, CURRENT_DATE())"]
  })
}}

-- depends_on: {{ ref('snowplow_structured_events') }}

{% if is_incremental() %}

WITH filtered_table AS (

  SELECT *
  FROM {{ this }}
  WHERE DATE_TRUNC(MONTH, derived_tstamp::DATE) >= DATEADD(MONTH, -10, DATE_TRUNC(MONTH,CURRENT_DATE)) 

)

{% endif %}

SELECT *
FROM {{ ref('snowplow_structured_events_all') }}
WHERE DATE_TRUNC(MONTH, derived_tstamp::DATE) >= DATEADD(MONTH, -400, DATE_TRUNC(MONTH,CURRENT_DATE))

{% if is_incremental() %}
  AND event_id NOT IN (SELECT event_id FROM filtered_table)
{% endif %}
