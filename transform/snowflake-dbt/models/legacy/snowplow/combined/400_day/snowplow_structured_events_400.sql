{{config({
    "materialized":"incremental",
    "unique_key":"event_id",
    "post-hook": ["DELETE FROM {{ this }} WHERE derived_tstamp::DATE < DATEADD('DAY', -400, CURRENT_DATE())"]
  })
}}

{% if is_incremental() %}

{% set days_to_look_back = 30 %}

WITH filtered_table AS (

  SELECT *
  FROM {{ this }}
  WHERE derived_tstamp::DATE >= DATEADD(DAY, -{{days_to_look_back}}, CURRENT_DATE::DATE) 

)

{% endif %}

SELECT *
FROM {{ ref('snowplow_structured_events_all') }}

{% if is_incremental() %}
WHERE derived_tstamp::DATE >= DATEADD(DAY, -{{days_to_look_back}}, CURRENT_DATE::DATE)
  AND event_id NOT IN (SELECT event_id FROM filtered_table)

{% else %}

WHERE derived_tstamp::DATE >= DATEADD(DAY, -400, CURRENT_DATE::DATE)
{% endif %}
