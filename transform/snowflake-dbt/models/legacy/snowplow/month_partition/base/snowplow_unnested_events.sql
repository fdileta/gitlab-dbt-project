{{config({
    "unique_key":"event_id",
    cluster_by=['event', 'derived_tstamp::DATE']
  })
}}

WITH gitlab as (

    SELECT *
    FROM {{ ref('snowplow_gitlab_events') }}

), events_to_ignore as (

    SELECT event_id
    FROM {{ ref('snowplow_duplicate_events') }}

)

SELECT *
FROM gitlab
WHERE event_id NOT IN (SELECT event_id FROM events_to_ignore)
