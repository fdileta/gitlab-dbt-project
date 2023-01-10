{{ config({
    "alias": "snowplow_gitlab_good_events_source"
}) }}

WITH source as (

  SELECT
    {{ dbt_utils.star(from=source('gitlab_snowplow', 'events'), except=['geo_zipcode', 'geo_latitude', 'geo_longitude']) }}
  FROM {{ source('gitlab_snowplow', 'events') }} 

)

SELECT *,
  NULL AS geo_zipcode,
  NULL AS geo_latitude,
  NULL AS geo_longitude
FROM source
