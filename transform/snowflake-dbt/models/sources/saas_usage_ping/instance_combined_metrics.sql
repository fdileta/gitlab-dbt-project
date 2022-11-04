WITH source AS (

  SELECT
    *
  FROM {{ source('saas_usage_ping', 'instance_combined_metrics')}}

)

, cleaned AS (

  SELECT
    PARSE_JSON(query_map) AS query_map,
    PARSE_JSON(run_results) AS run_results,
    ping_date::DATE AS ping_date,
    DATEADD('s', _uploaded_at, '1970-01-01')::TIMESTAMP  AS _uploaded_at,
    run_id::VARCHAR AS run_id,
    recorded_at::TIMESTAMP AS recorded_at,
    -- version::VARCHAR,
    edition::VARCHAR AS edition,
    recording_ce_finished_at::TIMESTAMP AS recording_ce_finished_at,
    recording_ee_finished_at::TIMESTAMP AS recording_ee_finished_at,
    uuid::VARCHAR AS uuid,
    source::VARCHAR AS ping_source
  FROM source

)

SELECT 
  *
FROM cleaned