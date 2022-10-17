{{ config(
    materialized = "incremental",
    unique_key = "dim_behavior_browser_sk"
    )
}}
WITH browser_information AS (

  SELECT DISTINCT
    -- surrogate key
    {{ dbt_utils.surrogate_key([
        'br_family',
        'br_name',
        'br_version',
        'br_lang'
        ]) }}                   AS dim_behavior_browser_sk,

    -- natural key
    br_family::VARCHAR          AS browser_name,
    br_name::VARCHAR            AS browser_major_version,
    br_version::VARCHAR         AS browser_minor_version,

    -- attributes
    br_renderengine::VARCHAR    AS browser_engine,
    br_lang::VARCHAR            AS browser_language,
    MAX(derived_tstamp)       AS max_timestamp
  FROM {{ ref('prep_snowplow_unnested_events_all') }}
  WHERE true

  {% if is_incremental() %}
    
  AND derived_tstamp > (SELECT MAX(max_timestamp) FROM {{this}})
    
  {% endif %}

  {{ dbt_utils.group_by(n=6) }}


)

{{ dbt_audit(
    cte_ref="browser_information",
    created_by="@michellecooper",
    updated_by="@chrissharp",
    created_date="2022-09-20",
    updated_date="2022-10-14"
) }}
