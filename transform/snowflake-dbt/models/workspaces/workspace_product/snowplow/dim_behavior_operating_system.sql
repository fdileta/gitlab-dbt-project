{{ config(
        materialized = "incremental",
        unique_key = "dim_behavior_operating_system_sk"
    )
}}

WITH device_information AS (

  SELECT
    {{ dbt_utils.surrogate_key(['os_name', 'os_timezone']) }}   AS dim_behavior_operating_system_sk,
    os_family::VARCHAR                                          AS os,
    os_name::VARCHAR                                            AS os_name,
    os_manufacturer::VARCHAR                                    AS os_manufacturer,
    os_timezone::VARCHAR                                        AS os_timezone,
    dvce_type::VARCHAR                                          AS device_type,
    dvce_ismobile::BOOLEAN                                      AS is_device_mobile,
    MAX(collector_tstamp)                                       AS max_collector_timestamp
  FROM {{ ref('prep_snowplow_unnested_events_all') }}
  WHERE true

  {% if is_incremental() %}
    
  AND collector_tstamp > (SELECT MAX(max_collector_timestamp) FROM {{this}})
    
  {% endif %}

  AND domain_sessionid IS NOT NULL
  AND domain_sessionidx IS NOT NULL
  AND domain_userid IS NOT NULL

  {{ dbt_utils.group_by(n=7) }}

)

{{ dbt_audit(
    cte_ref="device_information",
    created_by="@michellecooper",
    updated_by="@chrissharp",
    created_date="2022-09-20",
    updated_date="2022-10-14"
) }}