{{config({
        "materialized":"table"
  })

WITH device_information AS (

  SELECT DISTINCT
    {{ dbt_utils.surrogate_key(['prep_snowplow_structured_event_all_source.os_name', 'prep_snowplow_structured_event_all_source.os_timezone']) }} AS dim_behavior_operating_system_sk,
    prep_snowplow_structured_event_all_source.os,
    prep_snowplow_structured_event_all_source.os_name,
    prep_snowplow_structured_event_all_source.os_manufacturer,
    prep_snowplow_structured_event_all_source.os_timezone,
    prep_snowplow_structured_event_all_source.device_type,
    prep_snowplow_structured_event_all_source.device_is_mobile AS is_device_mobile
  FROM {{ ref('prep_snowplow_structured_event_all_source') }}

)

{{ dbt_audit(
    cte_ref="device_information",
    created_by="@michellecooper",
    updated_by="@michellecooper",
    created_date="2022-09-20",
    updated_date="2022-09-20"
) }}