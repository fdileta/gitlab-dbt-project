{{config({
        "materialized":"table"
  })

WITH browser_information AS (

  SELECT DISTINCT
    -- surrogate key
    {{ dbt_utils.surrogate_key([
        'prep_snowplow_structured_event_all_source.browser_name',
        'prep_snowplow_structured_event_all_source.browser_major_version',
        'prep_snowplow_structured_event_all_source.browser_minor_version',
        'prep_snowplow_structured_event_all_source.browser_language'
        ]) }} AS dim_behavior_browser_sk,

    -- natural key
    prep_snowplow_structured_event_all_source.browser_name,
    prep_snowplow_structured_event_all_source.browser_major_version,
    prep_snowplow_structured_event_all_source.browser_minor_version,

    -- attributes
    prep_snowplow_structured_event_all_source.browser_engine,
    prep_snowplow_structured_event_all_source.browser_language
  FROM {{ ref('prep_snowplow_structured_event_all_source') }}

)

{{ dbt_audit(
    cte_ref="browser_information",
    created_by="@michellecooper",
    updated_by="@michellecooper",
    created_date="2022-09-20",
    updated_date="2022-09-20"
) }}
