{{ config(
        materialized = "incremental",
        unique_key = "dim_behavior_event_sk"
) }}

{{ simple_cte([
    ('events', 'prep_snowplow_unnested_events_all')
    ])
}}

, event_source AS (

    SELECT
      event,
      event_name,
      platform,
      gsc_environment       AS environment,
      MAX(collector_tstamp) AS max_collector_timestamp
    FROM events
    WHERE true

    {% if is_incremental() %}
    
    AND collector_tstamp > (SELECT MAX(max_collector_timestamp) FROM {{this}})
    
    {% endif %}

    AND domain_sessionid IS NOT NULL
    AND domain_sessionidx IS NOT NULL
    AND domain_userid IS NOT NULL

    {{ dbt_utils.group_by(n=4) }}
)

, final AS (

    SELECT
      -- Surrogate Key
      {{ dbt_utils.surrogate_key(['event','event_name','platform','environment']) }} AS dim_behavior_event_sk,

      -- Natural Keys
      event,
      event_name,
      platform,
      environment,

      --Time Attributes for Incremental Load
      max_collector_timestamp
    FROM event_source
)

{{ dbt_audit(
    cte_ref="final",
    created_by="@chrissharp",
    updated_by="@chrissharp",
    created_date="2022-09-20",
    updated_date="2022-09-20"
) }}
