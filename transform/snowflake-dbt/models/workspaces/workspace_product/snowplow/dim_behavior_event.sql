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
      se_category           AS event_category,
      se_action             AS event_action,
      se_label              AS event_label,
      se_property           AS event_property,
      MAX(derived_tstamp)   AS max_timestamp
    FROM events

    {% if is_incremental() %}
    
    WHERE derived_tstamp > (SELECT MAX(max_timestamp) FROM {{this}})
    
    {% endif %}

    {{ dbt_utils.group_by(n=8) }}
)

, final AS (

    SELECT
      -- Surrogate Key
      {{ dbt_utils.surrogate_key(['event', 'event_name', 'platform', 'environment', 'event_category', 'event_action', 'event_label', 'event_property']) }} AS dim_behavior_event_sk,

      -- Natural Keys
      event,
      event_name,
      platform,
      environment,
      event_category,
      event_action,
      event_label,
      event_property,

      --Time Attributes for Incremental Load
      max_timestamp
    FROM event_source
)

{{ dbt_audit(
    cte_ref="final",
    created_by="@chrissharp",
    updated_by="@chrissharp",
    created_date="2022-09-20",
    updated_date="2022-09-20"
) }}
