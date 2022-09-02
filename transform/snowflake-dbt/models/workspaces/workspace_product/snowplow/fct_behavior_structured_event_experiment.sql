{{ 
    simple_cte([
    ('fct_behavior_structured_event', 'fct_behavior_structured_event'),
    ('snowplow_gitlab_events_experiment_contexts', 'snowplow_gitlab_events_experiment_contexts')

    ])
}}

, final AS (

    SELECT
      fct_behavior_structured_event.*,
      snowplow_gitlab_events_experiment_contexts.experiment_name,
      snowplow_gitlab_events_experiment_contexts.experiment_variant,
      snowplow_gitlab_events_experiment_contexts.context_key
    FROM fct_behavior_structured_event
    INNER JOIN snowplow_gitlab_events_experiment_contexts
      ON fct_behavior_structured_event.event_id = snowplow_gitlab_events_experiment_contexts.event_id

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@michellecooper",
    updated_by="@michellecooper",
    created_date="2022-09-01",
    updated_date="2022-09-01"
) }}