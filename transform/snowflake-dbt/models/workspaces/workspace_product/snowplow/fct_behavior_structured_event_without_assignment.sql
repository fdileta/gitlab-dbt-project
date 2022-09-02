{{ 
    simple_cte([
    ('fct_behavior_structured_event', 'fct_behavior_structured_event')
    ])
}}

, final AS (

    SELECT *
    FROM fct_behavior_structured_event
    WHERE event_action != 'assignment'


)

{{ dbt_audit(
    cte_ref="final",
    created_by="@michellecooper",
    updated_by="@michellecooper",
    created_date="2022-09-01",
    updated_date="2022-09-01"
) }}