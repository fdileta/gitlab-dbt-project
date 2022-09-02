{{ 
    simple_cte([
    ('fct_behavior_structured_event', 'fct_behavior_structured_event')
    ])
}}

, final AS (

    SELECT *
    FROM fct_behavior_structured_event
    WHERE event_action IN (
    'g_analytics_valuestream',
    'action_active_users_project_repo',
    'push_package',
    'ci_templates_unique',
    'p_terraform_state_api_unique_users',
    'i_search_paid'
  )


)

{{ dbt_audit(
    cte_ref="final",
    created_by="@michellecooper",
    updated_by="@michellecooper",
    created_date="2022-09-01",
    updated_date="2022-09-01"
) }}
