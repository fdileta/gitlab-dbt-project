{{ config(
        materialized = "incremental",
        unique_key = "behavior_structured_event_pk"
) }}

{{ 
    simple_cte([
    ('fct_behavior_structured_event', 'fct_behavior_structured_event')
    ])
}}

, final AS (

    SELECT 

      -- Primary Key
      fct_behavior_structured_event.behavior_structured_event_pk,

      -- Foreign Keys
      fct_behavior_structured_event.dim_behavior_website_page_sk,
      fct_behavior_structured_event.dim_behavior_browser_sk,
      fct_behavior_structured_event.dim_behavior_operating_system_sk,
      fct_behavior_structured_event.dim_namespace_id,
      fct_behavior_structured_event.dim_project_id,

      -- Time Attributes
      fct_behavior_structured_event.dvce_created_tstamp,
      fct_behavior_structured_event.behavior_at,

      -- Degenerate Dimensions (Event Attributes)
      fct_behavior_structured_event.event_action,
      fct_behavior_structured_event.event_category,
      fct_behavior_structured_event.event_label,
      fct_behavior_structured_event.event_property,
      fct_behavior_structured_event.event_value,
      fct_behavior_structured_event.v_tracker,
      fct_behavior_structured_event.session_index,
      fct_behavior_structured_event.app_id,
      fct_behavior_structured_event.session_id,
      fct_behavior_structured_event.page_url,
      fct_behavior_structured_event.page_url_scheme,
      fct_behavior_structured_event.page_url_host,
      fct_behavior_structured_event.page_url_fragment,
      fct_behavior_structured_event.user_snowplow_domain_id,
      fct_behavior_structured_event.contexts,

      -- Degenerate Dimensions (Gitlab Standard Context Attributes)
      fct_behavior_structured_event.gsc_google_analytics_client_id,
      fct_behavior_structured_event.gsc_pseudonymized_user_id,
      fct_behavior_structured_event.gsc_environment,
      fct_behavior_structured_event.gsc_extra,
      fct_behavior_structured_event.gsc_plan,
      fct_behavior_structured_event.gsc_source
      
    FROM fct_behavior_structured_event
    WHERE event_action IN (
    'g_analytics_valuestream',
    'action_active_users_project_repo',
    'push_package',
    'ci_templates_unique',
    'p_terraform_state_api_unique_users',
    'i_search_paid'
    )
   
    {% if is_incremental() %}

    AND behavior_at > (SELECT MAX(behavior_at) FROM {{this}})

    {% endif %}


)

{{ dbt_audit(
    cte_ref="final",
    created_by="@michellecooper",
    updated_by="@michellecooper",
    created_date="2022-09-01",
    updated_date="2022-10-03"
) }}
