{{ config({
    "materialized": "table"
    })
}}

{{ simple_cte([
    ('fct_behavior_structured_event_without_assignment','fct_behavior_structured_event_without_assignment'),
    ('dim_behavior_event','dim_behavior_event')
]) }}

, saas_purchase_flow_funnel AS (

  SELECT
    fct_behavior_structured_event_without_assignment.behavior_at,
    fct_behavior_structured_event_without_assignment.gsc_pseudonymized_user_id,
    dim_behavior_event.event_category,
    dim_behavior_event.event_action,
    dim_behavior_event.event_label,
    dim_behavior_event.event_property,
    fct_behavior_structured_event_without_assignment.app_id
  FROM fct_behavior_structured_event_without_assignment
  LEFT JOIN dim_behavior_event
    ON fct_behavior_structured_event_without_assignment.dim_behavior_event_sk = dim_behavior_event.dim_behavior_event_sk
  WHERE fct_behavior_structured_event_without_assignment.behavior_at > '2022-03-07'
    AND fct_behavior_structured_event_without_assignment.app_id IN (
      'gitlab',
      'gitlab_customers'
    )
    AND dim_behavior_event.event_category IN (
      'subscriptions:new',
      'SubscriptionsController'
    )
    AND dim_behavior_event.event_action IN (
      'render',
      'click_button'
    )
    AND dim_behavior_event.event_label IN (
      'saas_checkout',
      'continue_billing',
      'continue_payment',
      'review_order',
      'confirm_purchase',
      'update_plan_type',
      'update_group',
      'update_seat_count',
      'select_country',
      'state',
      'saas_checkout_postal_code',
      'tax_link',
      'edit'
    )
)

{{ dbt_audit(
    cte_ref="saas_purchase_flow_funnel",
    created_by="@michellecooper",
    updated_by="@michellecooper",
    created_date="2022-11-01",
    updated_date="2022-11-01"
) }}