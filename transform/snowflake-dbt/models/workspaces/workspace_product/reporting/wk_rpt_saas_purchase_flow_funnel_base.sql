{{ config({
    "materialized": "table"
    })
}}

WITH filtered_snowplow_events AS (

  SELECT
    derived_tstamp,
    gsc_pseudonymized_user_id,
    event_category,
    event_action,
    event_label,
    event_property,
    app_id
  FROM {{ ref('snowplow_structured_events_400') }}
  WHERE derived_tstamp > '2022-03-07'
    AND app_id IN (
      'gitlab',
      'gitlab_customers'
    )
    AND event_category IN (
      'subscriptions:new',
      'SubscriptionsController'
    )
    AND event_action IN (
      'render',
      'click_button'
    )
    AND event_label IN (
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
    cte_ref="filtered_snowplow_events",
    created_by="@mdrussell",
    updated_by="@mdrussell",
    created_date="2022-01-14",
    updated_date="2022-08-26"
) }}