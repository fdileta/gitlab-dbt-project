{{
  config({
    "materialized": "table",
    "tags": ["mnpi_exception"]
  })
}}

WITH subscriptions AS (

  SELECT
    *
  FROM {{ ref('dim_subscription') }}

),

months AS (

    {{ dbt_utils.date_spine(
        datepart="month",
        start_date="cast('2017-01-01' as date)",
        end_date="DATEADD('month', 1,DATE_TRUNC('month', CURRENT_DATE()))"
       )
    }}

),

joined AS (

  SELECT
    months.date_month,
    subscriptions.term_start_month,
    subscriptions.term_end_month,
    subscriptions.dim_subscription_id,
    subscriptions.dim_subscription_id_original,
    subscriptions.namespace_id AS dim_namespace_id,
    subscriptions.subscription_version,
    subscriptions.subscription_created_date
  FROM subscriptions
  INNER JOIN months
    ON (months.date_month >= subscriptions.term_start_month
        AND months.date_month < subscriptions.term_end_month)

),

final AS (

  SELECT
    date_month,
    dim_subscription_id,
    dim_subscription_id_original,
    dim_namespace_id,
    subscription_version
  FROM joined
  --picking most recent subscription version
  QUALIFY
    ROW_NUMBER() OVER(
      PARTITION BY
        dim_namespace_id, date_month
      ORDER BY subscription_created_date DESC, subscription_version DESC
    ) = 1

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@mdrussell",
    updated_by="@mdrussell",
    created_date="2022-10-28",
    updated_date="2022-10-28"
) }}
