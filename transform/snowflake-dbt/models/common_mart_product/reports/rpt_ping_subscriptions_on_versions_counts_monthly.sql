{{ config(
    tags=["product", "mnpi_exception"],
    materialized = "table"
) }}

{{ simple_cte([
    ('metric_versions', 'rpt_ping_metric_first_last_versions'),
    ('latest_subscriptions', 'rpt_ping_latest_subscriptions_monthly')
    ])
}},

/*
Attach metrics_path to subscription IF the subscription is on a version with the metric instrumented
*/

latest_subscriptions_by_metric AS (

  SELECT
    latest_subscriptions.ping_created_date_month    AS ping_created_date_month,
    latest_subscriptions.latest_subscription_id     AS latest_subscription_id,
    latest_subscriptions.licensed_user_count        AS licensed_user_count,
    metric_versions.ping_edition                    AS ping_edition,
    metric_versions.metrics_path                    AS metrics_path
  FROM latest_subscriptions
  INNER JOIN metric_versions
    ON latest_subscriptions.major_minor_version_id 
      BETWEEN metric_versions.first_major_minor_version_id_with_counter AND metric_versions.last_major_minor_version_id_with_counter
      AND latest_subscriptions.version_is_prerelease = metric_versions.version_is_prerelease
  {{ dbt_utils.group_by(n=5) }}

),

/*
Aggregate CTE to determine count of arr, subscriptions and licensed users for each month/metric.
*/

agg_subscriptions AS (

  SELECT
    {{ dbt_utils.surrogate_key(['ping_created_date_month', 'metrics_path', 'ping_edition']) }}  AS ping_subscriptions_on_versions_counts_monthly_id,
    ping_created_date_month                                                                     AS ping_created_date_month,
    metrics_path                                                                                AS metrics_path,
    ping_edition                                                                                AS ping_edition,
    COUNT(DISTINCT latest_subscription_id)                                                      AS total_subscription_count,
    SUM(licensed_user_count)                                                                    AS total_licensed_users
  FROM latest_subscriptions_by_metric
  {{ dbt_utils.group_by(n=4) }}

)

{{ dbt_audit(
    cte_ref="agg_subscriptions",
    created_by="@icooper-acp",
    updated_by="@jpeguero",
    created_date="2022-04-20",
    updated_date="2022-12-16"
) }}
