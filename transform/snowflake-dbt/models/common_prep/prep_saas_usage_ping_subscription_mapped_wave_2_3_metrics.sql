{{ config(
    tags=["mnpi_exception"]
) }}

{{
  config({
    "materialized": "table"
  })
}}

{% set gainsight_wave_metrics = dbt_utils.get_column_values(table=ref ('gainsight_wave_2_3_metrics'), column='metric_name', max_records=1000, default=['']) %}

{{ simple_cte([
    ('prep_saas_usage_ping_namespace','prep_saas_usage_ping_namespace'),
    ('dim_date','dim_date'),
    ('bdg_namespace_subscription','bdg_namespace_order_subscription_monthly'),
    ('gainsight_wave_2_3_metrics','gainsight_wave_2_3_metrics'),
    ('instance_types', 'dim_host_instance_type')
]) }}

, instance_types_ordering AS (
    SELECT
      *,
      CASE
        WHEN instance_type = 'Production' THEN 1
        WHEN instance_type = 'Non-Production' THEN 2
        WHEN instance_type = 'Unknown' THEN 3
        ELSE 4
      END AS ordering_field
    FROM instance_types
)

, joined AS (

    SELECT 
      prep_saas_usage_ping_namespace.dim_namespace_id,
      prep_saas_usage_ping_namespace.ping_date,
      prep_saas_usage_ping_namespace.ping_name,
      prep_saas_usage_ping_namespace.counter_value,
      dim_date.first_day_of_month                           AS reporting_month, 
      bdg_namespace_subscription.dim_subscription_id,
      instance_types_ordering.instance_type
    FROM prep_saas_usage_ping_namespace
    LEFT JOIN instance_types_ordering
      ON prep_saas_usage_ping_namespace.dim_namespace_id = instance_types_ordering.namespace_id
    INNER JOIN dim_date
      ON prep_saas_usage_ping_namespace.ping_date = dim_date.date_day
    INNER JOIN bdg_namespace_subscription
      ON prep_saas_usage_ping_namespace.dim_namespace_id = bdg_namespace_subscription.dim_namespace_id
      AND dim_date.first_day_of_month = bdg_namespace_subscription.snapshot_month
      AND namespace_order_subscription_match_status = 'Paid All Matching'
    INNER JOIN gainsight_wave_2_3_metrics
      ON prep_saas_usage_ping_namespace.ping_name = gainsight_wave_2_3_metrics.metric_name
    QUALIFY ROW_NUMBER() OVER (
      PARTITION BY 
        dim_date.first_day_of_month,
        bdg_namespace_subscription.dim_subscription_id,
        prep_saas_usage_ping_namespace.dim_namespace_id,
        prep_saas_usage_ping_namespace.ping_name
        ORDER BY 
          prep_saas_usage_ping_namespace.ping_date DESC,
          instance_types_ordering.ordering_field ASC --prioritizing Production instances
    ) = 1

), pivoted AS (

    SELECT
      dim_namespace_id,
      dim_subscription_id,
      reporting_month,
      instance_type,
      MAX(ping_date)                                        AS ping_date,
      {{ dbt_utils.pivot('ping_name', gainsight_wave_metrics, then_value='counter_value') }}
    FROM joined
    {{ dbt_utils.group_by(n=4)}}

)

{{ dbt_audit(
    cte_ref="pivoted",
    created_by="@mpeychet_",
    updated_by="@mdrussell",
    created_date="2021-03-22",
    updated_date="2022-07-14"
) }}