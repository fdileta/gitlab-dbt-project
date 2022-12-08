{{ config(
    tags=["product", "mnpi_exception"]
) }}

{{
  config({
    "materialized": "table"
  })
}}

{% set gainsight_wave_metrics = dbt_utils.get_column_values(table=ref ('health_score_metrics'), column='metric_name', max_records=1000, default=['']) %}

{{ simple_cte([
    ('prep_saas_usage_ping_namespace','prep_saas_usage_ping_namespace'),
    ('dim_date','dim_date'),
    ('bdg_namespace_subscription','bdg_namespace_order_subscription_monthly'),
    ('gainsight_wave_2_3_metrics','health_score_metrics'),
    ('instance_types', 'dim_host_instance_type'),
    ('map_subscription_namespace_month', 'map_latest_subscription_namespace_monthly')
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

, namespace_subscription_monthly_distinct AS (

    SELECT DISTINCT
      dim_namespace_id,
      dim_subscription_id,
      dim_subscription_id_original,
      snapshot_month,
      subscription_version
    FROM bdg_namespace_subscription
    WHERE namespace_order_subscription_match_status = 'Paid All Matching'
)

, joined AS (

    SELECT 
      prep_saas_usage_ping_namespace.dim_namespace_id,
      prep_saas_usage_ping_namespace.ping_date,
      prep_saas_usage_ping_namespace.ping_name,
      prep_saas_usage_ping_namespace.counter_value,
      dim_date.first_day_of_month                           AS reporting_month, 
      COALESCE(
        map_subscription_namespace_month.dim_subscription_id,
        namespace_subscription_monthly_distinct.dim_subscription_id
      ) AS dim_subscription_id,
      instance_types_ordering.instance_type
    FROM prep_saas_usage_ping_namespace
    LEFT JOIN instance_types_ordering
      ON prep_saas_usage_ping_namespace.dim_namespace_id = instance_types_ordering.namespace_id
    INNER JOIN dim_date
      ON prep_saas_usage_ping_namespace.ping_date = dim_date.date_day
    LEFT JOIN namespace_subscription_monthly_distinct
      ON prep_saas_usage_ping_namespace.dim_namespace_id = namespace_subscription_monthly_distinct.dim_namespace_id
      AND dim_date.first_day_of_month = namespace_subscription_monthly_distinct.snapshot_month
    INNER JOIN gainsight_wave_2_3_metrics
      ON prep_saas_usage_ping_namespace.ping_name = gainsight_wave_2_3_metrics.metric_name
    LEFT JOIN map_subscription_namespace_month
      ON prep_saas_usage_ping_namespace.dim_namespace_id = map_subscription_namespace_month.dim_namespace_id
      AND dim_date.first_day_of_month = map_subscription_namespace_month.date_month
    WHERE COALESCE(
        map_subscription_namespace_month.dim_subscription_id,
        namespace_subscription_monthly_distinct.dim_subscription_id
      ) IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
      PARTITION BY 
        dim_date.first_day_of_month,
        COALESCE(
          map_subscription_namespace_month.dim_subscription_id,
          namespace_subscription_monthly_distinct.dim_subscription_id
        ),
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
    updated_date="2022-10-21"
) }}