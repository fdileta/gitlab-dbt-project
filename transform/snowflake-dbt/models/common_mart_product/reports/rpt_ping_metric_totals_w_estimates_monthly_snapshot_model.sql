{{ config(
    materialized='incremental',
    unique_key='ping_metric_totals_w_estimates_monthly_snapshot_id',
    tags=["mnpi_exception","edm_snapshot", "product"]
) }}


WITH snapshot_dates AS (

    SELECT *
    FROM {{ ref('dim_date') }}
    WHERE date_actual >= '2022-12-01'
    AND date_actual <= CURRENT_DATE {% if is_incremental() %}

    -- this filter will only be applied on an incremental run
    AND date_id > (SELECT max(snapshot_id) FROM {{ this }})

    {% endif %}

), rpt_ping_metric_totals_w_estimates_monthly AS (

    SELECT *
    FROM {{  source('snapshots','rpt_ping_metric_totals_w_estimates_monthly_snapshot') }}

), rpt_ping_metric_totals_w_estimates_monthly_spined AS (

    SELECT
      snapshot_dates.date_id     AS snapshot_id,
      snapshot_dates.date_actual AS snapshot_date,
      rpt_ping_metric_totals_w_estimates_monthly.*
    FROM rpt_ping_metric_totals_w_estimates_monthly
    INNER JOIN snapshot_dates
    ON snapshot_dates.date_actual >= rpt_ping_metric_totals_w_estimates_monthly.dbt_valid_from
    AND snapshot_dates.date_actual < {{ coalesce_to_infinity('rpt_ping_metric_totals_w_estimates_monthly.dbt_valid_to') }}

), final AS (

     SELECT
       {{ dbt_utils.surrogate_key(['snapshot_id', 'ping_metric_totals_w_estimates_monthly_id']) }} AS ping_metric_totals_w_estimates_monthly_snapshot_id,
       *
     FROM rpt_ping_metric_totals_w_estimates_monthly_spined

)



{{ dbt_audit(
    cte_ref="final",
    created_by="@snalamaru",
    updated_by="@snalamaru",
    created_date="2022-11-03",
    updated_date="2022-11-03"
) }}
