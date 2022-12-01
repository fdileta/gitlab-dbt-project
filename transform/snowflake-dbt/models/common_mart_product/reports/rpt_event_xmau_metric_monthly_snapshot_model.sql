{{ config(
    materialized='incremental',
    unique_key='rpt_event_xmau_metric_monthly_snapshot_id',
    tags=["edm_snapshot", "product"]
) }}


WITH snapshot_dates AS (

    SELECT *
    FROM {{ ref('dim_date') }}
    WHERE date_actual >= '2022-11-10'
    AND date_actual <= CURRENT_DATE {% if is_incremental() %}

    -- this filter will only be applied on an incremental run
    AND date_id > (SELECT max(snapshot_id) FROM {{ this }})

    {% endif %}

), rpt_event_xmau_metric_monthly AS (

    SELECT *
    FROM {{ ref('rpt_event_xmau_metric_monthly_snapshot') }}

), rpt_event_xmau_metric_monthly_spined AS (

    SELECT
      snapshot_dates.date_id     AS snapshot_id,
      snapshot_dates.date_actual AS snapshot_date,
      rpt_event_xmau_metric_monthly.*
    FROM rpt_event_xmau_metric_monthly
    INNER JOIN snapshot_dates
    ON snapshot_dates.date_actual >= rpt_event_xmau_metric_monthly.dbt_valid_from
    AND snapshot_dates.date_actual < {{ coalesce_to_infinity('rpt_event_xmau_metric_monthly.dbt_valid_to') }}

), final AS (

     SELECT
       {{ dbt_utils.surrogate_key(['snapshot_id', 'primary_key']) }} AS rpt_event_xmau_metric_monthly_snapshot_id,
       *
     FROM rpt_event_xmau_metric_monthly_spined

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@snalamaru",
    updated_by="@snalamaru",
    created_date="2022-11-03",
    updated_date="2022-11-03"
) }}
