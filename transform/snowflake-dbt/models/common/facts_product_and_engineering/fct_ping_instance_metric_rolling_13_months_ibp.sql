{{ config(
    tags=["product", "mnpi_exception"],
    materialized='insert_by_period',
    unique_key='ping_instance_metric_id',
    period='month',
    timestamp_field = "ping_created_date",
    start_date = "2021-08-01"   
) }}

{{ simple_cte([
    ('dim_ping_metric', 'dim_ping_metric')
    ])

}}

, fct_ping_instance_metric AS (

    SELECT
      {{ dbt_utils.star(from=ref('fct_ping_instance_metric'), except=['CREATED_BY', 'UPDATED_BY', 'MODEL_CREATED_DATE', 'MODEL_UPDATED_DATE', 'DBT_CREATED_AT', 'DBT_UPDATED_AT']) }}
    FROM {{ ref('fct_ping_instance_metric') }} 
    WHERE DATE_TRUNC(MONTH, ping_created_date) >= DATEADD(MONTH, -13, DATE_TRUNC(MONTH,CURRENT_DATE))
    AND __PERIOD_FILTER__

),

final AS (
    
    SELECT 
      fct_ping_instance_metric.*,
      dim_ping_metric.time_frame
    FROM fct_ping_instance_metric
    LEFT JOIN dim_ping_metric
      ON fct_ping_instance_metric.metrics_path = dim_ping_metric.metrics_path
    
        
)


{{ dbt_audit(
    cte_ref="final",
    created_by="@iweeks",
    updated_by="@iweeks",
    created_date="2022-07-20",
    updated_date="2022-07-29"
) }}