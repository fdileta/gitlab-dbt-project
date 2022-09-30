{{ config(
    materialized='table',
    tags=["mnpi_exception", "product"]
) }}

{{ simple_cte([
    ('dim_ping_metric', 'dim_ping_metric')
    ])

}}

, fct_ping_instance_metric AS (

    SELECT
        {{ dbt_utils.star(from=ref('fct_ping_instance_metric'), except=['CREATED_BY', 'UPDATED_BY', 'MODEL_CREATED_DATE', 'MODEL_UPDATED_DATE', 'DBT_CREATED_AT', 'DBT_UPDATED_AT']) }}
    FROM {{ ref('fct_ping_instance_metric') }}

)

SELECT
  fct_ping_instance_metric.*,
  dim_ping_metric.time_frame
FROM fct_ping_instance_metric
INNER JOIN dim_ping_metric
  ON fct_ping_instance_metric.metrics_path = dim_ping_metric.metrics_path
WHERE time_frame = 'none'
