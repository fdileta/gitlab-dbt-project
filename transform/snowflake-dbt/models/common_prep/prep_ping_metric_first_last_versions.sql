
{{ config(
    tags=["product", "mnpi_exception"]
) }}

{{ simple_cte([
    ('dim_ping_instance', 'dim_ping_instance'),
    ('dim_ping_metric', 'dim_ping_metric')
    ])

}}

, fct_ping_instance_metric AS (

    SELECT
      {{ dbt_utils.star(from=ref('fct_ping_instance_metric'), except=['METRIC_VALUE', 'CREATED_BY', 'UPDATED_BY', 'MODEL_CREATED_DATE',
          'MODEL_UPDATED_DATE', 'DBT_CREATED_AT', 'DBT_UPDATED_AT']) }},
      TRY_TO_NUMBER(metric_value::TEXT) AS metric_value
    FROM {{ ref('fct_ping_instance_metric') }}

),

final AS (


    SELECT
      fct_ping_instance_metric.metrics_path,
      dim_ping_instance.ping_edition,
      dim_ping_instance.version_is_prerelease
      dim_ping_instance.major_minor_version_id ,
      dim_ping_metric.time_frame
    FROM fct_ping_instance_metric
    INNER JOIN dim_ping_metric
      ON fct_ping_instance_metric.metrics_path = dim_ping_metric.metrics_path
    INNER JOIN dim_ping_instance
      ON fct_ping_instance_metric.dim_ping_instance_id = dim_ping_instance.dim_ping_instance_id
      WHERE --excluding 7d but thought this would be more forward reliable in case of new time_frames
      (
      dim_ping_metric.time_frame = 'none'
      OR 
      dim_ping_metric.time_frame = 'all'
      OR 
      dim_ping_metric.time_frame = '28d'
      )

{{ dbt_audit(
    cte_ref="final",
    created_by="@mpetersen",
    updated_by="@mpetersen",
    created_date="2022-11-04",
    updated_date="2022-11-04"
) }}
