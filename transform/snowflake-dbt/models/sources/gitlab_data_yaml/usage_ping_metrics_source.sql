WITH source AS (

    SELECT *
    FROM {{ source('gitlab_data_yaml', 'usage_ping_metrics') }}

), intermediate AS (

    SELECT
      d.value                                 AS data_by_row,
      uploaded_at,
      date_trunc('day', uploaded_at)::date    AS snapshot_date
    FROM source,
    LATERAL FLATTEN(INPUT => parse_json(jsontext), OUTER => TRUE) d

), renamed AS (

     SELECT 
      data_by_row['key_path']::TEXT                                                     AS metrics_path,
      data_by_row['data_source']::TEXT                                                  AS data_source,
      data_by_row['description']::TEXT                                                  AS description,
      data_by_row['product_category']::TEXT                                             AS product_category,
      data_by_row['product_group']::TEXT                                                AS product_group,
      data_by_row['product_section']::TEXT                                              AS product_section,
      data_by_row['product_stage']::TEXT                                                AS product_stage,
      data_by_row['milestone']::TEXT                                                    AS milestone,
      data_by_row['skip_validation']::TEXT                                              AS skip_validation,
      data_by_row['status']::TEXT                                                       AS metrics_status,
      data_by_row['tier']                                                               AS tier,
      data_by_row['time_frame']::TEXT                                                   AS time_frame,
      data_by_row['value_type']::TEXT                                                   AS value_type,
      ARRAY_CONTAINS( 'gmau'::VARIANT , data_by_row['performance_indicator_type'])      AS is_gmau,
      ARRAY_CONTAINS( 'smau'::VARIANT , data_by_row['performance_indicator_type'])      AS is_smau,
      ARRAY_CONTAINS( 'paid_gmau'::VARIANT , data_by_row['performance_indicator_type']) AS is_paid_gmau,
      ARRAY_CONTAINS( 'umau'::VARIANT , data_by_row['performance_indicator_type'])      AS is_umau,
      snapshot_date,
      uploaded_at,
      data_by_row
    FROM intermediate

)

SELECT *
FROM renamed
