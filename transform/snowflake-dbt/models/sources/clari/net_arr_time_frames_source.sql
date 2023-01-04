{{ config({
    "materialized": "incremental",
    "unique_key": "time_frame_id"
    })
}}

WITH
  source AS (
    SELECT
      value,
      jsontext:api_fiscal_quarter AS api_fiscal_quarter,
      uploaded_at
    FROM
      {{ source('clari', 'net_arr') }},
      LATERAL FLATTEN(input => jsontext:data:timeFrames)

      {% if is_incremental() %}
        WHERE uploaded_at > (SELECT MAX(uploaded_at) FROM {{this}})
      {% endif %}
  ),
  parsed AS (
    SELECT
      api_fiscal_quarter::varchar AS api_fiscal_quarter,
      value:timeFrameId::varchar time_frame_id,
      value:startDate::DATE week_start_date,
      value:endDate::DATE week_end_date,
      -- dense_rank() for dups prior to qualify
      DENSE_RANK() over (
        PARTITION BY
          api_fiscal_quarter
        ORDER BY
          time_frame_id
      ) AS week_number,
      uploaded_at
    FROM
      source
    -- remove dups in case of overlapping data from daily/quarter loads
    QUALIFY
      ROW_NUMBER() over (
        PARTITION BY
          time_frame_id
        ORDER BY
          uploaded_at desc
      ) = 1
    ORDER BY
      api_fiscal_quarter,
      time_frame_id
  )
SELECT
  *
FROM
  parsed
