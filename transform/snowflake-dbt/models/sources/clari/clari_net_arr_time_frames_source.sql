-- non-incremental, so that 'week_number' field can be calculated correctly

WITH
  source AS (
    SELECT
      value,
      jsontext:api_fiscal_quarter AS fiscal_quarter,
      uploaded_at
    FROM
      {{ source('clari', 'net_arr') }},
      LATERAL FLATTEN(input => jsontext:data:timeFrames)
  ),
  parsed AS (
    SELECT
      fiscal_quarter::varchar AS fiscal_quarter,
      concat(value:timeFrameId::varchar , '_', fiscal_quarter) time_frame_id,
      value:startDate::DATE week_start_date,
      value:endDate::DATE week_end_date,
      -- dense_rank() for dups prior to qualify
      DENSE_RANK() over (
        PARTITION BY
          fiscal_quarter
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
      fiscal_quarter,
      time_frame_id
  )
SELECT
  *
FROM
  parsed
