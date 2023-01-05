-- non-incremental, so that 'week_number' field can be calculated correctly

WITH
source AS (
  SELECT
    value,
    uploaded_at,
    jsontext:api_fiscal_quarter AS fiscal_quarter
  FROM
    {{ source('clari', 'net_arr') }},
    LATERAL FLATTEN(input => jsontext:data:timeFrames)
),

parsed AS (
  SELECT
    fiscal_quarter::varchar AS fiscal_quarter,
    uploaded_at,
    CONCAT(value:timeFrameId::varchar, '_', fiscal_quarter) AS time_frame_id,
    value:startDate::date AS week_start_date,
    -- dense_rank() for dups prior to qualify
    value:endDate::date AS week_end_date,
    DENSE_RANK() OVER (
      PARTITION BY
        fiscal_quarter
      ORDER BY
        time_frame_id
    ) AS week_number
  FROM
    source
  -- remove dups in case of overlapping data from daily/quarter loads
  QUALIFY
    ROW_NUMBER() OVER (
      PARTITION BY
        time_frame_id
      ORDER BY
        uploaded_at DESC
    ) = 1
  ORDER BY
    fiscal_quarter,
    time_frame_id
)

SELECT
  *
FROM
  parsed
