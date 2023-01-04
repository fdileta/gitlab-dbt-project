{{ config({
    "materialized": "incremental",
    "unique_key": "id"
    })
}}

WITH
  source AS (
    SELECT
      value,
      uploaded_at
    FROM
      {{ source('clari', 'net_arr') }},
      LATERAL FLATTEN(input => jsontext:data:entries)
    {% if is_incremental() %}
      WHERE uploaded_at > (SELECT MAX(uploaded_at) FROM {{this}})
    {% endif %}
  ),
  parsed AS (
    SELECT
      -- primary data
      concat_ws('-', fiscal_quarter, time_frame_id, user_id, field_id) AS id,
      value:forecastValue::NUMBER (38, 1) forecast_value,

      -- Foreign keys
      REPLACE(value:timePeriodId, '_', '-')::varchar fiscal_quarter,
      value:timeFrameId::varchar time_frame_id,
      value:userId::varchar user_id,
      value:fieldId::varchar field_id,

      -- Logical info
      value:currency::variant currency,
      value:isUpdated::boolean is_updated,
      value:updatedBy::varchar updated_by,
      TO_TIMESTAMP(value:updatedOn::int / 1000) updated_on,

      -- metadata
      uploaded_at
    FROM
      source
    -- remove dups in case of overlapping data from daily/quarter loads
    QUALIFY
      ROW_NUMBER() over (
        PARTITION BY
          fiscal_quarter,
          time_frame_id,
          user_id,
          field_id
        ORDER BY
          uploaded_at desc
      ) = 1
    ORDER BY
      time_frame_id
  )
SELECT
  *
FROM
  parsed
