{{ config({
    "materialized": "incremental",
    "unique_key": "entries_id"
    })
}}

WITH
  source AS (
    SELECT
      value,
      uploaded_at
    FROM
      {{ source('clari', 'clari_net_arr') }},
      LATERAL FLATTEN(input => jsontext:data:entries)
    {% if is_incremental() %}
      WHERE uploaded_at > (SELECT MAX(uploaded_at) FROM {{this}})
    {% endif %}
  ),
  parsed AS (
    SELECT
      -- foreign keys
      REPLACE(value:timePeriodId, '_', '-')::varchar fiscal_quarter,
      -- there can be the same timeFrameId across quarters
      concat(value:timeFrameId::varchar , '_', fiscal_quarter) as time_frame_id
      value:userId::varchar user_id,
      value:fieldId::varchar field_id,

      -- primary key, must be after aliased cols are derived else sql error
      concat_ws(' | ', fiscal_quarter, time_frame_id, user_id, field_id) as entries_id,
      -- logical info
      value:forecastValue::number (38, 1) forecast_value,
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
