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
    {{ source('clari', 'net_arr') }},
    LATERAL FLATTEN(input => jsontext:data:entries)
  {% if is_incremental() %}
    WHERE uploaded_at > (SELECT MAX(uploaded_at) FROM {{this}})
  {% endif %}
),

parsed AS (
  SELECT
    -- foreign keys
    uploaded_at,
    -- there can be the same timeFrameId across quarters
    REPLACE(value:timePeriodId, '_', '-')::varchar AS fiscal_quarter,
    CONCAT(value:timeFrameId::varchar, '_', fiscal_quarter) AS time_frame_id,
    value:userId::varchar AS user_id,

    -- primary key, must be after aliased cols are derived else sql error
    value:fieldId::varchar AS field_id,
    -- logical info
    CONCAT_WS(' | ', fiscal_quarter, time_frame_id, user_id, field_id) AS entries_id,
    value:forecastValue::number(38, 1) AS forecast_value,
    value:currency::variant AS currency,
    value:isUpdated::boolean AS is_updated,
    value:updatedBy::varchar AS updated_by,

    -- metadata
    TO_TIMESTAMP(value:updatedOn::int / 1000) AS updated_on
  FROM
    source
  -- remove dups in case of overlapping data from daily/quarter loads
  QUALIFY
    ROW_NUMBER() OVER (
      PARTITION BY
        fiscal_quarter,
        time_frame_id,
        user_id,
        field_id
      ORDER BY
        uploaded_at DESC
    ) = 1
  ORDER BY
    time_frame_id
)


SELECT
  *
FROM
  parsed
