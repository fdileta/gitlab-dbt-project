{{ config(
    materialized="incremental",
    unique_key="entries_id",
    tags=["mnpi"]
    )
}}

WITH
source AS (
  SELECT * FROM
    {{ source('clari', 'net_arr') }}
),

intermediate AS (
  SELECT
    d.value,
    source.uploaded_at
  FROM
    source,
    LATERAL FLATTEN(input => jsontext:data:entries) AS d
  {% if is_incremental() %}
    WHERE source.uploaded_at > (SELECT MAX(t.uploaded_at) FROM {{ this }} AS t)
  {% endif %}
),

parsed AS (
  SELECT

    -- foreign keys
    uploaded_at,
    REPLACE(value:timePeriodId, '_', '-')::varchar AS fiscal_quarter,
    -- add fiscal_quarter to unique key: can be dup timeFrameId across quarters
    CONCAT(value:timeFrameId::varchar, '_', fiscal_quarter) AS time_frame_id,
    value:userId::varchar AS user_id,
    value:fieldId::varchar AS field_id,

    -- primary key, must be after aliased cols are derived else sql error
    CONCAT_WS(' | ', time_frame_id, user_id, field_id) AS entries_id,

    -- logical info
    value:forecastValue::number(38, 1) AS forecast_value,
    value:currency::variant AS currency,
    value:isUpdated::boolean AS is_updated,
    value:updatedBy::varchar AS updated_by,

    -- metadata
    TO_TIMESTAMP(value:updatedOn::int / 1000) AS updated_on
  FROM
    intermediate

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
