{{ config(
    materialized="incremental",
    unique_key="fiscal_quarter"
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
    LATERAL FLATTEN(input => jsontext:data:timePeriods) AS d

  {% if is_incremental() %}
    WHERE uploaded_at > (SELECT MAX(uploaded_at) FROM {{ this }})
  {% endif %}
),

parsed AS (
  SELECT
    uploaded_at,
    REPLACE(value:timePeriodId, '_', '-')::varchar AS fiscal_quarter,
    value:startDate::date AS fiscal_quarter_start_date,
    value:endDate::date AS fiscal_quarter_end_date,
    value:label::varchar AS quarter,
    value:year::number AS year,
    value:crmId::varchar AS crm_id,
    value:type::varchar AS time_period_type
  FROM
    intermediate

  -- remove dups in case of overlapping data from daily/quarter loads
  QUALIFY
    ROW_NUMBER() OVER (
      PARTITION BY
        fiscal_quarter
      ORDER BY
        uploaded_at DESC
    ) = 1
  ORDER BY
    fiscal_quarter
)

SELECT
  *
FROM
  parsed
