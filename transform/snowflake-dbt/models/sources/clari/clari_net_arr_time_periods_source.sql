{{ config({
    "materialized": "incremental",
    "unique_key": "fiscal_quarter"
    })
}}

WITH
  source AS (
    SELECT
      value,
      uploaded_at
    FROM
      {{ source('clari', 'clari_net_arr') }},
      LATERAL FLATTEN(input => jsontext:data:timePeriods)

      {% if is_incremental() %}
        WHERE uploaded_at > (SELECT MAX(uploaded_at) FROM {{this}})
      {% endif %}
  ),
parsed AS (
    SELECT
      REPLACE(value:timePeriodId, '_', '-')::varchar fiscal_quarter,
      value:startDate::DATE fiscal_quarter_start_date,
      value:endDate::DATE fiscal_quarter_end_date,
      value:label::varchar quarter,
      value:year::number year,
      value:crmId::varchar crm_id,
      value:type::varchar time_period_type,
      uploaded_at
    FROM
      source
    -- remove dups in case of overlapping data from daily/quarter loads
    QUALIFY
      ROW_NUMBER() over (
        PARTITION BY
          fiscal_quarter
        ORDER BY
          uploaded_at desc
      ) = 1
    ORDER BY
      fiscal_quarter
  )
SELECT
  *
FROM
  parsed
