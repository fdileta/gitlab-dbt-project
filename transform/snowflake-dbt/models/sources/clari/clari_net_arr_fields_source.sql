{{ config({
    "materialized": "incremental",
    "unique_key": "field_id"
    })
}}

WITH
source AS (
  SELECT
    value,
    uploaded_at
  FROM
    {{ source('clari', 'net_arr') }},
    LATERAL FLATTEN(input => jsontext:data:fields)

  {% if is_incremental() %}
    WHERE uploaded_at > (SELECT MAX(uploaded_at) FROM {{this}})
  {% endif %}
),

parsed AS (
  SELECT
    uploaded_at,
    value:fieldId::varchar AS field_id,
    value:fieldName::varchar AS field_name,
    value:fieldType::varchar AS field_type
  FROM
    source
  -- remove dups in case of overlapping data from daily/quarter loads
  QUALIFY
    ROW_NUMBER() OVER (
      PARTITION BY
        field_id
      ORDER BY
        uploaded_at DESC
    ) = 1
  ORDER BY field_id
)

SELECT
  *
FROM
  parsed
