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
      value:fieldId::varchar field_id,
      value:fieldName::varchar field_name,
      value:fieldType::varchar field_type,
      uploaded_at
    FROM
      source
    -- remove dups in case of overlapping data from daily/quarter loads
    QUALIFY
      ROW_NUMBER() over (
        PARTITION BY
          field_id
        ORDER BY
          uploaded_at desc
      ) = 1
    ORDER BY field_id
  )
SELECT
  *
FROM
  parsed
