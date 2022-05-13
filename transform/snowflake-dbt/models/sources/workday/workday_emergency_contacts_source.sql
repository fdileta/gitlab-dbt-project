WITH source AS (
  SELECT *
  FROM {{ source('workday','emergency_contacts') }}
),

renamed AS (

  SELECT
    source.employee_id::NUMBER AS employee_id,
    source._fivetran_synced::TIMESTAMP AS uploaded_at,
    events.value['FULL_NAME']::VARCHAR AS full_name,
    events.value['HOME_PHONE']::VARCHAR AS home_phone,
    events.value['MOBILE_PHONE']::VARCHAR AS mobile_phone,
    events.value['WORK_PHONE']::VARCHAR AS work_phone
  FROM source
  INNER JOIN LATERAL FLATTEN(INPUT => source.emergency_contacts) AS events

)

SELECT *
FROM renamed
