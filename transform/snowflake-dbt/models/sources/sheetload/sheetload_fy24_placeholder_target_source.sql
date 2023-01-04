WITH source AS (

  SELECT *
  FROM {{ source('sheetload', 'fy24_placeholder_target')}}

)

SELECT *
FROM source