WITH source AS (

  SELECT * 
  FROM {{ source('driveload','marketing_press_sov') }}

)
SELECT * 
FROM source