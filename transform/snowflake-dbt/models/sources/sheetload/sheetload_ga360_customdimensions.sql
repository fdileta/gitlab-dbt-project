 WITH source AS (

        SELECT *
        FROM {{ source('sheetload','ga360_customdimensions') }}

)

SELECT * 
FROM source
