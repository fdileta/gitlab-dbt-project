 WITH source AS (

        SELECT 
            LOWER(Name)::VARCHAR AS name,
            LOWER(Index)::VARCHAR AS index,
            LOWER(Scope)::VARCHAR AS scope
        FROM {{ source('sheetload','ga360_customdimensions') }}

)

SELECT * 
FROM source
