 WITH source AS (

        SELECT 
            Name::VARCHAR AS name,
            Index::VARCHAR AS index,
            Scope::VARCHAR AS scope
        FROM {{ source('sheetload','ga360_customdimensions') }}

)

SELECT * 
FROM source
