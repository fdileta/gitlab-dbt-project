 WITH source AS (

        SELECT 
            Name::VARCHAR AS dimension_name,
            Index::NUMBER AS dimension_index,
            Scope::VARCHAR AS dimension_scope
        FROM {{ source('sheetload','ga360_customdimensions') }}

)

SELECT * 
FROM source
