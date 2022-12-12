WITH source AS (

    SELECT *
    FROM {{ ref('sheetload_ga360_customdimensions') }}

)

SELECT *
FROM source
