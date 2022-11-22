WITH source AS (

    SELECT *
    FROM {{ source('sheetload', 'toy_marketing_handbook') }}

)

SELECT *
FROM source


