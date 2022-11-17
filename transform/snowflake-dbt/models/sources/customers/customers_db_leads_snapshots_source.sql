WITH source AS (

    SELECT *
    FROM {{ source('snapshots','customers_db_leads_snapshots') }}

), renamed AS (

    SELECT
      *
    FROM source
    
)

SELECT  *
FROM renamed