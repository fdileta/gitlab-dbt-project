WITH source AS (

    SELECT *
    FROM {{ ref('customers_db_license_version') }}

)

SELECT *
FROM source