WITH source AS (

    SELECT *
    FROM {{ ref('customers_db_license_version_source') }}

)

SELECT *
FROM source