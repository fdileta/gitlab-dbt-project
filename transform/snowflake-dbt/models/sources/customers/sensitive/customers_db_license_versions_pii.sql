WITH source AS (

    SELECT *
    FROM {{ ref('customers_db_license_versions') }}

), customers_db_license_versions_pii AS (

    SELECT
      customer_id,
      {{ nohash_sensitive_columns('customers_db_license_versions', 'customer_email') }}
    FROM source

)

SELECT *
FROM customers_db_license_versions_pii
