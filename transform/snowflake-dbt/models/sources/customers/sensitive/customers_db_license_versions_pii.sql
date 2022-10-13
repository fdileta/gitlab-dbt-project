WITH source AS (

    SELECT *
    FROM {{ ref('customers_db_license_versions_source') }}

), customers_db_license_versions_source_pii AS (

    SELECT id,
      {{ nohash_sensitive_columns('customers_db_license_versions_source', 'id') }}
    FROM source

)

SELECT *
FROM customers_db_license_versions_source_pii