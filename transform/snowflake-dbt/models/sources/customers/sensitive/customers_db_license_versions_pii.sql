WITH source AS (

    SELECT *
    FROM {{ ref('customers_db_license_versions_source') }}

), customers_db_license_versions_source_pii AS (

    SELECT item_id,
      {{ nohash_sensitive_columns('customers_db_license_versions_source', 'item_id') }}
    FROM source

)

SELECT *
FROM customers_db_license_versions_source_pii