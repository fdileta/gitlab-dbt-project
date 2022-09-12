
WITH source AS (

  SELECT {{ hash_sensitive_columns('customers_db_license_versions_source') }}
  FROM {{ref('customers_db_license_versions_source')}}

)

SELECT *
FROM source
