{{ config(alias='wk_sales_tableau_rls_roles') }}

WITH source AS (

    SELECT *
    --FROM {{ ref('sheetload_sales_analytics_tableau_rls_roles_source') }}
    FROM {{ source('sheetload', 'sales_analytics_tableau_rls_roles') }}

)


SELECT *
FROM source
