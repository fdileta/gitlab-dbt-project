{{ config(alias='wk_sales_tableau_rls_roles') }}

WITH source AS (

    SELECT *
    FROM {{ source('sheetload', 'sales_analytics_tableau_rls_roles') }}

)


SELECT *
FROM source
