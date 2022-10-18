{{ config(alias='wk_sales_tableau_rls_users') }}

WITH source AS (

    SELECT *
    FROM {{ source('sheetload', 'sales_analytics_tableau_rls_users') }}

)


SELECT *
FROM source
