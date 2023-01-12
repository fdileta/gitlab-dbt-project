{{ config(
    materialized='table',
    )
}}


WITH services AS (

  SELECT * FROM {{ ref('gcp_billing_pl_mapping_day') }}

),

hvs AS (
SELECT
inte.day,
inte.project_id AS gcp_project_id,
inte.service AS gcp_service_description,
inte.sku_description AS gcp_sku_description,
inte.sku_subtype AS finance_new_sku_subtype,
inte.networking_allocation_sku_mapping AS finance_old_sku_type,
inte.gl_product_category AS gl_label_product_category,
inte.product_gl_service AS product_gl_service,
CASE
    WHEN product_gl_service IS NOT NULL THEN 'COGS OK: gitlab service'
    WHEN gl_label_product_category IS NOT NULL THEN 'COGS OK: infra label'
    WHEN gcp_service_description = 'Support' THEN 'COGS OK: Support is internal'
    WHEN regexp_like(inte.project_id, '.*([0-9])') -- contains a number 
    AND regexp_like(inte.project_id, '([a-z]){0,15}-[0-9a-z]{8}$') THEN 'COGS OK: sandbox'
    WHEN lower(gcp_sku_description) like '%commitment%' THEN 'COGS OK: same split as the rest of compute engine'
    WHEN lower(gcp_service_description) like '%security command center%' THEN 'COGS OK: security internal?' -- or split logic with top customers network transfers?
END
AS finance_hvs,
inte.net_cost AS net_cost
FROM
inte)
SELECT
*
FROM
hvs
