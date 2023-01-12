{{ config(
    materialized='table',
    )
}}


WITH service_base AS (

  SELECT * FROM {{ ref('rpt_gcp_billing_service_mapping_day') }}


),

allocation as (

  SELECT * FROM {{ ref('gcp_billing_service_allocation') }}
  unpivot(allocation for type in (free, internal, paid))

),

sandbox as (

  SELECT * FROM {{ ref('gcp_billing_sandbox_projects') }}

)

SELECT
service_base.day,
service_base.gcp_project_id,
service_base.gcp_service_description,
service_base.gcp_sku_description,
service_base.gitlab_service,
coalesce(allocation.type, sandbox.classification, 'unknown') as finance_pl,
service_base.net_cost * coalesce(allocation.allocation, 1) as net_cost
FROM
service_base
LEFT JOIN allocation on allocation.service = service_base.gitlab_service
LEFT JOIN sandbox on sandbox.project_name = service_base.gcp_project_id


-- CASE
--     WHEN product_gl_service IS NOT NULL THEN 'COGS OK: gitlab service'
--     WHEN gl_label_product_category IS NOT NULL THEN 'COGS OK: infra label'
--     WHEN gcp_service_description = 'Support' THEN 'COGS OK: Support is internal'
--     WHEN regexp_like(inte.project_id, '.*([0-9])') -- contains a number 
--     AND regexp_like(inte.project_id, '([a-z]){0,15}-[0-9a-z]{8}$') THEN 'COGS OK: sandbox'
--     WHEN lower(gcp_sku_description) like '%commitment%' THEN 'COGS OK: same split as the rest of compute engine'
--     WHEN lower(gcp_service_description) like '%security command center%' THEN 'COGS OK: security internal?' -- or split logic with top customers network transfers?
-- END
-- AS finance_hvs,
