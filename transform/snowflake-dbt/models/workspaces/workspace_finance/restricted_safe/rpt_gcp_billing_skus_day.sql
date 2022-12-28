{{ config(
    materialized='table',
    )
}}


WITH export as (

SELECT * FROM {{ ref('gcp_billing_export_xf') }}
WHERE invoice_month >= '2022-01-01'
)



SELECT 
    date(export.usage_end_time) as day,
    export.project_id as project_id,
    export.service_description as service,
    export.sku_description as sku_description,
    CASE 
        -- STORAGE
        WHEN lower(service_description) like 'cloud storage' and (LOWER(sku_description) LIKE '%standard storage%')  THEN 'Object (storage)'
        WHEN lower(service_description) like 'cloud storage' and (LOWER(sku_description) LIKE '%coldline storage%')  THEN 'Object (storage)'
        WHEN lower(service_description) like 'cloud storage' and (LOWER(sku_description) LIKE '%archive storage%')  THEN 'Object (storage)'
        WHEN lower(service_description) like 'cloud storage' and (LOWER(sku_description) LIKE '%nearline storage%')  THEN 'Object (storage)'
        WHEN lower(service_description) like 'cloud storage' and (LOWER(sku_description) LIKE '%operations%')  THEN 'Object (operations)'
        WHEN lower(service_description) like 'compute engine' AND LOWER(sku_description) LIKE '%pd capacity%' THEN 'Repository (storage)'
        WHEN lower(service_description) like 'compute engine' AND LOWER(sku_description) LIKE '%pd snapshot%'  THEN 'Repository (storage)'
        WHEN (LOWER(service_description) LIKE 'bigquery' and lower(sku_description) like '%storage%') then 'Data Warehouse (storage)'
        WHEN (LOWER(service_description) LIKE 'cloud sql' and lower(sku_description) like '%storage%') then 'Databases (storage)'
        
        -- COMPUTE
        WHEN LOWER(sku_description) LIKE '%commitment%' THEN 'Committed Usage'
        WHEN LOWER(service_description) LIKE 'bigquery' AND LOWER(sku_description) not like 'storage' THEN 'Data Warehouse (compute)'
        WHEN LOWER(service_description) LIKE 'cloud sql' AND (LOWER(sku_description) like '%cpu%' 
                                                              OR lower(sku_description) like '%ram%') THEN 'Data Warehouse (compute)'
        WHEN LOWER(service_description) LIKE 'kubernetes engine' THEN 'Container orchestration (compute)'
        WHEN LOWER(service_description) LIKE 'vertex ai' THEN 'AI/ML (compute)'
        WHEN lower(service_description) LIKE 'compute engine' and lower(sku_description) like '%gpu%' THEN 'AI/ML (compute)'
        WHEN lower(service_description) LIKE '%memorystore%' and lower(sku_description) like '%capacity%' THEN 'Memorystores (compute)'
        WHEN lower(service_description) LIKE 'compute engine' and (lower(sku_description) like '%ram%'
                                                                OR lower(sku_description) like '%cpu%'
                                                                OR lower(sku_description) like '%core%') THEN 'Containers (compute)'
        
        -- NETWORKING
        WHEN lower(service_description) LIKE '%cloud sql%' and lower(sku_description) LIKE '%networking%' THEN 'Databases (networking)'
        WHEN lower(service_description) LIKE '%cloud pub/sub%' THEN 'Messaging (networking)'
        WHEN lower(service_description) LIKE '%memorystore%' and lower(sku_description) LIKE '%networking%' THEN 'Memorystores (networking)'
        WHEN lower(service_description) LIKE '%cloud storage%' and lower(sku_description) LIKE '%egress%' THEN 'Object (networking)'
        WHEN lower(service_description) LIKE '%cloud storage%' and lower(sku_description) LIKE '%cdn%' THEN 'Object CDN (networking)'
        WHEN LOWER(service_description) = 'networking' THEN 'Networking (mixed)'
        WHEN LOWER(sku_description) LIKE '%load balanc%' THEN 'Load Balancing (networking)'
        WHEN lower(service_description) LIKE '%compute engine%' and lower(sku_description) LIKE '%ip%' THEN 'Container networking (networking)'
        WHEN lower(service_description) LIKE '%compute engine%' and lower(sku_description) LIKE '%network%' THEN 'Container networking (networking)'
        WHEN lower(service_description) LIKE '%cloud storage%' and (lower(sku_description) LIKE '%network%' 
                                                                    OR lower(sku_description) LIKE '%download%') THEN 'Container networking (networking)'
                                                                    

        -- LOGGING AND METRICS
        WHEN LOWER(service_description) LIKE 'stackdriver monitoring' THEN 'Metrics (logging and metrics)'
        WHEN LOWER(service_description) LIKE 'cloud logging' THEN 'Logging (logging and metrics)'
        
        -- SUPPORT
        WHEN LOWER(sku_description) LIKE '%support%' THEN 'Google Support (support)'
        WHEN LOWER(sku_description) LIKE '%security command center%' THEN 'Security (support)'
        WHEN LOWER(sku_description) LIKE '%marketplace%' THEN 'Marketplace (support)'
    ELSE 'Other'
    END AS sku_subtype,
    sum(export.usage_amount) as usage_amount,
    sum(export.total_cost) AS net_cost
  FROM export 
  group by 1, 2, 3, 4, 5