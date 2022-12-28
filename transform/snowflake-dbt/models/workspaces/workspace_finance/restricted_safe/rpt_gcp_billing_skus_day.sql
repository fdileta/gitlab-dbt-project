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
    export.project_id as gcp_project_id,
    export.service_description as gcp_service_description,
    export.sku_description as gcp_sku_description,
        CASE 
        -- STORAGE
        WHEN lower(gcp_service_description) like 'cloud storage' and (LOWER(gcp_sku_description) LIKE '%standard storage%')  THEN 'Storage'
        WHEN lower(gcp_service_description) like 'cloud storage' and (LOWER(gcp_sku_description) LIKE '%coldline storage%')  THEN 'Storage'
        WHEN lower(gcp_service_description) like 'cloud storage' and (LOWER(gcp_sku_description) LIKE '%archive storage%')  THEN 'Storage'
        WHEN lower(gcp_service_description) like 'cloud storage' and (LOWER(gcp_sku_description) LIKE '%nearline storage%')  THEN 'Storage'
        WHEN lower(gcp_service_description) like 'cloud storage' and (LOWER(gcp_sku_description) LIKE '%operations%')  THEN 'Storage'
        WHEN lower(gcp_service_description) like 'compute engine' AND LOWER(gcp_sku_description) LIKE '%pd capacity%' THEN 'Storage'
        WHEN lower(gcp_service_description) like 'compute engine' AND LOWER(gcp_sku_description) LIKE '%pd snapshot%'  THEN 'Storage'
        WHEN (LOWER(gcp_service_description) LIKE 'bigquery' and lower(gcp_sku_description) like '%storage%') then 'Storage'
        WHEN (LOWER(gcp_service_description) LIKE 'cloud sql' and lower(gcp_sku_description) like '%storage%') then 'Storage'
        
        -- COMPUTE
        WHEN LOWER(gcp_sku_description) LIKE '%commitment%' THEN 'Committed Usage'
        WHEN LOWER(gcp_service_description) LIKE 'bigquery' AND LOWER(gcp_sku_description) not like 'storage' THEN 'Compute'
        WHEN LOWER(gcp_service_description) LIKE 'cloud sql' AND (LOWER(gcp_sku_description) like '%cpu%' 
                                                              OR lower(gcp_sku_description) like '%ram%') THEN 'Compute'
        WHEN LOWER(gcp_service_description) LIKE 'kubernetes engine' THEN 'Compute'
        WHEN LOWER(gcp_service_description) LIKE 'vertex ai' THEN 'Compute'
        WHEN lower(gcp_service_description) LIKE 'compute engine' and lower(gcp_sku_description) like '%gpu%' THEN 'Compute'
        WHEN lower(gcp_service_description) LIKE '%memorystore%' and lower(gcp_sku_description) like '%capacity%' THEN 'Compute'
        WHEN lower(gcp_service_description) LIKE 'compute engine' and (lower(gcp_sku_description) like '%ram%'
                                                                OR lower(gcp_sku_description) like '%cpu%'
                                                                OR lower(gcp_sku_description) like '%core%') THEN 'Compute'
        
        -- NETWORKING
        WHEN lower(gcp_service_description) LIKE '%cloud sql%' and lower(gcp_sku_description) LIKE '%networking%' THEN 'Networking'
        WHEN lower(gcp_service_description) LIKE '%cloud pub/sub%' THEN 'Networking'
        WHEN lower(gcp_service_description) LIKE '%memorystore%' and lower(gcp_sku_description) LIKE '%networking%' THEN 'Networking'
        WHEN lower(gcp_service_description) LIKE '%cloud storage%' and lower(gcp_sku_description) LIKE '%egress%' THEN 'Networking'
        WHEN lower(gcp_service_description) LIKE '%cloud storage%' and lower(gcp_sku_description) LIKE '%cdn%' THEN 'Networking'
        WHEN LOWER(gcp_service_description) = 'networking' THEN 'Networking'
        WHEN LOWER(gcp_sku_description) LIKE '%load balanc%' THEN 'Networking'
        WHEN lower(gcp_service_description) LIKE '%compute engine%' and lower(gcp_sku_description) LIKE '%ip%' THEN 'Networking'
        WHEN lower(gcp_service_description) LIKE '%compute engine%' and lower(gcp_sku_description) LIKE '%network%' THEN 'Networking'
        WHEN lower(gcp_service_description) LIKE '%cloud storage%' and (lower(gcp_sku_description) LIKE '%network%' 
                                                                    OR lower(gcp_sku_description) LIKE '%download%') THEN 'Networking'
                                                                    

        -- LOGGING AND METRICS
        WHEN LOWER(gcp_service_description) LIKE 'stackdriver monitoring' THEN 'Logging and Metrics'
        WHEN LOWER(gcp_service_description) LIKE 'cloud logging' THEN 'Logging and Metrics'
        
        -- SUPPORT
        WHEN LOWER(gcp_sku_description) LIKE '%support%' THEN 'Support'
        WHEN LOWER(gcp_sku_description) LIKE '%security command center%' THEN 'Support'
        WHEN LOWER(gcp_sku_description) LIKE '%marketplace%' THEN 'Support'
    ELSE 'Other'
    END AS finance_sku_type,
    CASE 
        -- STORAGE
        WHEN lower(gcp_service_description) like 'cloud storage' and (LOWER(gcp_sku_description) LIKE '%standard storage%')  THEN 'Object (storage)'
        WHEN lower(gcp_service_description) like 'cloud storage' and (LOWER(gcp_sku_description) LIKE '%coldline storage%')  THEN 'Object (storage)'
        WHEN lower(gcp_service_description) like 'cloud storage' and (LOWER(gcp_sku_description) LIKE '%archive storage%')  THEN 'Object (storage)'
        WHEN lower(gcp_service_description) like 'cloud storage' and (LOWER(gcp_sku_description) LIKE '%nearline storage%')  THEN 'Object (storage)'
        WHEN lower(gcp_service_description) like 'cloud storage' and (LOWER(gcp_sku_description) LIKE '%operations%')  THEN 'Object (operations)'
        WHEN lower(gcp_service_description) like 'compute engine' AND LOWER(gcp_sku_description) LIKE '%pd capacity%' THEN 'Repository (storage)'
        WHEN lower(gcp_service_description) like 'compute engine' AND LOWER(gcp_sku_description) LIKE '%pd snapshot%'  THEN 'Repository (storage)'
        WHEN (LOWER(gcp_service_description) LIKE 'bigquery' and lower(gcp_sku_description) like '%storage%') then 'Data Warehouse (storage)'
        WHEN (LOWER(gcp_service_description) LIKE 'cloud sql' and lower(gcp_sku_description) like '%storage%') then 'Databases (storage)'
        
        -- COMPUTE
        WHEN LOWER(gcp_sku_description) LIKE '%commitment%' THEN 'Committed Usage'
        WHEN LOWER(gcp_service_description) LIKE 'bigquery' AND LOWER(gcp_sku_description) not like 'storage' THEN 'Data Warehouse (compute)'
        WHEN LOWER(gcp_service_description) LIKE 'cloud sql' AND (LOWER(gcp_sku_description) like '%cpu%' 
                                                              OR lower(gcp_sku_description) like '%ram%') THEN 'Data Warehouse (compute)'
        WHEN LOWER(gcp_service_description) LIKE 'kubernetes engine' THEN 'Container orchestration (compute)'
        WHEN LOWER(gcp_service_description) LIKE 'vertex ai' THEN 'AI/ML (compute)'
        WHEN lower(gcp_service_description) LIKE 'compute engine' and lower(gcp_sku_description) like '%gpu%' THEN 'AI/ML (compute)'
        WHEN lower(gcp_service_description) LIKE '%memorystore%' and lower(gcp_sku_description) like '%capacity%' THEN 'Memorystores (compute)'
        WHEN lower(gcp_service_description) LIKE 'compute engine' and (lower(gcp_sku_description) like '%ram%'
                                                                OR lower(gcp_sku_description) like '%cpu%'
                                                                OR lower(gcp_sku_description) like '%core%') THEN 'Containers (compute)'
        
        -- NETWORKING
        WHEN lower(gcp_service_description) LIKE '%cloud sql%' and lower(gcp_sku_description) LIKE '%networking%' THEN 'Databases (networking)'
        WHEN lower(gcp_service_description) LIKE '%cloud pub/sub%' THEN 'Messaging (networking)'
        WHEN lower(gcp_service_description) LIKE '%memorystore%' and lower(gcp_sku_description) LIKE '%networking%' THEN 'Memorystores (networking)'
        WHEN lower(gcp_service_description) LIKE '%cloud storage%' and lower(gcp_sku_description) LIKE '%egress%' THEN 'Object (networking)'
        WHEN lower(gcp_service_description) LIKE '%cloud storage%' and lower(gcp_sku_description) LIKE '%cdn%' THEN 'Object CDN (networking)'
        WHEN LOWER(gcp_service_description) = 'networking' THEN 'Networking (mixed)'
        WHEN LOWER(gcp_sku_description) LIKE '%load balanc%' THEN 'Load Balancing (networking)'
        WHEN lower(gcp_service_description) LIKE '%compute engine%' and lower(gcp_sku_description) LIKE '%ip%' THEN 'Container networking (networking)'
        WHEN lower(gcp_service_description) LIKE '%compute engine%' and lower(gcp_sku_description) LIKE '%network%' THEN 'Container networking (networking)'
        WHEN lower(gcp_service_description) LIKE '%cloud storage%' and (lower(gcp_sku_description) LIKE '%network%' 
                                                                    OR lower(gcp_sku_description) LIKE '%download%') THEN 'Container networking (networking)'
                                                                    

        -- LOGGING AND METRICS
        WHEN LOWER(gcp_service_description) LIKE 'stackdriver monitoring' THEN 'Metrics (logging and metrics)'
        WHEN LOWER(gcp_service_description) LIKE 'cloud logging' THEN 'Logging (logging and metrics)'
        
        -- SUPPORT
        WHEN LOWER(gcp_sku_description) LIKE '%support%' THEN 'Google Support (support)'
        WHEN LOWER(gcp_sku_description) LIKE '%security command center%' THEN 'Security (support)'
        WHEN LOWER(gcp_sku_description) LIKE '%marketplace%' THEN 'Marketplace (support)'
    ELSE 'Other'
    END AS finance_sku_subtype,
    usage_unit as gcp_usage_unit,
    sum(export.usage_amount) as usage_amount,
    sum(export.total_cost) AS net_cost
  FROM export 
  {{ dbt_utils.group_by(n=7) }} 




