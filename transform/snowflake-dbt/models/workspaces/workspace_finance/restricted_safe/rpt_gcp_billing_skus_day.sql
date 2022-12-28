{{ config(
    materialized='table',
    )
}}


WITH export AS (

  SELECT * FROM {{ ref('gcp_billing_export_xf') }}
  WHERE invoice_month >= '2022-01-01'

)

SELECT
  export.project_id AS gcp_project_id,
  export.service_description AS gcp_service_description,
  export.sku_description AS gcp_sku_description,
  export.usage_unit AS gcp_usage_unit,
  date(export.usage_end_time) AS day,
  CASE
    -- STORAGE
    WHEN
      lower(
        gcp_service_description
      ) LIKE 'cloud storage' AND (
        lower(gcp_sku_description) LIKE '%standard storage%'
      ) THEN 'Storage'
    WHEN
      lower(
        gcp_service_description
      ) LIKE 'cloud storage' AND (
        lower(gcp_sku_description) LIKE '%coldline storage%'
      ) THEN 'Storage'
    WHEN
      lower(
        gcp_service_description
      ) LIKE 'cloud storage' AND (
        lower(gcp_sku_description) LIKE '%archive storage%'
      ) THEN 'Storage'
    WHEN
      lower(
        gcp_service_description
      ) LIKE 'cloud storage' AND (
        lower(gcp_sku_description) LIKE '%nearline storage%'
      ) THEN 'Storage'
    WHEN
      lower(
        gcp_service_description
      ) LIKE 'cloud storage' AND (lower(gcp_sku_description) LIKE '%operations%') THEN 'Storage'
    WHEN
      lower(
        gcp_service_description
      ) LIKE 'compute engine' AND lower(gcp_sku_description) LIKE '%pd capacity%' THEN 'Storage'
    WHEN
      lower(
        gcp_service_description
      ) LIKE 'compute engine' AND lower(gcp_sku_description) LIKE '%pd snapshot%' THEN 'Storage'
    WHEN
      (
        lower(
          gcp_service_description
        ) LIKE 'bigquery' AND lower(gcp_sku_description) LIKE '%storage%'
      ) THEN 'Storage'
    WHEN
      (
        lower(
          gcp_service_description
        ) LIKE 'cloud sql' AND lower(gcp_sku_description) LIKE '%storage%'
      ) THEN 'Storage'

    -- COMPUTE
    WHEN lower(gcp_sku_description) LIKE '%commitment%' THEN 'Committed Usage'
    WHEN
      lower(
        gcp_service_description
      ) LIKE 'bigquery' AND lower(gcp_sku_description) NOT LIKE 'storage' THEN 'Compute'
    WHEN
      lower(gcp_service_description) LIKE 'cloud sql' AND (lower(gcp_sku_description) LIKE '%cpu%'
        OR lower(gcp_sku_description) LIKE '%ram%') THEN 'Compute'
    WHEN lower(gcp_service_description) LIKE 'kubernetes engine' THEN 'Compute'
    WHEN lower(gcp_service_description) LIKE 'vertex ai' THEN 'Compute'
    WHEN
      lower(
        gcp_service_description
      ) LIKE 'compute engine' AND lower(gcp_sku_description) LIKE '%gpu%' THEN 'Compute'
    WHEN
      lower(
        gcp_service_description
      ) LIKE '%memorystore%' AND lower(gcp_sku_description) LIKE '%capacity%' THEN 'Compute'
    WHEN
      lower(
        gcp_service_description
      ) LIKE 'compute engine' AND (lower(gcp_sku_description) LIKE '%ram%'
        OR lower(gcp_sku_description) LIKE '%cpu%'
        OR lower(gcp_sku_description) LIKE '%core%') THEN 'Compute'

    -- NETWORKING
    WHEN
      lower(
        gcp_service_description
      ) LIKE '%cloud sql%' AND lower(gcp_sku_description) LIKE '%networking%' THEN 'Networking'
    WHEN lower(gcp_service_description) LIKE '%cloud pub/sub%' THEN 'Networking'
    WHEN
      lower(
        gcp_service_description
      ) LIKE '%memorystore%' AND lower(gcp_sku_description) LIKE '%networking%' THEN 'Networking'
    WHEN
      lower(
        gcp_service_description
      ) LIKE '%cloud storage%' AND lower(gcp_sku_description) LIKE '%egress%' THEN 'Networking'
    WHEN
      lower(
        gcp_service_description
      ) LIKE '%cloud storage%' AND lower(gcp_sku_description) LIKE '%cdn%' THEN 'Networking'
    WHEN lower(gcp_service_description) = 'networking' THEN 'Networking'
    WHEN lower(gcp_sku_description) LIKE '%load balanc%' THEN 'Networking'
    WHEN
      lower(
        gcp_service_description
      ) LIKE '%compute engine%' AND lower(gcp_sku_description) LIKE '%ip%' THEN 'Networking'
    WHEN
      lower(
        gcp_service_description
      ) LIKE '%compute engine%' AND lower(gcp_sku_description) LIKE '%network%' THEN 'Networking'
    WHEN
      lower(
        gcp_service_description
      ) LIKE '%cloud storage%' AND (lower(gcp_sku_description) LIKE '%network%'
        OR lower(gcp_sku_description) LIKE '%download%') THEN 'Networking'


    -- LOGGING AND METRICS
    WHEN lower(gcp_service_description) LIKE 'stackdriver monitoring' THEN 'Logging and Metrics'
    WHEN lower(gcp_service_description) LIKE 'cloud logging' THEN 'Logging and Metrics'

    -- SUPPORT
    WHEN lower(gcp_sku_description) LIKE '%support%' THEN 'Support'
    WHEN lower(gcp_sku_description) LIKE '%security command center%' THEN 'Support'
    WHEN lower(gcp_sku_description) LIKE '%marketplace%' THEN 'Support'
    ELSE 'Other'
  END AS finance_sku_type,
  CASE
    -- STORAGE
    WHEN
      lower(
        gcp_service_description
      ) LIKE 'cloud storage' AND (
        lower(gcp_sku_description) LIKE '%standard storage%'
      ) THEN 'Object (storage)'
    WHEN
      lower(
        gcp_service_description
      ) LIKE 'cloud storage' AND (
        lower(gcp_sku_description) LIKE '%coldline storage%'
      ) THEN 'Object (storage)'
    WHEN
      lower(
        gcp_service_description
      ) LIKE 'cloud storage' AND (
        lower(gcp_sku_description) LIKE '%archive storage%'
      ) THEN 'Object (storage)'
    WHEN
      lower(
        gcp_service_description
      ) LIKE 'cloud storage' AND (
        lower(gcp_sku_description) LIKE '%nearline storage%'
      ) THEN 'Object (storage)'
    WHEN
      lower(
        gcp_service_description
      ) LIKE 'cloud storage' AND (
        lower(gcp_sku_description) LIKE '%operations%'
      ) THEN 'Object (operations)'
    WHEN
      lower(
        gcp_service_description
      ) LIKE 'compute engine' AND lower(
        gcp_sku_description
      ) LIKE '%pd capacity%' THEN 'Repository (storage)'
    WHEN
      lower(
        gcp_service_description
      ) LIKE 'compute engine' AND lower(
        gcp_sku_description
      ) LIKE '%pd snapshot%' THEN 'Repository (storage)'
    WHEN
      (
        lower(
          gcp_service_description
        ) LIKE 'bigquery' AND lower(gcp_sku_description) LIKE '%storage%'
      ) THEN 'Data Warehouse (storage)'
    WHEN
      (
        lower(
          gcp_service_description
        ) LIKE 'cloud sql' AND lower(gcp_sku_description) LIKE '%storage%'
      ) THEN 'Databases (storage)'

    -- COMPUTE
    WHEN lower(gcp_sku_description) LIKE '%commitment%' THEN 'Committed Usage'
    WHEN
      lower(
        gcp_service_description
      ) LIKE 'bigquery' AND lower(
        gcp_sku_description
      ) NOT LIKE 'storage' THEN 'Data Warehouse (compute)'
    WHEN
      lower(gcp_service_description) LIKE 'cloud sql' AND (lower(gcp_sku_description) LIKE '%cpu%'
        OR lower(gcp_sku_description) LIKE '%ram%') THEN 'Data Warehouse (compute)'
    WHEN
      lower(
        gcp_service_description
      ) LIKE 'kubernetes engine' THEN 'Container orchestration (compute)'
    WHEN lower(gcp_service_description) LIKE 'vertex ai' THEN 'AI/ML (compute)'
    WHEN
      lower(
        gcp_service_description
      ) LIKE 'compute engine' AND lower(gcp_sku_description) LIKE '%gpu%' THEN 'AI/ML (compute)'
    WHEN
      lower(
        gcp_service_description
      ) LIKE '%memorystore%' AND lower(
        gcp_sku_description
      ) LIKE '%capacity%' THEN 'Memorystores (compute)'
    WHEN
      lower(
        gcp_service_description
      ) LIKE 'compute engine' AND (lower(gcp_sku_description) LIKE '%ram%'
        OR lower(gcp_sku_description) LIKE '%cpu%'
        OR lower(gcp_sku_description) LIKE '%core%') THEN 'Containers (compute)'

    -- NETWORKING
    WHEN
      lower(
        gcp_service_description
      ) LIKE '%cloud sql%' AND lower(
        gcp_sku_description
      ) LIKE '%networking%' THEN 'Databases (networking)'
    WHEN lower(gcp_service_description) LIKE '%cloud pub/sub%' THEN 'Messaging (networking)'
    WHEN
      lower(
        gcp_service_description
      ) LIKE '%memorystore%' AND lower(
        gcp_sku_description
      ) LIKE '%networking%' THEN 'Memorystores (networking)'
    WHEN
      lower(
        gcp_service_description
      ) LIKE '%cloud storage%' AND lower(
        gcp_sku_description
      ) LIKE '%egress%' THEN 'Object (networking)'
    WHEN
      lower(
        gcp_service_description
      ) LIKE '%cloud storage%' AND lower(
        gcp_sku_description
      ) LIKE '%cdn%' THEN 'Object CDN (networking)'
    WHEN lower(gcp_service_description) = 'networking' THEN 'Networking (mixed)'
    WHEN lower(gcp_sku_description) LIKE '%load balanc%' THEN 'Load Balancing (networking)'
    WHEN
      lower(
        gcp_service_description
      ) LIKE '%compute engine%' AND lower(
        gcp_sku_description
      ) LIKE '%ip%' THEN 'Container networking (networking)'
    WHEN
      lower(
        gcp_service_description
      ) LIKE '%compute engine%' AND lower(
        gcp_sku_description
      ) LIKE '%network%' THEN 'Container networking (networking)'
    WHEN
      lower(
        gcp_service_description
      ) LIKE '%cloud storage%' AND (lower(gcp_sku_description) LIKE '%network%'
        OR lower(gcp_sku_description) LIKE '%download%') THEN 'Container networking (networking)'


    -- LOGGING AND METRICS
    WHEN
      lower(
        gcp_service_description
      ) LIKE 'stackdriver monitoring' THEN 'Metrics (logging and metrics)'
    WHEN lower(gcp_service_description) LIKE 'cloud logging' THEN 'Logging (logging and metrics)'

    -- SUPPORT
    WHEN lower(gcp_sku_description) LIKE '%support%' THEN 'Google Support (support)'
    WHEN lower(gcp_sku_description) LIKE '%security command center%' THEN 'Security (support)'
    WHEN lower(gcp_sku_description) LIKE '%marketplace%' THEN 'Marketplace (support)'
    ELSE 'Other'
  END AS finance_sku_subtype,
  sum(export.usage_amount) AS usage_amount,
  sum(export.total_cost) AS net_cost
FROM export
{{ dbt_utils.group_by(n=7) }}
