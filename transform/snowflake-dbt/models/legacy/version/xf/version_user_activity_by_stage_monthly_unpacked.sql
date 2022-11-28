WITH
usage_data AS (

  SELECT *
  FROM {{ ref('version_usage_data_with_metadata') }}

),

unpacked_stage_json AS (

  SELECT
    usage_data.*,
    f.key AS stage_name,
    f.value AS stage_activity_count_json
  FROM usage_data
  INNER JOIN LATERAL FLATTEN(input => usage_data.usage_activity_by_stage_monthly) AS f
  WHERE IS_OBJECT(f.value) = TRUE


),

unpacked_other_metrics AS (

  SELECT
    usage_data.id,
    usage_data.version,
    usage_data.created_at,
    usage_data.uuid,
    usage_data.edition,
    usage_data.ping_source,
    usage_data.major_version,
    usage_data.main_edition,
    usage_data.edition_type,
    usage_data.license_plan_code,
    usage_data.company,
    usage_data.zuora_subscription_id,
    usage_data.zuora_subscription_status,
    usage_data.zuora_crm_id,
    NULL AS stage_name,
    DATEADD('days', -28, usage_data.created_at) AS period_start,
    usage_data.created_at AS period_end,
    f.key AS usage_action_name,
    IFF(f.value = -1, 0, f.value) AS usage_action_count
  FROM usage_data
  INNER JOIN LATERAL FLATTEN(input => usage_data.analytics_unique_visits) AS f
  WHERE IS_REAL(f.value) = TRUE

  UNION ALL

  SELECT
    usage_data.id,
    usage_data.version,
    usage_data.created_at,
    usage_data.uuid,
    usage_data.edition,
    usage_data.ping_source,
    usage_data.major_version,
    usage_data.main_edition,
    usage_data.edition_type,
    usage_data.license_plan_code,
    usage_data.company,
    usage_data.zuora_subscription_id,
    usage_data.zuora_subscription_status,
    usage_data.zuora_crm_id,
    'manage' AS stage_name,
    DATEADD('days', -28, usage_data.created_at) AS period_start,
    usage_data.created_at AS period_end,
    f.key AS usage_action_name,
    IFF(f.value = -1, 0, f.value) AS usage_action_count
  FROM usage_data
  INNER JOIN LATERAL FLATTEN(INPUT => usage_data.raw_usage_data_payload,
    recursive => TRUE, path => 'redis_hll_counters') AS f
  WHERE IS_REAL(f.value) = TRUE


),

unpacked_stage_metrics AS (

  SELECT
    unpacked_stage_json.id,
    unpacked_stage_json.version,
    unpacked_stage_json.created_at,
    unpacked_stage_json.uuid,
    unpacked_stage_json.edition,
    unpacked_stage_json.ping_source,
    unpacked_stage_json.major_version,
    unpacked_stage_json.main_edition,
    unpacked_stage_json.edition_type,
    unpacked_stage_json.license_plan_code,
    unpacked_stage_json.company,
    unpacked_stage_json.zuora_subscription_id,
    unpacked_stage_json.zuora_subscription_status,
    unpacked_stage_json.zuora_crm_id,
    unpacked_stage_json.stage_name,
    DATEADD('days', -28, unpacked_stage_json.created_at) AS period_start,
    unpacked_stage_json.created_at AS period_end,
    f.key AS usage_action_name,    
    IFF(f.value = -1, 0, f.value) AS usage_action_count
  FROM unpacked_stage_json
  INNER JOIN LATERAL FLATTEN(INPUT => unpacked_stage_json.stage_activity_count_json) AS f

),

final AS (

  SELECT *
  FROM unpacked_stage_metrics

  UNION ALL

  SELECT *
  FROM unpacked_other_metrics

)

SELECT *
FROM final
