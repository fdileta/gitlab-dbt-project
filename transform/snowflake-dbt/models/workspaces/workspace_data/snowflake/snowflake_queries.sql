{{
  config(
    materialized='incremental',
    unique_key='query_id',
    on_schema_change='sync_all_columns'
  )
}}

WITH source AS (

  SELECT *
  FROM {{ ref('snowflake_query_history_source') }}
  {% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  WHERE end_time > (SELECT MAX(end_time) FROM {{ this }})

  {% endif %}

),

expanded AS (

  SELECT
    database_name,
    database_id,
    query_id,
    query_type,
    query_text,
    query_tag,
    execution_status,
    schema_name,
    schema_id,
    user_name,
    role_name,
    warehouse_name,
    warehouse_id,
    warehouse_size,
    warehouse_type,
    cluster_number,
    start_time,
    end_time,
    compilation_time,
    execution_time,
    total_elapsed_time,
    queued_provisioning_time,
    queued_repair_time,
    queued_overload_time,
    transaction_blocked_time,
    rows_produced,
    bytes_written,
    bytes_spilled_to_remote_storage,
    bytes_spilled_to_local_storage,
    credits_used_cloud_services,
    percentage_scanned_from_cache,
    try_parse_json(regexp_substr(query_text, '\{\"app\".*\}')) AS dbt_metadata,
    dbt_metadata['dbt_version']::VARCHAR AS dbt_version,
    dbt_metadata['profile_name']::VARCHAR AS dbt_profile_name,
    dbt_metadata['target_name']::VARCHAR AS dbt_target_name,
    dbt_metadata['target_user']::VARCHAR AS dbt_target_user,
    dbt_metadata['invocation_id']::VARCHAR AS dbt_invocation_id,
    dbt_metadata['run_started_at']::VARCHAR AS dbt_run_started_at,
    dbt_metadata['full_refresh']::BOOLEAN AS is_model_full_refresh,
    dbt_metadata['is_full_refresh']::BOOLEAN AS is_invocation_full_refresh,
    dbt_metadata['materialized']::VARCHAR AS model_materialization,
    dbt_metadata['runner']::VARCHAR AS dbt_runner,
    dbt_metadata['file']::VARCHAR AS resource_file,
    dbt_metadata['node_id']::VARCHAR AS resource_id,
    dbt_metadata['node_name']::VARCHAR AS resource_name,
    dbt_metadata['resource_type']::VARCHAR AS resource_type,
    dbt_metadata['package_name']::VARCHAR AS package_name,
    dbt_metadata['relation']['database']::VARCHAR AS relation_database,
    dbt_metadata['relation']['schema']::VARCHAR AS relation_schema,
    dbt_metadata['relation']['identifier']::VARCHAR AS relation_identifier
  FROM source

)

SELECT *
FROM expanded
