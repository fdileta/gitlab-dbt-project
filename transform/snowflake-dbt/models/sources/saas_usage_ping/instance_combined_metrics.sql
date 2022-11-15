WITH source AS (

  SELECT
    *
  FROM {{ source('saas_usage_ping', 'instance_combined_metrics')}}

),

cleaned AS (

  SELECT
    ping_date::DATE AS ping_date,
    run_id::VARCHAR AS run_id,
    recorded_at::TIMESTAMP AS recorded_at,
    edition::VARCHAR AS edition,
    recording_ce_finished_at::TIMESTAMP AS recording_ce_finished_at,
    recording_ee_finished_at::TIMESTAMP AS recording_ee_finished_at,
    uuid::VARCHAR AS uuid,
    source::VARCHAR AS ping_source,
    PARSE_JSON(query_map) AS query_map,
    PARSE_JSON(run_results) AS run_results,
    DATEADD('s', _uploaded_at, '1970-01-01')::TIMESTAMP AS uploaded_at
  FROM source
  WHERE ping_source = 'combined'

),

final AS (
  SELECT
    run_id AS id,
    -- run_results['source_ip']::VARCHAR AS source_ip,
    recorded_at,
    uploaded_at AS created_at,
    uploaded_at AS updated_at,
    uuid,
    -- run_results['historical_max_users']::VARCHAR AS historical_max_users,
    edition,
    NULL AS raw_usage_data_id,
    NULL AS raw_usage_data_payload,
    run_results['version']::VARCHAR AS version,
    run_results['active_user_count']::NUMBER AS instance_user_count,
    run_results['license_md5']::VARCHAR AS license_md5,
    run_results['license_sha256']::VARCHAR AS license_sha256,
    run_results['license_user_count']::VARCHAR AS license_user_count,
    run_results['license_starts_at']::TIMESTAMP AS license_starts_at,
    run_results['license_expires_at']::TIMESTAMP AS license_expires_at,
    PARSE_JSON(run_results['license_expires_at']) AS license_add_ons,
    632::INT AS host_id, -- this is the GitLab host_id
    run_results['mattermost_enabled']::BOOLEAN AS mattermost_enabled,
    run_results['hostname']::VARCHAR AS hostname,
    run_results['license_trial']::BOOLEAN AS license_trial,
    run_results['source_license_id']::NUMBER AS source_license_id,
    run_results['installation_type']::VARCHAR AS installation_type,
    -- run_results['database_version']::VARCHAR AS database_version,
    run_results['license_plan']::VARCHAR AS license_plan,
    run_results['database']['adapter']::VARCHAR AS database_adapter,
    CONCAT(
      run_results['git']['version']['major'],
      '.',
      run_results['git']['version']['minor'],
      '.',
      run_results['git']['version']['patch'],
      '.',
      run_results['git']['version']['suffix_s']) AS git_version,
    run_results['gitlab_pages']['enabled']::BOOLEAN AS gitlab_pages_enabled,
    run_results['gitlab_pages']['version']::VARCHAR AS gitlab_pages_version,
    run_results['container_registry_enabled']::BOOLEAN AS container_registry_enabled,
    run_results['elasticsearch_enabled']::BOOLEAN AS elasticsearch_enabled,
    run_results['geo_enabled']::BOOLEAN AS geo_enabled,
    run_results['gitlab_shared_runners_enabled']::BOOLEAN AS gitlab_shared_runners_enabled,
    run_results['gravatar_enabled']::BOOLEAN AS gravatar_enabled,
    run_results['ldap_enabled']::BOOLEAN AS ldap_enabled,
    run_results['omniauth_enabled']::BOOLEAN AS omniauth_enabled,
    run_results['reply_by_email_enabled']::BOOLEAN AS reply_by_email_enabled,
    run_results['signup_enabled']::BOOLEAN AS signup_enabled,
    run_results['prometheus_metrics_enabled']::BOOLEAN AS prometheus_metrics_enabled,
    1::NUMBER AS gitaly_clusters,
    PARSE_JSON(run_results['usage_activity_by_stage']) AS usage_activity_by_stage,
    -- run_results['gitaly_servers']::NUMBER AS gitaly_servers,
    PARSE_JSON(run_results['usage_activity_by_stage_monthly']) AS usage_activity_by_stage_monthly,
    't'::VARCHAR AS gitpod_enabled, -- matching the data type of prep_ping_instance
    run_results['gitaly']['version']::VARCHAR AS gitaly_version,
    run_results['gitaly']['filesystems']::VARCHAR AS gitaly_filesystems,
    PARSE_JSON(run_results['object_store']) AS object_store,
    run_results['dependency_proxy_enabled']::BOOLEAN AS is_dependency_proxy_enabled,
    run_results['recording_ce_finished_at']::TIMESTAMP AS recording_ce_finished_at,
    run_results['recording_ee_finished_at']::TIMESTAMP AS recording_ee_finished_at,
    PARSE_JSON(run_results['counts']) AS stats_used,
    run_results['counts'] AS counts,
    run_results['ingress_modsecurity_enabled']::BOOLEAN AS is_ingress_modsecurity_enabled,
    PARSE_JSON(run_results['topology']) AS topology,
    run_results['grafana_link_enabled']::BOOLEAN AS is_grafana_link_enabled,
    'gitlab'::VARCHAR AS container_registry_vendor,
    PARSE_JSON(run_results['analytics_unique_visits']) AS analytics_unique_visits,
    run_results['container_registry_server']['version']::VARCHAR AS container_registry_version
  FROM cleaned
)

{{ dbt_audit(
    cte_ref="final",
    created_by="@mdrussell",
    updated_by="@mdrussell",
    created_date="2022-11-09",
    updated_date="2022-11-14"
) }}
