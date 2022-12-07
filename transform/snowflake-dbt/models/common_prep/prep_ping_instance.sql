{{ config(
    tags=["product", "mnpi_exception"],
    materialized = "incremental",
    unique_key = "dim_ping_instance_id"
) }}


{{ simple_cte([
    ('raw_usage_data', 'version_raw_usage_data_source'),
    ('automated_instance_service_ping', 'instance_combined_metrics')
    ])

}}

, source AS (

    SELECT
      id                                                                        AS dim_ping_instance_id,
      created_at::TIMESTAMP(0)                                                  AS ping_created_at,
      *,
      {{ nohash_sensitive_columns('version_usage_data_source', 'source_ip') }}  AS ip_address_hash
    FROM {{ ref('version_usage_data_source') }} as usage

  {% if is_incremental() %}
          WHERE ping_created_at >= (SELECT MAX(ping_created_at) FROM {{this}})
  {% endif %}

), usage_data AS (

    SELECT
      dim_ping_instance_id                                                                                                    AS dim_ping_instance_id,
      host_id                                                                                                                 AS dim_host_id,
      uuid                                                                                                                    AS dim_instance_id,
      ping_created_at                                                                                                         AS ping_created_at,
      source_ip_hash                                                                                                          AS ip_address_hash,
      edition                                                                                                                 AS original_edition,
      {{ dbt_utils.star(from=ref('version_usage_data_source'), except=['EDITION', 'CREATED_AT', 'SOURCE_IP']) }}
    FROM source
    WHERE uuid IS NOT NULL
      AND version NOT LIKE ('%VERSION%')

), joined_ping AS (

    SELECT
      dim_ping_instance_id                                                                                                                        AS dim_ping_instance_id,
      dim_host_id                                                                                                                                 AS dim_host_id,
      usage_data.dim_instance_id                                                                                                                  AS dim_instance_id,
      {{ dbt_utils.surrogate_key(['dim_host_id', 'dim_instance_id'])}}                                                                            AS dim_installation_id,
      ping_created_at                                                                                                                             AS ping_created_at,
      ip_address_hash                                                                                                                             AS ip_address_hash,
      original_edition                                                                                                                            AS original_edition,
      {{ dbt_utils.star(from=ref('version_usage_data_source'), relation_alias='usage_data', except=['EDITION', 'CREATED_AT', 'SOURCE_IP']) }},
      IFF(original_edition = 'CE', 'CE', 'EE')                                                                                                    AS main_edition,
      CASE
        WHEN original_edition = 'CE'                                     THEN 'Core'
        WHEN original_edition = 'EE Free'                                THEN 'Core'
        WHEN license_expires_at < ping_created_at                        THEN 'Core'
        WHEN original_edition = 'EE'                                     THEN 'Starter'
        WHEN original_edition = 'EES'                                    THEN 'Starter'
        WHEN original_edition = 'EEP'                                    THEN 'Premium'
        WHEN original_edition = 'EEU'                                    THEN 'Ultimate'
        ELSE NULL END                                                                                                                             AS product_tier,
        COALESCE(raw_usage_data.raw_usage_data_payload, usage_data.raw_usage_data_payload_reconstructed)                                          AS raw_usage_data_payload
    FROM usage_data
    LEFT JOIN raw_usage_data
      ON usage_data.raw_usage_data_id = raw_usage_data.raw_usage_data_id
    WHERE usage_data.ping_created_at <= (SELECT MAX(created_at) FROM raw_usage_data)
      AND NOT(dim_host_id = 632 AND ping_created_at >= '2022-12-01') --excluding GitLab SaaS pings from 2022-12-01 and after

), automated_service_ping AS (

    SELECT
      id AS dim_ping_instance_id,
      host_id AS dim_host_id,
      uuid AS dim_instance_id,
      {{ dbt_utils.surrogate_key(['host_id', 'uuid'])}} AS dim_installation_id,
      created_at AS ping_created_at,
      NULL AS ip_address_hash,
      edition AS original_edition,
      id,
      version,
      instance_user_count,
      license_md5,
      license_sha256,
      historical_max_users,
      license_user_count,
      license_starts_at,
      license_expires_at,
      license_add_ons,
      recorded_at,
      updated_at,
      mattermost_enabled,
      uuid,
      hostname,
      host_id,
      license_trial,
      source_license_id,
      installation_type,
      license_plan,
      database_adapter,
      database_version,
      git_version,
      gitlab_pages_enabled,
      gitlab_pages_version,
      container_registry_enabled,
      elasticsearch_enabled,
      geo_enabled,
      gitlab_shared_runners_enabled,
      gravatar_enabled,
      ldap_enabled,
      omniauth_enabled,
      reply_by_email_enabled,
      signup_enabled,
      prometheus_metrics_enabled,
      usage_activity_by_stage,
      usage_activity_by_stage_monthly,
      gitaly_clusters,
      gitaly_version,
      gitaly_servers,
      gitaly_filesystems,
      gitpod_enabled,
      object_store,
      is_dependency_proxy_enabled,
      recording_ce_finished_at,
      recording_ee_finished_at,
      stats_used,
      counts,
      is_ingress_modsecurity_enabled,
      topology,
      is_grafana_link_enabled,
      analytics_unique_visits,
      raw_usage_data_id,
      container_registry_vendor,
      container_registry_version,
      NULL AS raw_usage_data_payload_reconstructed,
      IFF(edition = 'CE', 'CE', 'EE') AS main_edition,
      CASE
        WHEN edition = 'CE'                   THEN 'Core'
        WHEN edition = 'EE Free'              THEN 'Core'
        WHEN license_expires_at < created_at  THEN 'Core'
        WHEN edition = 'EE'                   THEN 'Starter'
        WHEN edition = 'EES'                  THEN 'Starter'
        WHEN edition = 'EEP'                  THEN 'Premium'
        WHEN edition = 'EEU'                  THEN 'Ultimate'
        ELSE NULL 
      END AS product_tier,
      run_results AS raw_usage_data_payload
    FROM automated_instance_service_ping
    WHERE created_at >= '2022-12-01' --start using the automated SaaS Service Ping in for Dec 2022 reporting

), final AS (

    SELECT * FROM joined_ping

    UNION ALL

    SELECT * FROM automated_service_ping

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@icooper-acp",
    updated_by="@mdrussell",
    created_date="2022-03-17",
    updated_date="2022-12-06"
) }}
