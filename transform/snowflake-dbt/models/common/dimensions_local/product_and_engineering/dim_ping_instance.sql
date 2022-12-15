{{ config(
    tags=["product", "mnpi_exception"],
    materialized = "table"
) }}

{{ simple_cte([
    ('dim_date', 'dim_date'),
    ('prep_ping_instance', 'prep_ping_instance'),
    ('prep_license','prep_license'),
    ('prep_charge','prep_charge'),
    ('prep_product_detail','prep_product_detail')
    ])

}}

, raw_flattened AS (

  SELECT 
    raw_usage_data_id,
    REPLACE(REPLACE(REPLACE(LOWER((raw_usage_data_payload:settings.collected_data_categories::TEXT)),'"',''),'[',''),']','') collected_data_categories 
  FROM prep_ping_instance,
    LATERAL FLATTEN(input=> raw_usage_data_payload,RECURSIVE => true)
  WHERE key = 'settings'
    AND value LIKE '%collected_data_categories%'
)

, usage_data_w_date AS (
  SELECT
    prep_ping_instance.*,
    dim_date.date_id                                 AS dim_ping_date_id
  FROM prep_ping_instance
  LEFT JOIN dim_date
    ON TO_DATE(ping_created_at) = dim_date.date_day

), last_ping_of_month_flag AS (

SELECT DISTINCT
    usage_data_w_date.id                              AS id,
    usage_data_w_date.dim_ping_date_id                AS dim_ping_date_id,
    usage_data_w_date.uuid                            AS uuid,
    usage_data_w_date.host_id                         AS host_id,
    usage_data_w_date.ping_created_at::TIMESTAMP(0)   AS ping_created_at,
    dim_date.first_day_of_month                       AS first_day_of_month,
    TRUE                                              AS last_ping_of_month_flag,
    usage_data_w_date.raw_usage_data_payload          AS raw_usage_data_payload
  FROM usage_data_w_date
  INNER JOIN dim_date
    ON usage_data_w_date.dim_ping_date_id = dim_date.date_id
  QUALIFY ROW_NUMBER() OVER (
          PARTITION BY usage_data_w_date.uuid, usage_data_w_date.host_id, dim_date.first_day_of_month
          ORDER BY ping_created_at DESC) = 1

), last_ping_of_week_flag AS (

  SELECT DISTINCT
    usage_data_w_date.id                              AS id,
    usage_data_w_date.dim_ping_date_id                AS dim_ping_date_id,
    usage_data_w_date.uuid                            AS uuid,
    usage_data_w_date.host_id                         AS host_id,
    usage_data_w_date.ping_created_at::TIMESTAMP(0)   AS ping_created_at,
    dim_date.first_day_of_month                       AS first_day_of_month,
    TRUE                                              AS last_ping_of_week_flag
  FROM usage_data_w_date
  INNER JOIN dim_date
    ON usage_data_w_date.dim_ping_date_id = dim_date.date_id
  QUALIFY ROW_NUMBER() OVER (
          PARTITION BY usage_data_w_date.uuid, usage_data_w_date.host_id, dim_date.first_day_of_week
          ORDER BY ping_created_at DESC) = 1

), fct_w_month_flag AS (

  SELECT
    usage_data_w_date.*,
    last_ping_of_month_flag.last_ping_of_month_flag   AS last_ping_of_month_flag,
    last_ping_of_week_flag.last_ping_of_week_flag     AS last_ping_of_week_flag
  FROM usage_data_w_date
  LEFT JOIN last_ping_of_month_flag
    ON usage_data_w_date.id = last_ping_of_month_flag.id
  LEFT JOIN last_ping_of_week_flag
    ON usage_data_w_date.id = last_ping_of_week_flag.id

), dedicated_instance AS (

  SELECT DISTINCT prep_ping_instance.uuid
  FROM prep_ping_instance
  INNER JOIN prep_license
    ON (prep_ping_instance.license_md5    = prep_license.license_md5 OR
        prep_ping_instance.license_sha256 = prep_license.license_sha256)
  INNER JOIN prep_charge
    ON prep_license.dim_subscription_id = prep_charge.dim_subscription_id
  INNER JOIN prep_product_detail
    ON prep_charge.dim_product_detail_id = prep_product_detail.dim_product_detail_id
  WHERE LOWER(prep_product_detail.product_rate_plan_charge_name) LIKE '%dedicated%'

), final AS (

    SELECT DISTINCT
      fct_w_month_flag.dim_ping_instance_id                                                                       AS dim_ping_instance_id,
      fct_w_month_flag.dim_ping_date_id                                                                           AS dim_ping_date_id,
      fct_w_month_flag.dim_host_id                                                                                AS dim_host_id,
      fct_w_month_flag.dim_instance_id                                                                            AS dim_instance_id,
      fct_w_month_flag.dim_installation_id                                                                        AS dim_installation_id,
      fct_w_month_flag.ping_created_at                                                                            AS ping_created_at,
      TO_DATE(DATEADD('days', -28, fct_w_month_flag.ping_created_at))                                             AS ping_created_date_28_days_earlier,
      TO_DATE(DATE_TRUNC('YEAR', fct_w_month_flag.ping_created_at))                                               AS ping_created_date_year,
      TO_DATE(DATE_TRUNC('MONTH', fct_w_month_flag.ping_created_at))                                              AS ping_created_date_month,
      TO_DATE(DATE_TRUNC('WEEK', fct_w_month_flag.ping_created_at))                                               AS ping_created_date_week,
      TO_DATE(DATE_TRUNC('DAY', fct_w_month_flag.ping_created_at))                                                AS ping_created_date,
      fct_w_month_flag.ip_address_hash                                                                            AS ip_address_hash,
      fct_w_month_flag.version                                                                                    AS version,
      fct_w_month_flag.instance_user_count                                                                        AS instance_user_count,
      fct_w_month_flag.license_md5                                                                                AS license_md5,
      fct_w_month_flag.license_sha256                                                                             AS license_sha256,
      fct_w_month_flag.historical_max_users                                                                       AS historical_max_users,
      fct_w_month_flag.license_user_count                                                                         AS license_user_count,
      fct_w_month_flag.license_starts_at                                                                          AS license_starts_at,
      fct_w_month_flag.license_expires_at                                                                         AS license_expires_at,
      fct_w_month_flag.license_add_ons                                                                            AS license_add_ons,
      fct_w_month_flag.recorded_at                                                                                AS recorded_at,
      fct_w_month_flag.updated_at                                                                                 AS updated_at,
      fct_w_month_flag.mattermost_enabled                                                                         AS mattermost_enabled,
      fct_w_month_flag.main_edition                                                                               AS ping_edition,
      fct_w_month_flag.hostname                                                                                   AS host_name,
      fct_w_month_flag.product_tier                                                                               AS product_tier,
      fct_w_month_flag.license_trial                                                                              AS is_trial,
      fct_w_month_flag.source_license_id                                                                          AS source_license_id,
      fct_w_month_flag.installation_type                                                                          AS installation_type,
      fct_w_month_flag.license_plan                                                                               AS license_plan,
      fct_w_month_flag.database_adapter                                                                           AS database_adapter,
      fct_w_month_flag.database_version                                                                           AS database_version,
      fct_w_month_flag.git_version                                                                                AS git_version,
      fct_w_month_flag.gitlab_pages_enabled                                                                       AS gitlab_pages_enabled,
      fct_w_month_flag.gitlab_pages_version                                                                       AS gitlab_pages_version,
      fct_w_month_flag.container_registry_enabled                                                                 AS container_registry_enabled,
      fct_w_month_flag.elasticsearch_enabled                                                                      AS elasticsearch_enabled,
      fct_w_month_flag.geo_enabled                                                                                AS geo_enabled,
      fct_w_month_flag.gitlab_shared_runners_enabled                                                              AS gitlab_shared_runners_enabled,
      fct_w_month_flag.gravatar_enabled                                                                           AS gravatar_enabled,
      fct_w_month_flag.ldap_enabled                                                                               AS ldap_enabled,
      fct_w_month_flag.omniauth_enabled                                                                           AS omniauth_enabled,
      fct_w_month_flag.reply_by_email_enabled                                                                     AS reply_by_email_enabled,
      fct_w_month_flag.signup_enabled                                                                             AS signup_enabled,
      fct_w_month_flag.prometheus_metrics_enabled                                                                 AS prometheus_metrics_enabled,
      fct_w_month_flag.usage_activity_by_stage                                                                    AS usage_activity_by_stage,
      fct_w_month_flag.usage_activity_by_stage_monthly                                                            AS usage_activity_by_stage_monthly,
      fct_w_month_flag.gitaly_clusters                                                                            AS gitaly_clusters,
      fct_w_month_flag.gitaly_version                                                                             AS gitaly_version,
      fct_w_month_flag.gitaly_servers                                                                             AS gitaly_servers,
      fct_w_month_flag.gitaly_filesystems                                                                         AS gitaly_filesystems,
      fct_w_month_flag.gitpod_enabled                                                                             AS gitpod_enabled,
      fct_w_month_flag.object_store                                                                               AS object_store,
      fct_w_month_flag.is_dependency_proxy_enabled                                                                AS is_dependency_proxy_enabled,
      fct_w_month_flag.recording_ce_finished_at                                                                   AS recording_ce_finished_at,
      fct_w_month_flag.recording_ee_finished_at                                                                   AS recording_ee_finished_at,
      fct_w_month_flag.is_ingress_modsecurity_enabled                                                             AS is_ingress_modsecurity_enabled,
      fct_w_month_flag.topology                                                                                   AS topology,
      fct_w_month_flag.is_grafana_link_enabled                                                                    AS is_grafana_link_enabled,
      fct_w_month_flag.analytics_unique_visits                                                                    AS analytics_unique_visits,
      fct_w_month_flag.raw_usage_data_id                                                                          AS raw_usage_data_id,
      fct_w_month_flag.container_registry_vendor                                                                  AS container_registry_vendor,
      fct_w_month_flag.container_registry_version                                                                 AS container_registry_version,
      IFF(fct_w_month_flag.license_expires_at >= fct_w_month_flag.ping_created_at 
          OR fct_w_month_flag.license_expires_at IS NULL, fct_w_month_flag.main_edition, 'EE Free')               AS cleaned_edition,
      REGEXP_REPLACE(NULLIF(fct_w_month_flag.version, ''), '[^0-9.]+')                                            AS cleaned_version,
      IFF(fct_w_month_flag.version ILIKE '%-pre', True, False)                                                    AS version_is_prerelease,
      SPLIT_PART(cleaned_version, '.', 1)::NUMBER                                                                 AS major_version,
      SPLIT_PART(cleaned_version, '.', 2)::NUMBER                                                                 AS minor_version,
      major_version || '.' || minor_version                                                                       AS major_minor_version,
      major_version * 100 + minor_version                                                                         AS major_minor_version_id,
      CASE
        WHEN fct_w_month_flag.uuid = 'ea8bf810-1d6f-4a6a-b4fd-93e8cbd8b57f'      THEN 'SaaS'
        ELSE 'Self-Managed'
        END                                                                                                       AS ping_delivery_type,
      CASE
        WHEN EXISTS (SELECT 1 FROM dedicated_instance di
                     WHERE fct_w_month_flag.uuid = di.uuid)     THEN TRUE
        ELSE FALSE END                                                                                            AS is_saas_dedicated,
      CASE
        WHEN ping_delivery_type = 'SaaS'                                          THEN TRUE
        WHEN fct_w_month_flag.installation_type = 'gitlab-development-kit'        THEN TRUE
        WHEN fct_w_month_flag.hostname = 'gitlab.com'                             THEN TRUE
        WHEN fct_w_month_flag.hostname ILIKE '%.gitlab.com'                       THEN TRUE
        ELSE FALSE END                                                                                            AS is_internal,
      CASE
        WHEN fct_w_month_flag.hostname ilike 'staging.%'                          THEN TRUE
        WHEN fct_w_month_flag.hostname IN (
        'staging.gitlab.com',
        'dr.gitlab.com'
      )                                                         THEN TRUE
        ELSE FALSE END                                                                                            AS is_staging,
      CASE
        WHEN fct_w_month_flag.last_ping_of_month_flag = TRUE                      THEN TRUE
        ELSE FALSE
      END                                                                                                         AS is_last_ping_of_month,
      CASE
        WHEN fct_w_month_flag.last_ping_of_week_flag = TRUE                       THEN TRUE
        ELSE FALSE
      END                                                                                                         AS is_last_ping_of_week,
      raw_flattened.collected_data_categories,
      fct_w_month_flag.raw_usage_data_payload,
      fct_w_month_flag.ping_type
    FROM fct_w_month_flag
    LEFT JOIN raw_flattened
      ON fct_w_month_flag.raw_usage_data_id = raw_flattened.raw_usage_data_id
)

{{ dbt_audit(
    cte_ref="final",
    created_by="@icooper-acp",
    updated_by="@mdrussell",
    created_date="2022-03-08",
    updated_date="2022-12-12"
) }}
