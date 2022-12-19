{{ config(
    tags=["product", "mnpi_exception"],
    materialized = "incremental",
    unique_key = "ping_instance_metric_id"
) }}

{%- set settings_columns = dbt_utils.get_column_values(table=ref('prep_usage_ping_metrics_setting'), column='metrics_path', max_records=1000, default=['']) %}

{{ simple_cte([
    ('prep_subscription', 'prep_subscription'),
    ('prep_usage_ping_metrics_setting', 'prep_usage_ping_metrics_setting'),
    ('dim_date', 'dim_date'),
    ('map_ip_to_country', 'map_ip_to_country'),
    ('locations', 'prep_location_country'),
    ('prep_ping_instance', 'prep_ping_instance_flattened'),
    ('dim_product_tier', 'dim_product_tier')
    ])

}}
, prep_license AS (

    SELECT
      license_md5,
      license_sha256,
      dim_license_id,
      dim_subscription_id
    FROM {{ ref('prep_license') }}

), prep_license_md5 AS (

    SELECT
      license_md5,
      dim_license_id,
      dim_subscription_id
    FROM prep_license
    WHERE license_md5 IS NOT NULL

), prep_license_sha256 AS (

    SELECT
      license_sha256,
      dim_license_id,
      dim_subscription_id
    FROM prep_license
    WHERE license_sha256 IS NOT NULL

), map_ip_location AS (

    SELECT
      map_ip_to_country.ip_address_hash                 AS ip_address_hash,
      map_ip_to_country.dim_location_country_id         AS dim_location_country_id
    FROM map_ip_to_country
    INNER JOIN locations
      WHERE map_ip_to_country.dim_location_country_id = locations.dim_location_country_id

), source AS (

    SELECT
      prep_ping_instance.*
    FROM prep_ping_instance
      {% if is_incremental() %}
                  WHERE ping_created_at >= (SELECT MAX(ping_created_at) FROM {{this}})
      {% endif %}

), add_country_info_to_usage_ping AS (

    SELECT
      source.*,
      map_ip_location.dim_location_country_id     AS dim_location_country_id
    FROM source
    LEFT JOIN map_ip_location
      ON source.ip_address_hash = map_ip_location.ip_address_hash

), prep_usage_ping_cte AS (

    SELECT
      dim_ping_instance_id                                AS dim_ping_instance_id,
      dim_host_id                                         AS dim_host_id,
      dim_instance_id                                     AS dim_instance_id,
      dim_installation_id                                 AS dim_installation_id,
      dim_product_tier.dim_product_tier_id                AS dim_product_tier_id,
      ping_created_at                                     AS ping_created_at,
      license_md5                                         AS license_md5,
      license_sha256                                      AS license_sha256,
      dim_location_country_id                             AS dim_location_country_id,
      license_trial_ends_on                               AS license_trial_ends_on,
      license_subscription_id                             AS license_subscription_id,
      umau_value                                          AS umau_value,
      product_tier                                        AS product_tier,
      main_edition                                        AS main_edition,
      metrics_path                                        AS metrics_path,
      metric_value                                        AS metric_value,
      has_timed_out                                       AS has_timed_out
    FROM add_country_info_to_usage_ping
    LEFT JOIN dim_product_tier
    ON TRIM(LOWER(add_country_info_to_usage_ping.product_tier)) = TRIM(LOWER(dim_product_tier.product_tier_historical_short))
    AND IFF( add_country_info_to_usage_ping.dim_instance_id = 'ea8bf810-1d6f-4a6a-b4fd-93e8cbd8b57f','SaaS','Self-Managed') = dim_product_tier.product_delivery_type
    AND dim_product_tier.product_tier_name != 'Dedicated - Ultimate'
    --AND main_edition = 'EE'

), joined_payload AS (

    SELECT
      prep_usage_ping_cte.*,
      COALESCE(prep_license_md5.dim_license_id, prep_license_sha256.dim_license_id)                        AS dim_license_id,
      dim_date.date_id                                                                                     AS dim_ping_date_id,
      COALESCE(prep_subscription.dim_subscription_id,license_subscription_id)                              AS dim_subscription_id,
      IFF(prep_usage_ping_cte.ping_created_at < license_trial_ends_on, TRUE, FALSE)                        AS is_trial,
      IFF(COALESCE(prep_license_md5.dim_subscription_id,prep_license_sha256.dim_subscription_id) IS NOT NULL, TRUE, FALSE)
                                                                                                           AS is_license_mapped_to_subscription, -- does the license table have a value in both license_id and subscription_id
      IFF(prep_subscription.dim_subscription_id IS NULL, FALSE, TRUE)                                      AS is_license_subscription_id_valid   -- is the subscription_id in the license table valid (does it exist in the subscription table?)
    FROM prep_usage_ping_cte
    LEFT JOIN prep_license_md5
      ON prep_usage_ping_cte.license_md5    = prep_license.license_md5
    LEFT JOIN prep_license_sha256
      ON prep_usage_ping_cte.license_sha256 = prep_license.license_sha256
    LEFT JOIN prep_subscription
      ON prep_license_md5.dim_subscription_id = prep_subscription.dim_subscription_id
    LEFT JOIN prep_subscription
      ON prep_license_sha256.dim_subscription_id = prep_subscription.dim_subscription_id
    LEFT JOIN dim_date
      ON TO_DATE(prep_usage_ping_cte.ping_created_at) = dim_date.date_day

), flattened_high_level as (
    SELECT
      {{ dbt_utils.surrogate_key(['dim_ping_instance_id', 'joined_payload.metrics_path']) }}                      AS ping_instance_metric_id,
      dim_ping_instance_id                                                                                        AS dim_ping_instance_id,
      joined_payload.metrics_path                                                                                 AS metrics_path,
      metric_value                                                                                                AS metric_value,
      has_timed_out                                                                                               AS has_timed_out,
      dim_product_tier_id                                                                                         AS dim_product_tier_id,
      dim_subscription_id                                                                                         AS dim_subscription_id,
      dim_location_country_id                                                                                     AS dim_location_country_id,
      dim_ping_date_id                                                                                            AS dim_ping_date_id,
      dim_instance_id                                                                                             AS dim_instance_id,
      dim_host_id                                                                                                 AS dim_host_id,
      dim_installation_id                                                                                         AS dim_installation_id,
      dim_license_id                                                                                              AS dim_license_id,
      license_md5                                                                                                 AS license_md5,
      license_sha256                                                                                              AS license_sha256,
      ping_created_at                                                                                             AS ping_created_at,
      ping_created_at::DATE                                                                                       AS ping_created_date,
      umau_value                                                                                                  AS umau_value,
      license_subscription_id                                                                                     AS dim_subscription_license_id,
      is_license_mapped_to_subscription                                                                           AS is_license_mapped_to_subscription,
      is_license_subscription_id_valid                                                                            AS is_license_subscription_id_valid,
      IFF(dim_license_id IS NULL, FALSE, TRUE)                                                                    AS is_service_ping_license_in_customerDot,
      'VERSION_DB'                                                                                                AS data_source
  FROM joined_payload

)

{{ dbt_audit(
    cte_ref="flattened_high_level",
    created_by="@icooper-acp",
    updated_by="@rbacovic",
    created_date="2022-03-08",
    updated_date="2022-12-19"
) }}
