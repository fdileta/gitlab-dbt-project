{{ config(
    tags=["product", "mnpi_exception"]
) }}

{{
    config({
        "materialized": "incremental",
        "unique_key": "dim_usage_ping_id"
    })
}}

WITH usage_pings_with_license AS (

    SELECT
      dim_usage_ping_id,
      dim_product_tier_id,
      ping_created_at,
      ping_created_at_28_days_earlier,
      ping_created_at_year,
      ping_created_at_month,
      ping_created_at_week,
      ping_created_at_date,
      raw_usage_data_id,
      raw_usage_data_payload,
      license_md5,
      license_sha256,
      original_edition,
      edition,
      main_edition,
      product_tier,
      main_edition_product_tier,
      cleaned_version,
      version_is_prerelease,
      major_version,
      minor_version,
      major_minor_version,
      ping_source,
      is_internal,
      is_staging,
      dim_location_country_id,
      license_user_count
    FROM {{ ref('prep_usage_ping') }}
    WHERE (license_md5 IS NOT NULL OR
        license_sha256 IS NOT NULL)

), map_license_subscription_account AS (

    SELECT
      dim_license_id,
      dim_subscription_id,
      is_license_mapped_to_subscription,
      is_license_subscription_id_valid,
      dim_crm_account_id,
      dim_parent_crm_account_id,
      REPLACE(license_md5, '-')    AS license_md5,
      REPLACE(license_sha256, '-') AS license_sha256
    FROM  {{ ref('map_license_subscription_account') }}

), map_license_subscription_account_md5 AS (

    SELECT
      dim_license_id,
      dim_subscription_id,
      is_license_mapped_to_subscription,
      is_license_subscription_id_valid,
      dim_crm_account_id,
      dim_parent_crm_account_id,
      license_md5
    FROM  {{ ref('map_license_subscription_account') }}
    WHERE license_md5 IS NOT NULL

), map_license_subscription_account_sha256 AS (

    SELECT
      dim_license_id,
      dim_subscription_id,
      is_license_mapped_to_subscription,
      is_license_subscription_id_valid,
      dim_crm_account_id,
      dim_parent_crm_account_id,
      license_sha256
    FROM  {{ ref('map_license_subscription_account') }}
    WHERE license_sha256 IS NOT NULL

), final AS (

    SELECT
      usage_pings_with_license.dim_usage_ping_id,
      usage_pings_with_license.dim_product_tier_id,
      usage_pings_with_license.ping_created_at,
      usage_pings_with_license.ping_created_at_28_days_earlier,
      usage_pings_with_license.ping_created_at_year,
      usage_pings_with_license.ping_created_at_month,
      usage_pings_with_license.ping_created_at_week,
      usage_pings_with_license.ping_created_at_date,
      usage_pings_with_license.raw_usage_data_id,
      usage_pings_with_license.raw_usage_data_payload,
      usage_pings_with_license.license_md5,
      usage_pings_with_license.license_sha256,
      usage_pings_with_license.original_edition,
      usage_pings_with_license.edition,
      usage_pings_with_license.main_edition,
      usage_pings_with_license.product_tier,
      usage_pings_with_license.main_edition_product_tier,
      usage_pings_with_license.cleaned_version,
      usage_pings_with_license.version_is_prerelease,
      usage_pings_with_license.major_version,
      usage_pings_with_license.minor_version,
      usage_pings_with_license.major_minor_version,
      usage_pings_with_license.ping_source,
      usage_pings_with_license.is_internal,
      usage_pings_with_license.is_staging,
      usage_pings_with_license.dim_location_country_id,
      usage_pings_with_license.license_user_count,
      COALESCE(map_license_subscription_account_md5.dim_license_id,map_license_subscription_account_sha256.dim_license_id)                                             AS dim_license_id,
      COALESCE(map_license_subscription_account_md5.dim_subscription_id,map_license_subscription_account_sha256.dim_subscription_id)                                   AS dim_subscription_id,
      COALESCE(map_license_subscription_account_md5.is_license_mapped_to_subscription,map_license_subscription_account_sha256.is_license_mapped_to_subscription)       AS is_license_mapped_to_subscription,
      COALESCE(map_license_subscription_account_md5.is_license_subscription_id_valid,map_license_subscription_account_sha256.is_license_subscription_id_valid)         AS is_license_subscription_id_valid,
      COALESCE(map_license_subscription_account_md5.dim_crm_account_id,map_license_subscription_account_sha256.dim_crm_account_id) AS dim_crm_account_id,
      COALESCE(map_license_subscription_account_md5.dim_parent_crm_account_id,map_license_subscription_account_sha256.dim_parent_crm_account_id)                       AS dim_parent_crm_account_id,
      IFF(COALESCE(map_license_subscription_account_md5.dim_license_id,map_license_subscription_account_sha256.dim_license_id)  IS NULL, FALSE, TRUE)                  AS is_usage_ping_license_in_licenseDot
    FROM usage_pings_with_license
    LEFT JOIN map_license_subscription_account_md5
      ON usage_pings_with_license.license_md5 = map_license_subscription_account_md5.license_md5
    LEFT JOIN map_license_subscription_account_sha256
      ON usage_pings_with_license.license_sha256 = map_license_subscription_account_sha256.license_sha256

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@kathleentam",
    updated_by="@rbacovic",
    created_date="2021-01-10",
    updated_date="2022-12-19"
) }}