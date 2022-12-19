{{ config(
    tags=["mnpi_exception"]
) }}

{{config({
    "materialized": "incremental",
    "unique_key": "dim_usage_ping_id"
})}}

WITH map_license_account AS (

    SELECT
      license_md5,
      license_sha256,
      dim_subscription_id,
      dim_crm_account_id,
      dim_parent_crm_account_id
    FROM {{ ref('map_license_subscription_account') }}

), usage_pings AS (

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
      dim_location_country_id
    FROM {{ ref('prep_usage_ping') }}

), instance_types AS (

    SELECT
      instance_type,
      instance_uuid,
      instance_hostname
    FROM {{ ref('dim_host_instance_type') }}

), map_license_account_md5 AS (

    SELECT
      license_md5,
      dim_subscription_id,
      dim_crm_account_id,
      dim_parent_crm_account_id
    FROM map_license_account
    WHERE license_md5 IS NOT NULL

), map_license_account_sha256 AS (

    SELECT
      license_sha256,
      dim_subscription_id,
      dim_crm_account_id,
      dim_parent_crm_account_id
    FROM map_license_account
    WHERE license_sha256 IS NOT NULL

), core_usage_pings AS (

    SELECT
      usage_pings.dim_usage_ping_id,
      usage_pings.dim_product_tier_id,
      usage_pings.ping_created_at,
      usage_pings.ping_created_at_28_days_earlier,
      usage_pings.ping_created_at_year,
      usage_pings.ping_created_at_month,
      usage_pings.ping_created_at_week,
      usage_pings.ping_created_at_date,
      usage_pings.raw_usage_data_id,
      usage_pings.raw_usage_data_payload,
      usage_pings.license_md5,
      usage_pings.license_sha256,
      usage_pings.original_edition,
      usage_pings.edition,
      usage_pings.main_edition,
      usage_pings.product_tier,
      usage_pings.main_edition_product_tier,
      usage_pings.cleaned_version,
      usage_pings.version_is_prerelease,
      usage_pings.major_version,
      usage_pings.minor_version,
      usage_pings.major_minor_version,
      usage_pings.ping_source,
      usage_pings.is_internal,
      usage_pings.is_staging,
      usage_pings.dim_location_country_id,
      COALESCE(map_license_account_md5.dim_subscription_id, map_license_account_sha256.dim_subscription_id)             AS dim_subscription_id,
      COALESCE(map_license_account_md5.dim_crm_account_id, map_license_account_sha256.dim_crm_account_id)               AS dim_crm_account_id,
      COALESCE(map_license_account_md5.dim_parent_crm_account_id, map_license_account_sha256.dim_parent_crm_account_id) AS dim_parent_crm_account_id
    FROM usage_pings
    LEFT JOIN map_license_account_md5
    ON usage_pings.license_md5 = map_license_account_md5.license_md5
    LEFT JOIN map_license_account_sha256
    ON usage_pings.license_sha256 = map_license_account_sha256.license_sha256
    WHERE usage_pings.product_tier = 'Core'

), joined AS (

    SELECT

    {{ default_usage_ping_information() }}

    instance_types.instance_type,
    core_usage_pings.dim_subscription_id,
    core_usage_pings.dim_crm_account_id,
    core_usage_pings.dim_parent_crm_account_id,
    core_usage_pings.dim_location_country_id,

    {{ sales_wave_2_3_metrics() }}

    FROM core_usage_pings
    LEFT JOIN instance_types
      ON core_usage_pings.raw_usage_data_payload['uuid']::VARCHAR = instance_types.instance_uuid
      AND core_usage_pings.raw_usage_data_payload['hostname']::VARCHAR = instance_types.instance_hostname
    QUALIFY ROW_NUMBER() OVER (
      PARTITION BY dim_usage_ping_id
        ORDER BY ping_created_at DESC
      ) = 1
)

{{ dbt_audit(
    cte_ref="joined",
    created_by="@ischweickartDD",
    updated_by="@rbacovic",
    created_date="2021-06-04",
    updated_date="2022-12-19"
) }}
