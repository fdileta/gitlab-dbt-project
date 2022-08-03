{{ config(
    tags=["product", "mnpi_exception"],
    materialized = "incremental",
    unique_key = "ping_instance_id"
) }}

{{ simple_cte([
    ('dim_ping_instance', 'dim_ping_instance'),
    ('dim_product_tier', 'dim_product_tier'),
    ('dim_date', 'dim_date'),
    ('dim_billing_account', 'dim_billing_account'),
    ('dim_crm_accounts', 'dim_crm_account'),
    ('dim_product_detail', 'dim_product_detail'),
    ('fct_charge', 'fct_charge'),
    ('dim_license', 'dim_license'),
    ('dim_hosts', 'dim_hosts'),
    ('dim_location', 'dim_location_country'),
    ('fct_ping_instance', 'fct_ping_instance'),
    ('dim_ping_metric', 'dim_ping_metric')
    ])

}}

, dim_subscription AS (

    SELECT *
    FROM {{ ref('dim_subscription') }}
    WHERE (subscription_name_slugify <> zuora_renewal_subscription_name_slugify[0]::TEXT
      OR zuora_renewal_subscription_name_slugify IS NULL)
      AND subscription_status NOT IN ('Draft', 'Expired')

), fct_ping_instance_metric AS  (

  SELECT
    * FROM fct_ping_instance
    {% if is_incremental() %}
                WHERE ping_created_at >= (SELECT MAX(ping_created_at) FROM {{this}})
    {% endif %}

), subscription_source AS (

    SELECT *
    FROM {{ ref('zuora_subscription_source') }}
    WHERE is_deleted = FALSE
      AND exclude_from_analysis IN ('False', '')

), license_subscriptions AS (

    SELECT DISTINCT
      dim_date.first_day_of_month                                                 AS reporting_month,
      dim_license_id                                                              AS license_id,
      dim_license.license_md5                                                     AS license_md5,
      dim_license.company                                                         AS license_company_name,
      subscription_source.subscription_name_slugify                               AS original_subscription_name_slugify,
      dim_subscription.dim_subscription_id                                        AS latest_subscription_id,
      dim_subscription.subscription_start_date                                    AS subscription_start_date,
      dim_subscription.subscription_end_date                                      AS subscription_end_date,
      dim_subscription.subscription_start_month                                   AS subscription_start_month,
      dim_subscription.subscription_end_month                                     AS subscription_end_month,
      dim_billing_account.dim_billing_account_id                                  AS dim_billing_account_id,
      dim_crm_accounts.crm_account_name                                           AS crm_account_name,
      dim_crm_accounts.dim_parent_crm_account_id                                  AS dim_parent_crm_account_id,
      dim_crm_accounts.parent_crm_account_name                                    AS parent_crm_account_name,
      dim_crm_accounts.parent_crm_account_billing_country                         AS parent_crm_account_billing_country,
      dim_crm_accounts.parent_crm_account_sales_segment                           AS parent_crm_account_sales_segment,
      dim_crm_accounts.parent_crm_account_industry                                AS parent_crm_account_industry,
      dim_crm_accounts.parent_crm_account_owner_team                              AS parent_crm_account_owner_team,
      dim_crm_accounts.parent_crm_account_sales_territory                         AS parent_crm_account_sales_territory,
      dim_crm_accounts.technical_account_manager                                  AS technical_account_manager,
      IFF(MAX(mrr) > 0, TRUE, FALSE)                                              AS is_paid_subscription,
      MAX(IFF(product_rate_plan_name ILIKE ANY ('%edu%', '%oss%'), TRUE, FALSE))  AS is_program_subscription,
      ARRAY_AGG(DISTINCT dim_product_detail.product_tier_name)
        WITHIN GROUP (ORDER BY dim_product_detail.product_tier_name ASC)          AS product_category_array,
      ARRAY_AGG(DISTINCT product_rate_plan_name)
        WITHIN GROUP (ORDER BY product_rate_plan_name ASC)                        AS product_rate_plan_name_array,
      SUM(quantity)                                                               AS quantity,
      SUM(mrr * 12)                                                               AS arr
    FROM dim_license
    INNER JOIN subscription_source
      ON dim_license.dim_subscription_id = subscription_source.subscription_id
    LEFT JOIN dim_subscription
      ON subscription_source.subscription_name_slugify = dim_subscription.subscription_name_slugify
    LEFT JOIN subscription_source AS all_subscriptions
      ON subscription_source.subscription_name_slugify = all_subscriptions.subscription_name_slugify
    INNER JOIN fct_charge
      ON all_subscriptions.subscription_id = fct_charge.dim_subscription_id
        AND charge_type = 'Recurring'
    INNER JOIN dim_product_detail
      ON dim_product_detail.dim_product_detail_id = fct_charge.dim_product_detail_id
      AND dim_product_detail.product_delivery_type = 'Self-Managed'
      AND product_rate_plan_name NOT IN ('Premium - 1 Year - Eval')
    LEFT JOIN dim_billing_account
      ON dim_subscription.dim_billing_account_id = dim_billing_account.dim_billing_account_id
    LEFT JOIN dim_crm_accounts
      ON dim_billing_account.dim_crm_account_id = dim_crm_accounts.dim_crm_account_id
    INNER JOIN dim_date
      ON effective_start_month <= dim_date.date_day AND effective_end_month > dim_date.date_day
    {{ dbt_utils.group_by(n=20)}}


  ), joined AS (

      SELECT
        fct_ping_instance_metric.dim_ping_date_id                                                                                       AS dim_ping_date_id,
        fct_ping_instance_metric.dim_license_id                                                                                         AS dim_license_id,
        fct_ping_instance_metric.dim_installation_id                                                                                    AS dim_installation_id,
        fct_ping_instance_metric.dim_ping_instance_id                                                                                   AS dim_ping_instance_id,
        dim_ping_instance.license_md5                                                                                                   AS license_md5,
        dim_ping_instance.is_trial                                                                                                      AS is_trial,
        fct_ping_instance_metric.umau_value                                                                                             AS umau_value,
        license_subscriptions.license_id                                                                                                AS license_id,
        license_subscriptions.license_company_name                                                                                      AS license_company_name,
        license_subscriptions.latest_subscription_id                                                                                    AS latest_subscription_id,
        license_subscriptions.original_subscription_name_slugify                                                                        AS original_subscription_name_slugify,
        license_subscriptions.product_category_array                                                                                    AS product_category_array,
        license_subscriptions.product_rate_plan_name_array                                                                              AS product_rate_plan_name_array,
        license_subscriptions.subscription_start_month                                                                                  AS subscription_start_month,
        license_subscriptions.subscription_end_month                                                                                    AS subscription_end_month,
        license_subscriptions.dim_billing_account_id                                                                                    AS dim_billing_account_id,
        license_subscriptions.crm_account_name                                                                                          AS crm_account_name,
        license_subscriptions.dim_parent_crm_account_id                                                                                 AS dim_parent_crm_account_id,
        license_subscriptions.parent_crm_account_name                                                                                   AS parent_crm_account_name,
        license_subscriptions.parent_crm_account_billing_country                                                                        AS parent_crm_account_billing_country,
        license_subscriptions.parent_crm_account_sales_segment                                                                          AS parent_crm_account_sales_segment,
        license_subscriptions.parent_crm_account_industry                                                                               AS parent_crm_account_industry,
        license_subscriptions.parent_crm_account_owner_team                                                                             AS parent_crm_account_owner_team,
        license_subscriptions.parent_crm_account_sales_territory                                                                        AS parent_crm_account_sales_territory,
        license_subscriptions.technical_account_manager                                                                                 AS technical_account_manager,
        COALESCE(is_paid_subscription, FALSE)                                                                                           AS is_paid_subscription,
        COALESCE(is_program_subscription, FALSE)                                                                                        AS is_program_subscription,
        dim_ping_instance.ping_delivery_type                                                                                            AS ping_delivery_type,
        dim_ping_instance.ping_edition                                                                                                  AS ping_edition,
        dim_ping_instance.product_tier                                                                                                  AS ping_product_tier,
        dim_ping_instance.ping_edition || ' - ' || dim_ping_instance.product_tier                                                       AS ping_edition_product_tier,
        dim_ping_instance.major_version                                                                                                 AS major_version,
        dim_ping_instance.minor_version                                                                                                 AS minor_version,
        dim_ping_instance.major_minor_version                                                                                           AS major_minor_version,
        dim_ping_instance.major_minor_version_id                                                                                        AS major_minor_version_id,
        dim_ping_instance.version_is_prerelease                                                                                         AS version_is_prerelease,
        dim_ping_instance.is_internal                                                                                                   AS is_internal,
        dim_ping_instance.is_staging                                                                                                    AS is_staging,
        dim_ping_instance.instance_user_count                                                                                           AS instance_user_count,
        dim_ping_instance.ping_created_at                                                                                               AS ping_created_at,
        dim_date.first_day_of_month                                                                                                     AS ping_created_date_month,
        fct_ping_instance_metric.dim_host_id                                                                                            AS dim_host_id,
        fct_ping_instance_metric.dim_instance_id                                                                                        AS dim_instance_id,
        dim_ping_instance.host_name                                                                                                     AS host_name,
        dim_ping_instance.is_last_ping_of_month                                                                                         AS is_last_ping_of_month,
        fct_ping_instance_metric.dim_location_country_id                                                                                AS dim_location_country_id,
        dim_location.country_name                                                                                                       AS country_name,
        dim_location.iso_2_country_code                                                                                                 AS iso_2_country_code
      FROM fct_ping_instance_metric
      INNER JOIN dim_date
        ON fct_ping_instance_metric.dim_ping_date_id = dim_date.date_id
      LEFT JOIN dim_ping_instance
        ON fct_ping_instance_metric.dim_ping_instance_id = dim_ping_instance.dim_ping_instance_id
      LEFT JOIN dim_hosts
        ON dim_ping_instance.dim_host_id = dim_hosts.host_id
          AND dim_ping_instance.ip_address_hash = dim_hosts.source_ip_hash
          AND dim_ping_instance.dim_instance_id = dim_hosts.instance_id
      LEFT JOIN license_subscriptions
        ON dim_ping_instance.license_md5 = license_subscriptions.license_md5
          AND dim_date.first_day_of_month = license_subscriptions.reporting_month
      LEFT JOIN dim_location
        ON fct_ping_instance_metric.dim_location_country_id = dim_location.dim_location_country_id
      WHERE ping_delivery_type = 'Self-Managed'
        OR (ping_delivery_type = 'SaaS' AND fct_ping_instance_metric.dim_installation_id = '8b52effca410f0a380b0fcffaa1260e7')

), sorted AS (

    SELECT

      -- Primary Key
      {{ dbt_utils.surrogate_key(['dim_ping_instance_id']) }} AS ping_instance_id,
      dim_ping_date_id,
      dim_ping_instance_id,

      --Foreign Key
      dim_instance_id,
      dim_license_id,
      dim_installation_id,
      latest_subscription_id,
      dim_billing_account_id,
      dim_parent_crm_account_id,
      major_minor_version_id,
      dim_host_id,
      host_name,
      -- metadata usage ping
      ping_delivery_type,
      ping_edition,
      ping_product_tier,
      ping_edition_product_tier,
      major_version,
      minor_version,
      major_minor_version,
      version_is_prerelease,
      is_internal,
      is_staging,
      is_trial,
      umau_value,

      --metadata instance
      instance_user_count,

      --metadata subscription
      original_subscription_name_slugify,
      subscription_start_month,
      subscription_end_month,
      product_category_array,
      product_rate_plan_name_array,
      is_paid_subscription,
      is_program_subscription,

      -- account metadata
      crm_account_name,
      parent_crm_account_name,
      parent_crm_account_billing_country,
      parent_crm_account_sales_segment,
      parent_crm_account_industry,
      parent_crm_account_owner_team,
      parent_crm_account_sales_territory,
      technical_account_manager,

      ping_created_at,
      ping_created_date_month,
      is_last_ping_of_month


    FROM joined

)

{{ dbt_audit(
    cte_ref="sorted",
    created_by="@icooper-acp",
    updated_by="@snalamaru",
    created_date="2022-03-11",
    updated_date="2022-05-20"
) }}
