{{ config({
    "materialized": "incremental",
    "unique_key" : "source_primary_key"
    })
}}

WITH source AS (

    SELECT *
    FROM {{ ref('test_gcp_billing_source') }}
    {% if is_incremental() %}

    WHERE uploaded_at >= (SELECT MAX(uploaded_at) FROM {{this}})

    {% endif %}

), credits AS (

    SELECT
        source_primary_key                               AS source_primary_key,
        SUM(IFNULL(credit_amount,0))                     AS total_credits
    FROM {{ ref('test_gcp_billing_export_credits') }}
    GROUP BY 1

), renamed as (

    SELECT
        source.primary_key                                   AS source_primary_key,
        source.billing_account_id                            AS billing_account_id,
        source.service_id                                    AS service_id,
        source.service_description                           AS service_description,
        source.sku_id                                        AS sku_id,
        source.sku_description                               AS sku_description,
        source.invoice_month                                 AS invoice_month,
        source.usage_start_time                              AS usage_start_time,
        source.usage_end_time                                AS usage_end_time,
        source.project_id                                    AS project_id,
        source.project_name                                  AS project_name,
        source.project_labels                                AS project_labels,
        source.folder_id                                     AS folder_id,
        source.resource_location                             AS resource_location,
        source.resource_zone                                 AS resource_zone,
        source.resource_region                               AS resource_region,
        source.resource_country                              AS resource_country,
        source.labels                                        AS resource_labels,
        source.system_labels                                 AS system_labels,
        source.cost                                          AS cost_before_credits,
        credits.total_credits                                AS total_credits,
        source.cost + IFNULL(credits.total_credits, 0)       AS total_cost,
        source.usage_amount                                  AS usage_amount,
        source.usage_unit                                    AS usage_unit,
        source.usage_amount_in_pricing_units                 AS usage_amount_in_pricing_units,
        source.pricing_unit                                  AS pricing_unit,
        source.currency                                      AS currency,
        source.currency_conversion_rate                      AS currency_conversion_rate,
        source.cost_type                                     AS cost_type,
        source.credits                                       AS credits,
        source.export_time                                   AS export_time,
        source.uploaded_at                                   AS uploaded_at
    FROM source
    LEFT JOIN credits
    ON source.primary_key = credits.source_primary_key

)

SELECT *
FROM renamed
