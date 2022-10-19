{{ config(
    materialized='incremental',
    unique_key='primary_key',
    on_schema_change='append_new_columns',
    full_refresh=only_force_full_refresh()
    )
}}

WITH
source AS (
  
  SELECT
    parse_json(value) AS value
  FROM {{ source('gcp_billing','summary_gcp_billing') }}
  {% if is_incremental() %}

  WHERE TO_TIMESTAMP(value['gcs_export_time']::INT, 6) > (SELECT MAX(uploaded_at) FROM {{ this }})

  {% endif %}

),

grouped AS (
  
  SELECT
    value,
    count(*) AS occurrence_multiplier
  FROM source
  GROUP BY 1
),

renamed AS (
  
  SELECT
    {{ dbt_utils.surrogate_key(['value']) }} as primary_key,
    value['billing_account_id']::VARCHAR AS billing_account_id,
    value['cost']::FLOAT * occurrence_multiplier AS cost,
    value['cost_type']::VARCHAR AS cost_type,
    value['credits']::VARIANT AS credits,
    value['currency']::VARCHAR AS currency,
    value['currency_conversion_rate']::FLOAT AS currency_conversion_rate,
    TO_TIMESTAMP(value['export_time']::INT, 6) AS export_time,
    TO_DATE(value['invoice']['month']::STRING, 'YYYYMM') AS invoice_month,
    value['labels']::VARIANT AS labels,
    value['location']['country']::VARCHAR AS resource_country,
    value['location']['location']::VARCHAR AS resource_location,
    value['location']['region']::VARCHAR AS resource_region,
    value['location']['zone']::VARCHAR AS resource_zone,
    value['project']['ancestors']::VARIANT AS project_ancestors,
    value['project']['ancestry_numbers']::VARCHAR AS folder_id,
    value['project']['id']::VARCHAR AS project_id,
    value['project']['labels']::VARIANT AS project_labels,
    value['project']['name']::VARCHAR AS project_name,
    value['service']['id']::VARCHAR AS service_id,
    value['service']['description']::VARCHAR AS service_description,
    value['sku']['id']::VARCHAR AS sku_id,
    value['sku']['description']::VARCHAR AS sku_description,
    value['system_labels']::VARIANT AS system_labels,
    value['usage']['pricing_unit']::VARCHAR AS pricing_unit,
    value['usage']['amount']::FLOAT * occurrence_multiplier AS usage_amount,
    value['usage']['amount_in_pricing_units']::FLOAT * occurrence_multiplier AS usage_amount_in_pricing_units,
    value['usage']['unit']::VARCHAR AS usage_unit,
    TO_TIMESTAMP(value['usage_start_time']::INT, 6) AS usage_start_time,
    TO_TIMESTAMP(value['usage_end_time']::INT, 6) AS usage_end_time,
    value['_partition_date']::DATE AS partition_date,
    TO_TIMESTAMP(value['gcs_export_time']::INT, 6) AS uploaded_at,
    occurrence_multiplier
  
  
  FROM grouped


)

SELECT *
FROM renamed