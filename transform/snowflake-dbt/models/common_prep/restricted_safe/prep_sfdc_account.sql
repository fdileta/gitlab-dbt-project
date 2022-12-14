{{ config({
        "materialized": "view",
    })
}}

WITH sfdc_account AS (

    SELECT *
    FROM {{ ref('sfdc_account_source') }}
    WHERE NOT is_deleted

), ultimate_parent_account AS (

    SELECT
      account_id,
      account_name,
      billing_country,
      df_industry,
      account_owner_team,
      account_demographics_territory,
      account_demographics_region,
      account_demographics_area
    FROM sfdc_account
    WHERE account_id = ultimate_parent_account_id

), sfdc_account_with_ultimate_parent AS (

    SELECT
      sfdc_account.account_id                                                               AS dim_crm_account_id,
      ultimate_parent_account.account_id                                                    AS ultimate_parent_account_id,
      {{ sales_segment_cleaning("sfdc_account.ultimate_parent_sales_segment") }}            AS ultimate_parent_sales_segment,
      ultimate_parent_account.billing_country                                               AS ultimate_parent_billing_country,
      ultimate_parent_account.df_industry                                                   AS ultimate_parent_df_industry,
      ultimate_parent_account.account_demographics_territory                                                 AS ultimate_parent_tsp_territory,
      {{ sales_segment_cleaning("sfdc_account.ultimate_parent_sales_segment") }}            AS sales_segment,
      CASE 
        WHEN {{ sales_segment_cleaning("sfdc_account.ultimate_parent_sales_segment") }} IN ('Large', 'PubSec')
          THEN 'Large'
        ELSE {{ sales_segment_cleaning("sfdc_account.ultimate_parent_sales_segment") }}
      END                                                                                   AS sales_segment_grouped,     
      sfdc_account.billing_country,
      sfdc_account.df_industry,
      sfdc_account.account_demographics_territory
    FROM sfdc_account
    LEFT JOIN ultimate_parent_account
      ON sfdc_account.ultimate_parent_account_id = ultimate_parent_account.account_id

), sfdc_account_final AS (

    SELECT
      dim_crm_account_id                                                                                    AS dim_crm_account_id,
      TRIM(account_demographics_territory)                                                                  AS account_territory_clean,
      TRIM(ultimate_parent_tsp_territory)                                                                   AS parent_territory_clean,
      ultimate_parent_account_id                                                                            AS dim_parent_crm_account_id,
      TRIM(SPLIT_PART(df_industry, '-', 1))                                                                 AS account_df_industry_clean,
      TRIM(SPLIT_PART(ultimate_parent_df_industry, '-', 1))                                                 AS parent_df_industry_clean,
      sales_segment                                                                                         AS account_sales_segment_clean,
      sales_segment_grouped                                                                                 AS account_sales_segment_grouped_clean,
      ultimate_parent_sales_segment                                                                         AS parent_sales_segment_clean,
      ultimate_parent_sales_segment                                                                         AS parent_sales_segment_clean,
      TRIM(SPLIT_PART(billing_country, '-', 1))                                                             AS account_billing_country_clean,
      TRIM(SPLIT_PART(ultimate_parent_billing_country, '-', 1))                                             AS parent_billing_country_clean,
      MAX(account_territory_clean) OVER (PARTITION BY UPPER(TRIM(account_territory_clean)))                 AS dim_account_sales_territory_name_source,
      MAX(parent_territory_clean) OVER (PARTITION BY UPPER(TRIM(parent_territory_clean)))                   AS dim_parent_sales_territory_name_source,
      MAX(account_df_industry_clean) OVER (PARTITION BY UPPER(TRIM(account_df_industry_clean)))             AS dim_account_industry_name_source,
      MAX(parent_df_industry_clean) OVER (PARTITION BY UPPER(TRIM(parent_df_industry_clean)))               AS dim_parent_industry_name_source,
      MAX(account_sales_segment_clean) OVER (PARTITION BY UPPER(TRIM(account_sales_segment_clean)))         AS dim_account_sales_segment_name_source,
      MAX(account_sales_segment_grouped_clean) OVER (PARTITION BY UPPER(TRIM(account_sales_segment_grouped_clean)))
                                                                                                            AS dim_account_sales_segment_grouped_source,
      MAX(parent_sales_segment_clean) OVER (PARTITION BY UPPER(TRIM(parent_sales_segment_clean)))           AS dim_parent_sales_segment_name_source,
      MAX(account_billing_country_clean) OVER (PARTITION BY UPPER(TRIM(account_billing_country_clean)))     AS dim_account_location_country_name_source,
      MAX(parent_billing_country_clean) OVER (PARTITION BY UPPER(TRIM(parent_billing_country_clean)))       AS dim_parent_location_country_name_source
    FROM sfdc_account_with_ultimate_parent

)

{{ dbt_audit(
    cte_ref="sfdc_account_final",
    created_by="@paul_armstrong",
    updated_by="@jpeguero",
    created_date="2020-10-30",
    updated_date="2021-04-26"
) }}
