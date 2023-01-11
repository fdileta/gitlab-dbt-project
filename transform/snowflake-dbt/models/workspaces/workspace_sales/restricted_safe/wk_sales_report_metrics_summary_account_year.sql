{{ config(alias='report_metrics_summary_account_year') }}

WITH date_details AS (

    SELECT *
    --FROM  prod.workspace_sales.date_details
    FROM {{ ref('wk_sales_date_details') }}

 ), sfdc_opportunity_xf AS (

    SELECT *
    --FROM prod.restricted_safe_workspace_sales.sfdc_opportunity_xf
    FROM {{ref('wk_sales_sfdc_opportunity_xf')}}
    WHERE is_deleted = 0
      AND is_edu_oss = 0
      AND is_jihu_account = 0

 ), sfdc_opportunity_snapshot_xf AS (

    SELECT h.*
    --FROM prod.restricted_safe_workspace_sales.sfdc_opportunity_snapshot_history_xf AS h
    FROM {{ref('wk_sales_sfdc_opportunity_snapshot_history_xf')}} h
    INNER JOIN date_details snapshot_date
      ON snapshot_date.date_actual = h.snapshot_date
    WHERE h.is_deleted = 0
      AND h.is_edu_oss = 0
      AND h.is_jihu_account = 0
      -- same day of FY across years
      AND snapshot_date.day_of_fiscal_year_normalised = (SELECT DISTINCT day_of_fiscal_year_normalised
                                                          FROM date_details
                                                          WHERE date_actual = DATEADD(day, -2, CURRENT_DATE))
 ), mart_available_to_renew AS (

    SELECT renew.*,
        renew_date.first_day_of_fiscal_quarter  AS renew_fiscal_quarter_date,
        renew_date.fiscal_quarter_name_fy       AS renew_fiscal_quarter_name,
        renew_date.fiscal_quarter               AS renew_fiscal_quarter_number
    FROM {{ref('mart_available_to_renew')}} renew
    --FROM prod.restricted_safe_common_mart_finance.mart_available_to_renew
    LEFT JOIN date_details renew_date
        ON renew_date.date_actual = renew.renewal_month
    
 ), dim_subscription AS (

    SELECT
      dim_subscription_id,
      CASE
          WHEN dim_billing_account_id_invoice_owner_account != dim_billing_account_id
              THEN 1
          ELSE 0
      END AS is_channel_arr_flag
    --FROM prod.common.dim_subscription
    FROM {{ ref('dim_subscription') }}

 ), mart_arr AS (

    SELECT *
    --FROM prod.restricted_safe_common_mart_sales.mart_arr
    FROM {{ref('mart_arr')}}

     

  ), raw_account AS (
  
    SELECT *
    FROM {{ source('salesforce', 'account') }}
    --FROM raw.salesforce_stitch.account 

      
  -- missing fields in mart crm account so adding dim_crm_account cte here on top of the mart below
 
  ), dim_crm_account AS (

    SELECT *
    --FROM prod.restricted_safe_common.dim_crm_account
    FROM {{ref('dim_crm_account')}}

    -- missing fields in dim_crm_account so adding raw account here
  -- has_tam__c
  -- PUBLIC_SECTOR_ACCOUNT__C,
  -- PUBSEC_TYPE__C,
  -- POTENTIAL_ARR_LAM__C
  -- BILLINGSTATE
  ), mart_crm_account AS (

    SELECT acc.*,
        raw.has_tam__c                AS has_tam_flag,
        raw.public_sector_account__c  AS public_sector_account_flag,
        raw.pubsec_type__c            AS pubsec_type,
        raw.lam_tier__c               AS potential_lam_arr,
        raw.billingstatecode          AS account_billing_state
    --FROM prod.restricted_safe_common_mart_sales.mart_crm_account acc
    FROM {{ref('mart_crm_account')}} acc
    LEFT JOIN raw_account raw
      ON raw.id = acc.dim_crm_account_id
    
  ), sfdc_accounts_xf AS (

    SELECT *
    --FROM prod.restricted_safe_legacy.sfdc_accounts_xf
    FROM {{ref('sfdc_accounts_xf')}}

  ), sfdc_users_xf AS (

    SELECT *,
       CASE
            WHEN lower(title) like '%strategic account%'
                OR lower(title) like '%account executive%'
                OR lower(title) like '%country manager%'
                OR lower(title) like '%public sector channel manager%'
                THEN 'Rep'
            WHEN lower(title) like '%area sales manager%'
                THEN 'ASM'
            ELSE 'Other'
      END                                       AS title_category
    --FROM prod.workspace_sales.sfdc_users_xf
    FROM {{ref('wk_sales_sfdc_users_xf')}}

  ), report_dates AS (

    SELECT DISTINCT fiscal_year             AS report_fiscal_year,
                    first_day_of_month      AS report_month_date
    FROM date_details
    WHERE fiscal_year IN (2023,2022)
        AND month_actual = month(CURRENT_DATE)

  ), account_year_key AS (

    SELECT DISTINCT
      a.dim_crm_account_id AS account_id,
      d.report_fiscal_year,
      d.report_month_date
  FROM dim_crm_account AS a
  CROSS JOIN report_dates AS d

  ), nfy_atr_base AS (

    SELECT 
        dim_crm_account_id      AS account_id,
        report_dates.report_fiscal_year,
        SUM(arr)                AS nfy_atr,
         SUM(CASE
          WHEN atr.renew_fiscal_quarter_number = 1
            THEN arr
          ELSE 0
        END)                    AS nfy_q1_atr,
        SUM(CASE
          WHEN atr.renew_fiscal_quarter_number = 2
            THEN arr
          ELSE 0
        END)                    AS nfy_q2_atr,
        SUM(CASE
          WHEN atr.renew_fiscal_quarter_number = 3
            THEN arr
          ELSE 0
        END)                    AS nfy_q3_atr,
        SUM(CASE
          WHEN atr.renew_fiscal_quarter_number = 4
            THEN arr
          ELSE 0
        END)                    AS nfy_q4_atr
    FROM mart_available_to_renew atr
    CROSS JOIN report_dates
    WHERE is_available_to_renew = 1
    AND atr.fiscal_year = report_dates.report_fiscal_year + 1
    GROUP BY 1,2
    
), last_12m_atr_base AS (

    SELECT dim_crm_account_id   AS account_id,
        report_dates.report_fiscal_year,
        COUNT(DISTINCT atr.renewal_month) AS count_unique_months,
    
        SUM(arr)                AS last_12m_atr
    FROM mart_available_to_renew atr
    CROSS JOIN report_dates
    WHERE is_available_to_renew = 1
    --AND renewal_type = 'Non-MYB'
    AND atr.renewal_month < report_dates.report_month_date
    AND atr.renewal_month >= DATEADD(month,-12,report_dates.report_month_date)
    GROUP BY 1,2
    
), fy_atr_base AS (

    SELECT dim_crm_account_id   AS account_id,
        report_dates.report_fiscal_year,
        COUNT(DISTINCT atr.renewal_month) AS count_unique_months,
        SUM(arr)                AS fy_atr
    FROM mart_available_to_renew atr
    CROSS JOIN report_dates
    WHERE is_available_to_renew = 1
    --AND renewal_type = 'Non-MYB'
    AND atr.fiscal_year = report_dates.report_fiscal_year
    GROUP BY 1,2


-- Rolling 1 year Net ARR
), net_arr_last_12m AS (
  -- net_arr_ttm

    SELECT
      o.account_id,
      d.report_fiscal_year          AS report_fiscal_year,
      SUM(o.net_arr)                AS last_12m_booked_net_arr,   -- ttm_net_arr
      SUM(CASE
            WHEN  o.sales_qualified_source != 'Web Direct Generated'
              THEN o.net_arr
            ELSE 0
          END)          AS last_12m_booked_non_web_net_arr,  -- ttm_non_web_net_arr
      SUM(CASE
            WHEN o.sales_qualified_source = 'Web Direct Generated'
            THEN o.net_arr
            ELSE 0 END) AS last_12m_booked_web_direct_sourced_net_arr,  --ttm_web_direct_sourced_net_arr
      SUM(CASE
            WHEN o.sales_qualified_source = 'Channel Generated'
            THEN o.net_arr
            ELSE 0 END) AS last_12m_booked_channel_sourced_net_arr,  -- ttm_web_direct_sourced_net_arr
      SUM(CASE
            WHEN o.sales_qualified_source = 'SDR Generated'
            THEN o.net_arr
            ELSE 0 END) AS last_12m_booked_sdr_sourced_net_arr,  -- ttm_sdr_sourced_net_arr
      SUM(CASE
            WHEN o.sales_qualified_source = 'AE Generated'
            THEN o.net_arr
            ELSE 0 END) AS last_12m_booked_ae_sourced_net_arr,  -- ttm_ae_sourced_net_arr
      SUM(CASE
            WHEN o.is_eligible_churn_contraction_flag = 1
               THEN o.net_arr
            ELSE 0 END) AS last_12m_booked_churn_contraction_net_arr,  -- ttm_churn_contraction_net_arr

       -- FO year
        SUM(CASE
            WHEN o.order_type_stamped = '1. New - First Order'
            THEN o.net_arr
            ELSE 0 END) AS last_12m_booked_fo_net_arr,  -- ttm_fo_net_arr

        -- New Connected year
        SUM(CASE
            WHEN o.order_type_stamped = '2. New - Connected'
            THEN o.net_arr
            ELSE 0 END) AS last_12m_booked_new_connected_net_arr, -- ttm_new_connected_net_arr

        -- Growth year
        SUM(CASE
            WHEN o.order_type_stamped NOT IN ('2. New - Connected','1. New - First Order')
            THEN o.net_arr
            ELSE 0 END) AS last_12m_booked_growth_net_arr,   --ttm_growth_net_arr

        -- deal path direct year
        SUM(CASE
            WHEN o.deal_path != 'Channel'
            THEN o.net_arr
            ELSE 0 END) AS last_12m_booked_direct_net_arr,   --ttm_direct_net_arr

        -- deal path channel year
        SUM(CASE
            WHEN o.deal_path = 'Channel'
            THEN o.net_arr
            ELSE 0 END) AS last_12m_booked_channel_net_arr,   --ttm_channel_net_arr

        SUM (CASE
            WHEN o.is_won = 1
            THEN o.calculated_deal_count
            ELSE 0 END )   AS last_12m_booked_deal_count,  --ttm_deal_count

         SUM (CASE
            WHEN (o.is_won = 1
                  OR (o.is_renewal = 1 AND o.is_lost = 1))
            THEN o.calculated_deal_count
            ELSE 0 END )   AS last_12m_booked_trx_count,  -- ttm_trx_count

          SUM (CASE
            WHEN (o.is_won = 1
                  OR (o.is_renewal = 1 AND o.is_lost = 1))
                AND ((o.is_renewal = 1 AND o.arr_basis > 5000)
                        OR o.net_arr > 5000)
            THEN o.calculated_deal_count
            ELSE 0 END )   AS last_12m_booked_trx_over_5k_count,   -- ttm_trx_over_5k_count

          SUM (CASE
            WHEN (o.is_won = 1
                  OR (o.is_renewal = 1 AND o.is_lost = 1))
                AND ((o.is_renewal = 1 AND o.arr_basis > 10000)
                        OR o.net_arr > 10000)
            THEN o.calculated_deal_count
            ELSE 0 END )   AS last_12m_booked_trx_over_10k_count,  -- ttm_trx_over_10k_count

          SUM (CASE
            WHEN (o.is_won = 1
                  OR (o.is_renewal = 1 AND o.is_lost = 1))
                AND ((o.is_renewal = 1 AND o.arr_basis > 50000)
                        OR o.net_arr > 50000)
            THEN o.calculated_deal_count
            ELSE 0 END )   AS last_12m_booked_trx_over_50k_count,  -- ttm_trx_over_50k_count

          SUM (CASE
            WHEN o.is_renewal = 1
            THEN o.calculated_deal_count
            ELSE 0 END )   AS last_12m_booked_renewal_deal_count,   -- ttm_renewal_deal_count

        SUM(CASE
            WHEN o.is_eligible_churn_contraction_flag = 1
                AND o.opportunity_category IN ('Standard','Internal Correction','Ramp Deal','Contract Reset','Contract Reset/Ramp Deal')
            THEN o.calculated_deal_count
            ELSE 0 END) AS last_12m_booked_churn_contraction_deal_count,  -- ttm_churn_contraction_deal_count

          -- deal path direct year
        SUM(CASE
            WHEN o.deal_path != 'Channel'
                AND o.is_won = 1
            THEN o.calculated_deal_count
            ELSE 0 END) AS last_12m_booked_direct_deal_count,  -- ttm_direct_deal_count

        -- deal path channel year
        SUM(CASE
            WHEN o.deal_path = 'Channel'
                AND o.is_won = 1
            THEN o.calculated_deal_count
            ELSE 0 END) AS last_12m_booked_channel_deal_count  -- ttm_channel_deal_count

    FROM sfdc_opportunity_xf AS o
    CROSS JOIN report_dates AS d
    WHERE o.close_date BETWEEN DATEADD(month, -12,DATE_TRUNC('month',d.report_month_date)) and DATE_TRUNC('month',d.report_month_date)
        AND o.booked_net_arr <> 0
    GROUP BY 1, 2
    
  -- total booked net arr in fy
  ), fy_net_arr AS (

    SELECT
      o.account_id,
      o.close_fiscal_year   AS report_fiscal_year,
      SUM(o.booked_net_arr) AS fy_booked_net_arr,
      SUM(CASE
            WHEN  o.sales_qualified_source != 'Web Direct Generated'
              THEN o.booked_net_arr
            ELSE 0
          END)          AS fy_booked_non_web_booked_net_arr,
      SUM(CASE
            WHEN o.sales_qualified_source = 'Web Direct Generated'
            THEN o.booked_net_arr
            ELSE 0 END) AS fy_booked_web_direct_sourced_net_arr,
      SUM(CASE
            WHEN o.sales_qualified_source = 'Channel Generated'
            THEN o.booked_net_arr
            ELSE 0 END) AS fy_booked_channel_sourced_net_arr,
      SUM(CASE
            WHEN o.sales_qualified_source = 'SDR Generated'
            THEN o.booked_net_arr
            ELSE 0 END) AS fy_booked_sdr_sourced_net_arr,
      SUM(CASE
            WHEN o.sales_qualified_source = 'AE Generated'
            THEN o.booked_net_arr
            ELSE 0 END) AS fy_booked_ae_sourced_net_arr,
      SUM(CASE
            WHEN o.is_eligible_churn_contraction_flag = 1
            THEN o.booked_net_arr
            ELSE 0 END) AS fy_booked_churn_contraction_net_arr,

        -- First Order year
        SUM(CASE
            WHEN o.order_type_stamped = '1. New - First Order'
            THEN o.booked_net_arr
            ELSE 0 END) AS fy_booked_fo_net_arr,

        -- New Connected year
        SUM(CASE
            WHEN o.order_type_stamped = '2. New - Connected'
            THEN o.booked_net_arr
            ELSE 0 END) AS fy_booked_new_connected_net_arr,

        -- Growth year
        SUM(CASE
            WHEN o.order_type_stamped NOT IN ('2. New - Connected','1. New - First Order')
            THEN o.booked_net_arr
            ELSE 0 END) AS fy_booked_growth_net_arr,

        SUM(o.calculated_deal_count)   AS fy_booked_deal_count,

        -- deal path direct year
        SUM(CASE
            WHEN o.deal_path != 'Channel'
            THEN o.booked_net_arr
            ELSE 0 END) AS fy_booked_direct_net_arr,

        -- deal path channel year
        SUM(CASE
            WHEN o.deal_path = 'Channel'
            THEN o.booked_net_arr
            ELSE 0 END) AS fy_booked_channel_net_arr,

         -- deal path direct year
        SUM(CASE
            WHEN o.deal_path != 'Channel'
            THEN o.calculated_deal_count
            ELSE 0 END) AS fy_booked_direct_deal_count,

        -- deal path channel year
        SUM(CASE
            WHEN o.deal_path = 'Channel'
            THEN o.calculated_deal_count
            ELSE 0 END) AS fy_booked_channel_deal_count

    FROM sfdc_opportunity_xf AS o
    WHERE o.booked_net_arr <> 0
    GROUP BY 1,2

  -- Total open pipeline at the same point in previous fiscal years (total open pipe)
  ), op_forward_one_year AS (

    SELECT
      h.account_id,
      h.snapshot_fiscal_year        AS report_fiscal_year,
      -- net arr pipeline
      SUM(h.net_arr)                AS total_open_pipe,
      SUM(CASE 
              WHEN h.close_fiscal_year = h.snapshot_fiscal_year + 1
                  THEN h.net_arr
              ELSE 0
          END)                      AS nfy_open_pipeline,
      SUM(CASE 
              WHEN h.close_fiscal_year = h.snapshot_fiscal_year
                  THEN h.net_arr
              ELSE 0
          END)                       AS fy_open_pipeline,
      
      -- deal count pipeline
      SUM(h.calculated_deal_count)   AS total_count_open_deals,
      SUM(CASE 
              WHEN h.close_fiscal_year = h.snapshot_fiscal_year + 1
                  THEN h.net_arr
              ELSE 0
          END)                      AS nfy_count_open_deals,
      SUM(CASE 
              WHEN h.close_fiscal_year = h.snapshot_fiscal_year
                  THEN h.net_arr
              ELSE 0
          END)                       AS fy_count_open_deals
      
    FROM sfdc_opportunity_snapshot_xf AS h
    WHERE h.close_date > h.snapshot_date
      AND h.forecast_category_name NOT IN  ('Omitted','Closed')
      AND h.order_type_stamped != '7. PS / Other'
      AND h.net_arr != 0
      AND h.is_eligible_open_pipeline_flag = 1
      GROUP BY 1,2

  -- Last 12 months pipe gen at same point of time in the year
  ), pg_last_12_months AS (

    SELECT
      h.account_id,
      h.snapshot_fiscal_year AS report_fiscal_year,
      SUM(h.net_arr)                 AS pg_last_12m_net_arr,
      SUM(CASE
            WHEN h.sales_qualified_source = 'Web Direct Generated'
            THEN h.net_arr
            ELSE 0 END)              AS pg_last_12m_web_direct_sourced_net_arr,
      SUM(CASE
            WHEN h.sales_qualified_source = 'Channel Generated'
            THEN h.net_arr
            ELSE 0 END)              AS pg_last_12m_channel_sourced_net_arr,
      SUM(CASE
            WHEN h.sales_qualified_source = 'SDR Generated'
            THEN h.net_arr
            ELSE 0 END)              AS pg_last_12m_sdr_sourced_net_arr,
      SUM(CASE
            WHEN h.sales_qualified_source = 'AE Generated'
            THEN h.net_arr
            ELSE 0 END)              AS pg_last_12m_ae_sourced_net_arr,

      SUM(CASE
            WHEN h.sales_qualified_source = 'Web Direct Generated'
            THEN h.calculated_deal_count
            ELSE 0 END)              AS pg_last_12m_web_direct_sourced_deal_count,
      SUM(CASE
            WHEN h.sales_qualified_source = 'Channel Generated'
            THEN h.calculated_deal_count
            ELSE 0 END)              AS pg_last_12m_channel_sourced_deal_count,
      SUM(CASE
            WHEN h.sales_qualified_source = 'SDR Generated'
            THEN h.calculated_deal_count
            ELSE 0 END)              AS pg_last_12m_sdr_sourced_deal_count,
      SUM(CASE
            WHEN h.sales_qualified_source = 'AE Generated'
            THEN h.calculated_deal_count
            ELSE 0 END)              AS pg_last_12m_ae_sourced_deal_count

    FROM sfdc_opportunity_snapshot_xf AS h

    -- pipeline created within the last 12 months
    WHERE
        h.pipeline_created_date > dateadd(month,-12,h.snapshot_date)
      AND h.pipeline_created_date <= h.snapshot_date
      AND h.order_type_stamped != '7. PS / Other'
      AND h.is_eligible_created_pipeline_flag = 1
    GROUP BY 1,2

  -- Pipe generation at the same point in time in the fiscal year
  ), pg_ytd AS (

    SELECT
      h.account_id,
      h.net_arr_created_fiscal_year  AS report_fiscal_year,
      SUM(h.net_arr)                 AS pg_ytd_net_arr,
      SUM(CASE
            WHEN h.sales_qualified_source = 'Web Direct Generated'
            THEN h.net_arr
            ELSE 0 END) AS pg_ytd_web_direct_sourced_net_arr,
      SUM(CASE
            WHEN h.sales_qualified_source = 'Channel Generated'
            THEN h.net_arr
            ELSE 0 END) AS pg_ytd_channel_sourced_net_arr,
      SUM(CASE
            WHEN h.sales_qualified_source = 'SDR Generated'
            THEN h.net_arr
            ELSE 0 END) AS pg_ytd_sdr_sourced_net_arr,
      SUM(CASE
            WHEN h.sales_qualified_source = 'AE Generated'
            THEN h.net_arr
            ELSE 0 END) AS pg_ytd_ae_sourced_net_arr
    FROM sfdc_opportunity_snapshot_xf AS h
      -- pipeline created within the fiscal year
    WHERE h.snapshot_fiscal_year = h.net_arr_created_fiscal_year
      AND h.order_type_stamped != '7. PS / Other'
      AND h.is_eligible_created_pipeline_flag = 1
      AND h.net_arr > 0
      GROUP BY 1,2

  -- ARR at the same point in time in Fiscal Year
  ), arr_at_same_month AS (

    SELECT
      mrr.dim_crm_account_id AS account_id,
      mrr_date.fiscal_year   AS report_fiscal_year,
  --    ultimate_parent_account_id,
      SUM(mrr.mrr)      AS mrr,
      SUM(mrr.arr)      AS arr,
      SUM(CASE
              WHEN sub.is_channel_arr_flag = 1
                  THEN mrr.arr
              ELSE 0
          END)          AS reseller_arr,
      SUM(CASE
              WHEN  sub.is_channel_arr_flag = 0
                  THEN mrr.arr
              ELSE 0
          END)          AS direct_arr,


      SUM(CASE
              WHEN  (mrr.product_tier_name LIKE '%Starter%'
                      OR mrr.product_tier_name LIKE '%Bronze%')
                  THEN mrr.arr
              ELSE 0
          END)          AS product_starter_arr,


      SUM(CASE
              WHEN  mrr.product_tier_name LIKE '%Premium%'
                  THEN mrr.arr
              ELSE 0
          END)          AS product_premium_arr,
      SUM(CASE
              WHEN  mrr.product_tier_name LIKE '%Ultimate%'
                  THEN mrr.arr
              ELSE 0
          END)          AS product_ultimate_arr,

      SUM(CASE
              WHEN  mrr.product_tier_name LIKE '%Self-Managed%'
                  THEN mrr.arr
              ELSE 0
          END)          AS delivery_self_managed_arr,
      SUM(CASE
              WHEN  mrr.product_tier_name LIKE '%SaaS%'
                  THEN mrr.arr
              ELSE 0
          END)          AS delivery_saas_arr

    FROM mart_arr AS mrr
    INNER JOIN date_details AS mrr_date
      ON mrr.arr_month = mrr_date.date_actual
    INNER JOIN dim_subscription AS sub
      ON sub.dim_subscription_id = mrr.dim_subscription_id
    WHERE mrr_date.month_actual =  (SELECT DISTINCT month_actual
                                      FROM date_details
                                      WHERE date_actual = DATE_TRUNC('month', DATEADD(month, -1, CURRENT_DATE)))
    GROUP BY 1,2

), sao_last_12_month AS (
   
  SELECT 
        h.sales_accepted_fiscal_year   AS report_fiscal_year,
        h.account_id,
        SUM(h.calculated_deal_count)    AS last_12m_sao_deal_count,
        SUM(h.net_arr)                  AS last_12m_sao_net_arr,
        SUM(h.booked_net_arr)           AS last_12m_sao_booked_net_arr       
        
  FROM sfdc_opportunity_snapshot_xf AS h
    WHERE
        h.sales_accepted_date > dateadd(month,-12,h.snapshot_date)
      AND h.sales_accepted_date <= h.snapshot_date
      AND h.order_type_stamped != '7. PS / Other'
      AND h.is_eligible_sao_flag = 1
      AND h.is_renewal = 0
    GROUP BY 1,2
    
), sao_fy AS (      

  SELECT 
        h.sales_accepted_fiscal_year   AS report_fiscal_year,
        h.account_id,
        SUM(h.calculated_deal_count)    AS fy_sao_deal_count,
        SUM(h.net_arr)                  AS fy_sao_net_arr,
        SUM(h.booked_net_arr)           AS fy_sao_booked_net_arr       
        
  FROM sfdc_opportunity_snapshot_xf AS h
    WHERE
       h.snapshot_fiscal_year = h.sales_accepted_fiscal_year
      AND h.sales_accepted_date <= h.snapshot_date
      AND h.order_type_stamped != '7. PS / Other'
      AND h.is_eligible_sao_flag = 1
      AND h.is_renewal = 0
    GROUP BY 1,2
    
), consolidated_accounts AS (

  SELECT
    ak.report_fiscal_year,
    a.account_id                      AS account_id,
    a.account_name                    AS account_name,
    a.ultimate_parent_account_id      AS upa_id,
    a.ultimate_parent_account_name    AS upa_name,
    a.is_key_account,
    a.abm_tier,
    a.parent_id,
    u.name                              AS account_owner_name,
    a.owner_id                          AS account_owner_id,
    trim(u.employee_number)             AS account_owner_employee_number,
    upa_owner.name                      AS upa_owner_name,
    upa_owner.user_id                   AS upa_owner_id,
    upa_owner.title_category            AS upa_owner_title_category,
    trim(upa_owner.employee_number)     AS upa_owner_employee_number,
    dim_account.forbes_2000_rank        AS account_forbes_rank,
    a.billing_country                   AS account_country,
    a.billing_postal_code               AS account_zip_code,
    mart_crm_account.account_billing_state AS account_state,

    
    -- Account demographics fields
    upa_account.parent_crm_account_demographics_sales_segment       AS upa_ad_segment,
    upa_account.parent_crm_account_demographics_geo                 AS upa_ad_geo,
    upa_account.parent_crm_account_demographics_region              AS upa_ad_region,
    upa_account.parent_crm_account_demographics_area                AS upa_ad_area,
    
    coalesce(upa_account.parent_crm_account_billing_country, REPLACE(REPLACE(REPLACE(upa.tsp_address_country,'The Netherlands','Netherlands'),'Russian Federation','Russia'), 'Russia','Russian Federation'))              AS upa_ad_country,
    upa_account.parent_crm_account_demographics_upa_state           AS upa_ad_state,
    upa_account.parent_crm_account_demographics_upa_city            AS upa_ad_city,
    upa_account.parent_crm_account_demographics_upa_postal_code     AS upa_ad_zip_code,


    
    -- substitute this by key segment
    u.user_geo                                    AS account_user_geo,
    u.user_region                                 AS account_user_region,
    u.user_segment                                AS account_user_segment,
    u.user_area                                   AS account_user_area,
    u.role_name                                   AS account_owner_role,
    u.title_category                              AS account_owner_title_category,
    a.industry                                    AS account_industry,
    upa_owner.user_geo                            AS upa_user_geo,
    upa_owner.user_region                         AS upa_user_region,
    upa_owner.user_segment                        AS upa_user_segment,
    upa_owner.user_area                           AS upa_user_area,
    upa_owner.role_name                           AS upa_user_role,
    upa.industry                                  AS upa_industry,
    
    -- NF: These fields are only useful to calculate LAM Dev Count which is already calculated
    coalesce(mart_crm_account.potential_users, 0)                               AS potential_users,
    coalesce(mart_crm_account.number_of_licenses_this_account, 0)               AS licenses,
    coalesce(mart_crm_account.decision_maker_count_linkedin, 0)                 AS linkedin_developer,
    coalesce(mart_crm_account.crm_account_zoom_info_number_of_developers, 0)    AS zi_developers,
    coalesce(mart_crm_account.zoom_info_company_revenue, 0)                     AS zi_revenue,

    --
    upa_account.parent_crm_account_lam_dev_count                       AS upa_lam_dev_count,
    mart_crm_account.public_sector_account_flag,
    mart_crm_account.pubsec_type,
    mart_crm_account.potential_lam_arr,
    coalesce(mart_crm_account.crm_account_demographics_employee_count, 0)   AS employees,
    
    COALESCE(mart_crm_account.carr_account_family, 0)                       AS account_family_arr,
    LEAST(50000,GREATEST(coalesce(mart_crm_account.number_of_licenses_this_account,0),COALESCE(mart_crm_account.potential_users, mart_crm_account.decision_maker_count_linkedin, mart_crm_account.crm_account_zoom_info_number_of_developers, 0)))           AS calculated_developer_count,
    a.technical_account_manager_date,
    a.technical_account_manager                                             AS technical_account_manager_name,
    CASE
      WHEN mart_crm_account.has_tam_flag
          THEN 1
      ELSE 0
    END                                           AS has_technical_account_manager_flag,

    a.health_score_color                          AS account_health_score_color,
    a.health_number                               AS account_health_number,

    -- atr for current fy
    COALESCE(fy_atr_base.fy_atr,0)           AS fy_atr,
    -- next fiscal year atr base reported at fy
    COALESCE(nfy_atr_base.nfy_atr,0)         AS nfy_atr,
    COALESCE(nfy_atr_base.nfy_q1_atr,0)      AS nfy_q1_atr,
    COALESCE(nfy_atr_base.nfy_q2_atr,0)      AS nfy_q2_atr,
    COALESCE(nfy_atr_base.nfy_q3_atr,0)      AS nfy_q3_atr,
    COALESCE(nfy_atr_base.nfy_q4_atr,0)      AS nfy_q4_atr,
    -- last 12 months ATR
    COALESCE(last_12m_atr_base.last_12m_atr,0)    AS last_12m_atr,

    -- arr by fy
    COALESCE(arr.arr,0)                           AS arr,

    COALESCE(arr.reseller_arr,0)                  AS arr_channel,
    COALESCE(arr.direct_arr,0)                    AS arr_direct,

    COALESCE(arr.product_starter_arr,0)           AS product_starter_arr,
    COALESCE(arr.product_premium_arr,0)           AS product_premium_arr,
    COALESCE(arr.product_ultimate_arr,0)          AS product_ultimate_arr,


    CASE
      WHEN COALESCE(arr.product_ultimate_arr,0) > COALESCE(arr.product_starter_arr,0) + COALESCE(arr.product_premium_arr,0)
          THEN 1
      ELSE 0
    END                                           AS is_ultimate_customer_flag,

    CASE
      WHEN COALESCE(arr.product_ultimate_arr,0) < COALESCE(arr.product_starter_arr,0) + COALESCE(arr.product_premium_arr,0)
          THEN 1
      ELSE 0
    END                                           AS is_premium_customer_flag,

    COALESCE(arr.delivery_self_managed_arr,0)     AS delivery_self_managed_arr,
    COALESCE(arr.delivery_saas_arr,0)             AS delivery_saas_arr,

    -- accounts counts
    CASE
      WHEN COALESCE(arr.arr,0) = 0
      THEN 1
      ELSE 0
    END                                           AS is_prospect_flag,

    CASE
      WHEN COALESCE(arr.arr,0) > 0
      THEN 1
      ELSE 0
    END                                           AS is_customer_flag,

    CASE
      WHEN COALESCE(arr.arr,0) > 5000
      THEN 1
      ELSE 0
    END                                           AS is_over_5k_customer_flag,
    CASE
      WHEN COALESCE(arr.arr,0) > 10000
      THEN 1
      ELSE 0
    END                                           AS is_over_10k_customer_flag,
    CASE
      WHEN COALESCE(arr.arr,0) > 50000
      THEN 1
      ELSE 0
    END                                           AS is_over_50k_customer_flag,

    CASE
      WHEN COALESCE(arr.arr,0) > 100000
      THEN 1
      ELSE 0
    END                                           AS is_over_100k_customer_flag,

    CASE
      WHEN COALESCE(arr.arr,0) > 500000
      THEN 1
      ELSE 0
    END                                           AS is_over_500k_customer_flag,

    -- rolling last 12 months booked net arr
    COALESCE(net_arr_last_12m.last_12m_booked_net_arr,0)                       AS last_12m_booked_net_arr,
    COALESCE(net_arr_last_12m.last_12m_booked_non_web_net_arr,0)               AS last_12m_booked_non_web_net_arr,
    COALESCE(net_arr_last_12m.last_12m_booked_web_direct_sourced_net_arr,0)    AS last_12m_booked_web_direct_sourced_net_arr,
    COALESCE(net_arr_last_12m.last_12m_booked_channel_sourced_net_arr,0)       AS last_12m_booked_channel_sourced_net_arr,
    COALESCE(net_arr_last_12m.last_12m_booked_sdr_sourced_net_arr,0)           AS last_12m_booked_sdr_sourced_net_arr,
    COALESCE(net_arr_last_12m.last_12m_booked_ae_sourced_net_arr,0)            AS last_12m_booked_ae_sourced_net_arr,
    COALESCE(net_arr_last_12m.last_12m_booked_churn_contraction_net_arr,0)     AS last_12m_booked_churn_contraction_net_arr,
    COALESCE(net_arr_last_12m.last_12m_booked_fo_net_arr,0)                    AS last_12m_booked_fo_net_arr,
    COALESCE(net_arr_last_12m.last_12m_booked_new_connected_net_arr,0)         AS last_12m_booked_new_connected_net_arr,
    COALESCE(net_arr_last_12m.last_12m_booked_growth_net_arr,0)                AS last_12m_booked_growth_net_arr,
    COALESCE(net_arr_last_12m.last_12m_booked_deal_count,0)                    AS last_12m_booked_deal_count,
    COALESCE(net_arr_last_12m.last_12m_booked_direct_net_arr,0)                AS last_12m_booked_direct_net_arr,
    COALESCE(net_arr_last_12m.last_12m_booked_channel_net_arr,0)               AS last_12m_booked_channel_net_arr,
    COALESCE(net_arr_last_12m.last_12m_booked_channel_net_arr,0)  - COALESCE(net_arr_last_12m.last_12m_booked_channel_sourced_net_arr,0)   AS last_12m_booked_channel_co_sell_net_arr,
    COALESCE(net_arr_last_12m.last_12m_booked_direct_deal_count,0)             AS last_12m_booked_direct_deal_count,
    COALESCE(net_arr_last_12m.last_12m_booked_channel_deal_count,0)            AS last_12m_booked_channel_deal_count,
    COALESCE(net_arr_last_12m.last_12m_booked_churn_contraction_deal_count,0)  AS last_12m_booked_churn_contraction_deal_count,
    COALESCE(net_arr_last_12m.last_12m_booked_renewal_deal_count,0)            AS last_12m_booked_renewal_deal_count,
    COALESCE(net_arr_last_12m.last_12m_booked_trx_count,0)                     AS last_12m_booked_trx_count,
    COALESCE(net_arr_last_12m.last_12m_booked_trx_over_5k_count,0)             AS last_12m_booked_trx_over_5k_count,
    COALESCE(net_arr_last_12m.last_12m_booked_trx_over_10k_count,0)            AS last_12m_booked_trx_over_10k_count,
    COALESCE(net_arr_last_12m.last_12m_booked_trx_over_50k_count,0)            AS last_12m_booked_trx_over_50k_count,

    -- fy booked net arr
    COALESCE(net_arr_fiscal.fy_booked_net_arr,0)                     AS fy_booked_net_arr,
    COALESCE(net_arr_fiscal.fy_booked_web_direct_sourced_net_arr,0)  AS fy_booked_web_direct_sourced_net_arr,
    COALESCE(net_arr_fiscal.fy_booked_channel_sourced_net_arr,0)     AS fy_booked_channel_sourced_net_arr,
    COALESCE(net_arr_fiscal.fy_booked_sdr_sourced_net_arr,0)         AS fy_booked_sdr_sourced_net_arr,
    COALESCE(net_arr_fiscal.fy_booked_ae_sourced_net_arr,0)          AS fy_booked_ae_sourced_net_arr,
    COALESCE(net_arr_fiscal.fy_booked_churn_contraction_net_arr,0)   AS fy_booked_churn_contraction_net_arr,
    COALESCE(net_arr_fiscal.fy_booked_fo_net_arr,0)                  AS fy_booked_fo_net_arr,
    COALESCE(net_arr_fiscal.fy_booked_new_connected_net_arr,0)       AS fy_booked_new_connected_net_arr,
    COALESCE(net_arr_fiscal.fy_booked_growth_net_arr,0)              AS fy_booked_growth_net_arr,
    COALESCE(net_arr_fiscal.fy_booked_deal_count,0)                  AS fy_booked_deal_count,
    COALESCE(net_arr_fiscal.fy_booked_direct_net_arr,0)              AS fy_booked_direct_net_arr,
    COALESCE(net_arr_fiscal.fy_booked_channel_net_arr,0)             AS fy_booked_channel_net_arr,
    COALESCE(net_arr_fiscal.fy_booked_direct_deal_count,0)           AS fy_booked_direct_deal_count,
    COALESCE(net_arr_fiscal.fy_booked_channel_deal_count,0)          AS fy_booked_channel_deal_count,

    -- open pipe forward looking
    COALESCE(op.total_open_pipe,0)                  AS total_open_pipe,
    COALESCE(op.total_count_open_deals,0)           AS total_count_open_deals_pipe,
    COALESCE(op.nfy_open_pipeline,0)                AS nfy_open_pipeline,
    COALESCE(op.fy_open_pipeline,0)                 AS fy_open_pipeline,
    COALESCE(op.nfy_count_open_deals,0)             AS nfy_count_open_deals,
    COALESCE(op.fy_count_open_deals,0)              AS fy_count_open_deals,

    CASE
      WHEN COALESCE(arr.arr,0) > 0
          AND COALESCE(op.total_open_pipe,0) > 0
              THEN 1
          ELSE 0
    END                                                       AS customer_has_open_pipe_flag,

    CASE
      WHEN COALESCE(arr.arr,0) = 0
          AND COALESCE(op.total_open_pipe,0) > 0
              THEN 1
          ELSE 0
    END                                                       AS prospect_has_open_pipe_flag,

    -- pipe generation
    COALESCE(pg.pg_ytd_net_arr,0)                             AS pg_ytd_net_arr,
    COALESCE(pg.pg_ytd_web_direct_sourced_net_arr,0)          AS pg_ytd_web_direct_sourced_net_arr,
    COALESCE(pg.pg_ytd_channel_sourced_net_arr,0)             AS pg_ytd_channel_sourced_net_arr,
    COALESCE(pg.pg_ytd_sdr_sourced_net_arr,0)                 AS pg_ytd_sdr_sourced_net_arr,
    COALESCE(pg.pg_ytd_ae_sourced_net_arr,0)                  AS pg_ytd_ae_sourced_net_arr,

    COALESCE(pg_ly.pg_last_12m_net_arr,0)                     AS pg_last_12m_net_arr,
    COALESCE(pg_ly.pg_last_12m_web_direct_sourced_net_arr,0)  AS pg_last_12m_web_direct_sourced_net_arr,
    COALESCE(pg_ly.pg_last_12m_channel_sourced_net_arr,0)     AS pg_last_12m_channel_sourced_net_arr,
    COALESCE(pg_ly.pg_last_12m_sdr_sourced_net_arr,0)         AS pg_last_12m_sdr_sourced_net_arr,
    COALESCE(pg_ly.pg_last_12m_ae_sourced_net_arr,0)          AS pg_last_12m_ae_sourced_net_arr,

    COALESCE(pg_last_12m_web_direct_sourced_deal_count,0)     AS pg_last_12m_web_direct_sourced_deal_count,
    COALESCE(pg_last_12m_channel_sourced_deal_count,0)        AS pg_last_12m_channel_sourced_deal_count,
    COALESCE(pg_last_12m_sdr_sourced_deal_count,0)            AS pg_last_12m_sdr_sourced_deal_count,
    COALESCE(pg_last_12m_ae_sourced_deal_count,0)             AS pg_last_12m_ae_sourced_deal_count,
    
    -- SAO metrics
    COALESCE(sao_last_12_month.last_12m_sao_deal_count,0)       AS last_12m_sao_deal_count,
    COALESCE(sao_last_12_month.last_12m_sao_net_arr,0)          AS last_12m_sao_net_arr,
    COALESCE(sao_last_12_month.last_12m_sao_booked_net_arr,0)   AS last_12m_sao_booked_net_arr, 
    COALESCE(sao_fy.fy_sao_deal_count,0)                        AS fy_sao_deal_count,
    COALESCE(sao_fy.fy_sao_net_arr,0)                           AS fy_sao_net_arr,
    COALESCE(sao_fy.fy_sao_booked_net_arr,0)                    AS fy_sao_booked_net_arr,
    
     -- LAM Dev Count Category
    CASE 
        WHEN upa_account.parent_crm_account_lam_dev_count < 100
            THEN '0. <100'    
        WHEN upa_account.parent_crm_account_lam_dev_count >= 100
            AND upa_account.parent_crm_account_lam_dev_count < 250
            THEN '1. [100-250)'
        WHEN upa_account.parent_crm_account_lam_dev_count >= 250
            AND upa_account.parent_crm_account_lam_dev_count < 500
            THEN '2. [250-500)'
        WHEN upa_account.parent_crm_account_lam_dev_count >= 500
            AND upa_account.parent_crm_account_lam_dev_count < 1500
            THEN '3. [500-1500)'
        WHEN upa_account.parent_crm_account_lam_dev_count >= 1500
            AND upa_account.parent_crm_account_lam_dev_count < 2500
            THEN '4. [1500-2500)'
        WHEN upa_account.parent_crm_account_lam_dev_count >= 2500
            AND upa_account.parent_crm_account_lam_dev_count < 3500
            THEN '5. [2500-3500)'
        WHEN upa_account.parent_crm_account_lam_dev_count >= 3500
            AND upa_account.parent_crm_account_lam_dev_count < 5000
            THEN '6. [3500-5000)'
        WHEN upa_account.parent_crm_account_lam_dev_count >= 5000
            THEN '7. >5000'
    END AS lam_dev_count_bin_name,
    
    CASE 
        WHEN upa_account.parent_crm_account_lam_dev_count < 100
            THEN 0
        WHEN upa_account.parent_crm_account_lam_dev_count >= 100
            AND upa_account.parent_crm_account_lam_dev_count < 250
            THEN 100
        WHEN upa_account.parent_crm_account_lam_dev_count >= 250
            AND upa_account.parent_crm_account_lam_dev_count < 500
            THEN 250
        WHEN upa_account.parent_crm_account_lam_dev_count >= 500
            AND upa_account.parent_crm_account_lam_dev_count < 1500
            THEN 500
        WHEN upa_account.parent_crm_account_lam_dev_count >= 1500
            AND upa_account.parent_crm_account_lam_dev_count < 2500
            THEN 1500
        WHEN upa_account.parent_crm_account_lam_dev_count >= 2500
            AND upa_account.parent_crm_account_lam_dev_count < 3500
            THEN 2500
        WHEN upa_account.parent_crm_account_lam_dev_count >= 3500
            AND upa_account.parent_crm_account_lam_dev_count < 5000
            THEN 3500
        WHEN upa_account.parent_crm_account_lam_dev_count >= 5000
            THEN 5000
    END AS lam_dev_count_bin_rank,      
    
    -- Public Sector
    CASE
        WHEN mart_crm_account.pubsec_type ='ROW-PubSec'
            THEN 'Public'
        ELSE 'Private'
    END                     AS sector_type,
    CASE
        WHEN mart_crm_account.pubsec_type ='ROW-PubSec'
            THEN 1
        ELSE 0
    END                     AS is_public_sector_flag

  FROM account_year_key AS ak
  INNER JOIN sfdc_accounts_xf AS a
    ON ak.account_id = a.account_id
  LEFT JOIN dim_crm_account AS upa_account
    ON a.ultimate_parent_account_id = upa_account.dim_crm_account_id
  LEFT JOIN sfdc_accounts_xf AS upa
    ON a.ultimate_parent_account_id = upa.account_id
  LEFT JOIN dim_crm_account AS dim_account
    ON ak.account_id = dim_account.dim_crm_account_id
  LEFT JOIN mart_crm_account
    ON ak.account_id = mart_crm_account.dim_crm_account_id
  LEFT JOIN sfdc_users_xf AS u
    ON a.owner_id = u.user_id
  LEFT JOIN sfdc_users_xf AS upa_owner
    ON upa.owner_id = upa_owner.user_id
  LEFT JOIN fy_atr_base
    ON fy_atr_base.account_id = ak.account_id
    AND fy_atr_base.report_fiscal_year = ak.report_fiscal_year
  LEFT JOIN last_12m_atr_base AS last_12m_atr_base
    ON last_12m_atr_base.account_id = ak.account_id
    AND last_12m_atr_base.report_fiscal_year = ak.report_fiscal_year
  LEFT JOIN nfy_atr_base
    ON nfy_atr_base.account_id = ak.account_id
    AND nfy_atr_base.report_fiscal_year = ak.report_fiscal_year
  LEFT JOIN net_arr_last_12m
    ON net_arr_last_12m.account_id = ak.account_id
    AND net_arr_last_12m.report_fiscal_year = ak.report_fiscal_year
  LEFT JOIN op_forward_one_year AS op
    ON op.account_id = ak.account_id
    AND op.report_fiscal_year = ak.report_fiscal_year
  LEFT JOIN pg_ytd AS pg
    ON pg.account_id = ak.account_id
    AND pg.report_fiscal_year = ak.report_fiscal_year
  LEFT JOIN pg_last_12_months AS pg_ly
    ON pg_ly.account_id = ak.account_id
    AND pg_ly.report_fiscal_year = ak.report_fiscal_year
  LEFT JOIN arr_at_same_month AS arr
    ON arr.account_id = ak.account_id
    AND arr.report_fiscal_year = ak.report_fiscal_year
  LEFT JOIN fy_net_arr AS net_arr_fiscal
    ON net_arr_fiscal.account_id = ak.account_id
    AND net_arr_fiscal.report_fiscal_year = ak.report_fiscal_year
  -- SAOs
  LEFT JOIN sao_last_12_month 
    ON sao_last_12_month.account_id = ak.account_id
    AND sao_last_12_month.report_fiscal_year = ak.report_fiscal_year
  LEFT JOIN sao_fy
    ON sao_fy.account_id = ak.account_id
    AND sao_fy.report_fiscal_year = ak.report_fiscal_year

    -----------------------
-- Adjust for hierarchies split between different geos
), upa_virtual_cte AS (

SELECT 
    report_fiscal_year,
    upa_id,
    upa_name,
    upa_user_geo,
    account_id              AS virtual_upa_id,
    account_name            AS virtual_upa_name,
    upa_ad_segment          AS virtual_upa_segment,
    account_user_geo        AS virtual_upa_geo,
    account_user_region     AS virtual_upa_region,
    account_user_area       AS virtual_upa_area,
    account_country         AS virtual_upa_country,
    account_zip_code        AS virtual_upa_zip_code,
    account_industry        AS virtual_upa_industry,
    account_state           AS virtual_upa_state,
    account_owner_name      AS virtual_upa_owner_name,
    account_owner_title_category AS virtual_upa_owner_title_category,
    account_owner_id        AS virtual_upa_owner_id,
    account_id,
    account_name,
    account_owner_name,
    arr AS account_arr,
    1 AS level
FROM consolidated_accounts
WHERE upa_user_geo != account_user_geo
    AND arr > 5000
   -- AND upa_user_geo = 'EMEA'
UNION ALL 
SELECT 
    upa.report_fiscal_year,
    upa.upa_id,
    upa.upa_name,
    upa.upa_user_geo,
    upa.virtual_upa_id,
    upa.virtual_upa_name,
    upa.virtual_upa_segment,
    upa.virtual_upa_geo,
    upa.virtual_upa_region,
    upa.virtual_upa_area,
    upa.virtual_upa_country,
    upa.virtual_upa_zip_code,
    upa.virtual_upa_industry,
    upa.virtual_upa_state,
    upa.virtual_upa_owner_name,
    upa.virtual_upa_owner_title_category,
    upa.virtual_upa_owner_id,
    child.account_id,
    child.account_name,
    child.account_owner_name,
    child.arr AS account_arr,
    level + 1 AS level
FROM consolidated_accounts child
INNER JOIN upa_virtual_cte upa
    ON child.parent_id = upa.account_id
    AND child.report_fiscal_year = upa.report_fiscal_year

), max_virtual_upa_depth AS (

    SELECT 
        report_fiscal_year,
        upa_id,
        upa_name,
        virtual_upa_segment,
        virtual_upa_geo,
        virtual_upa_id, 
        virtual_upa_name, 
        MAX(level) AS max_depth
    FROM upa_virtual_cte
    GROUP BY 1,2,3,4,5,6,7
    
), selected_virtual_upa_head AS (

SELECT 
    report_fiscal_year,
    upa_id,
    upa_name,
    virtual_upa_segment,
    virtual_upa_geo,
    virtual_upa_id,
    virtual_upa_name,
    max_depth,
    ROW_NUMBER() OVER (PARTITION BY upa_id, report_fiscal_year ORDER BY max_depth DESC) AS level
FROM max_virtual_upa_depth
QUALIFY level = 1

        
-- selects the longest hierarchy from the virtual UPAs options
), selected_hierarchy_virtual_upa AS (

    
    SELECT total.*
    FROM upa_virtual_cte total
    INNER JOIN selected_virtual_upa_head selected
        ON total.virtual_upa_id = selected.virtual_upa_id
        AND total.report_fiscal_year = selected.report_fiscal_year


-- identify unique virtual upas
), select_unique_virtual_upa AS (

SELECT 
    final.report_fiscal_year,
    final.upa_id,
    final.upa_name,
    final.upa_user_geo,
    final.virtual_upa_id,
    final.virtual_upa_name,
    final.virtual_upa_segment,
    final.virtual_upa_geo,
    final.virtual_upa_region,
    final.virtual_upa_area,
    final.virtual_upa_country,
    final.virtual_upa_zip_code,
    final.virtual_upa_industry,
    final.virtual_upa_state,
    final.virtual_upa_owner_name,
    final.virtual_upa_owner_title_category,
    final.virtual_upa_owner_id
FROM selected_hierarchy_virtual_upa final
    

-- identify accounts that belong to the same owner of a virtual upa within the hierarchy
-- after creating the virtual UPA hierarchy, some accounts might 
), final_virtual_upa AS (
    
SELECT 
    final.report_fiscal_year,
    final.upa_id,
    final.upa_name,
    final.upa_user_geo,
    extra.virtual_upa_id,
    extra.virtual_upa_name,
    extra.virtual_upa_segment,
    extra.virtual_upa_geo,
    extra.virtual_upa_region,
    extra.virtual_upa_area,
    extra.virtual_upa_country,
    extra.virtual_upa_zip_code,
    extra.virtual_upa_industry,
    extra.virtual_upa_state,
    extra.virtual_upa_owner_name,
    extra.virtual_upa_owner_title_category,
    extra.virtual_upa_owner_id,
    final.account_id,
    final.account_name,
    final.account_owner_name,
    final.arr AS account_arr,
    -1 AS level
FROM consolidated_accounts final
    INNER JOIN select_unique_virtual_upa extra
        ON final.upa_id = extra.upa_id
        AND final.account_owner_name = extra.virtual_upa_owner_name
        AND final.report_fiscal_year = extra.report_fiscal_year
-- Exclude accounts already in the hierarchy table
WHERE final.account_id NOT IN (SELECT DISTINCT account_id FROM selected_hierarchy_virtual_upa)
UNION
  SELECT 
    final.report_fiscal_year,
    final.upa_id,
    final.upa_name,
    final.upa_user_geo,
    final.virtual_upa_id,
    final.virtual_upa_name,
    final.virtual_upa_segment,
    final.virtual_upa_geo,
    final.virtual_upa_region,
    final.virtual_upa_area,
    final.virtual_upa_country,
    final.virtual_upa_zip_code,
    final.virtual_upa_industry,
    final.virtual_upa_state,
    final.virtual_upa_owner_name,
    final.virtual_upa_owner_title_category,
    final.virtual_upa_owner_id,
    final.account_id,
    final.account_name,
    final.account_owner_name,
    final.account_arr,
    final.level
FROM selected_hierarchy_virtual_upa final
------------------------

), consolidated_upa AS (

  SELECT
    acc.report_fiscal_year,
    CASE 
        WHEN new_upa.upa_id IS NOT NULL 
            THEN 'Virtual'
        ELSE 'Real'
    END                                     AS upa_type,
    CASE 
        WHEN new_upa.upa_id IS NOT NULL 
            THEN new_upa.virtual_upa_id 
        ELSE acc.upa_id
    END                                     AS upa_id,
    CASE 
        WHEN new_upa.upa_id IS NOT NULL 
            THEN new_upa.virtual_upa_name
        ELSE acc.upa_name
    END                                     AS upa_name,
    CASE 
        WHEN new_upa.upa_id IS NOT NULL 
            THEN new_upa.virtual_upa_owner_name
        ELSE acc.upa_owner_name
    END                                     AS upa_owner_name,
    CASE 
        WHEN new_upa.upa_id IS NOT NULL 
            THEN new_upa.virtual_upa_owner_id 
        ELSE acc.upa_owner_id
    END                                     AS upa_owner_id,
    CASE 
        WHEN new_upa.upa_id IS NOT NULL 
            THEN new_upa.virtual_upa_owner_title_category
        ELSE acc.upa_owner_title_category
    END                                     AS upa_owner_title_category,
    
    CASE 
        WHEN new_upa.upa_id IS NOT NULL 
            THEN new_upa.virtual_upa_industry 
        ELSE acc.upa_industry
    END                                     AS upa_industry,
    
    -- Account Demographics
    CASE 
        WHEN new_upa.upa_id IS NOT NULL 
            THEN new_upa.virtual_upa_segment
        ELSE acc.upa_ad_segment
    END                                     AS upa_ad_segment,
    CASE 
        WHEN new_upa.upa_id IS NOT NULL 
            THEN new_upa.virtual_upa_geo
        ELSE acc.upa_ad_geo
    END                                     AS upa_ad_geo,
    CASE 
        WHEN new_upa.upa_id IS NOT NULL 
            THEN new_upa.virtual_upa_region 
        ELSE acc.upa_ad_region
    END                                     AS upa_ad_region,
    CASE 
        WHEN new_upa.upa_id IS NOT NULL 
            THEN new_upa.virtual_upa_area
        ELSE acc.upa_ad_area
    END                                     AS upa_ad_area,
    CASE 
        WHEN new_upa.upa_id IS NOT NULL 
            THEN new_upa.virtual_upa_country 
        ELSE acc.upa_ad_country
    END                                     AS upa_ad_country,

    CASE 
        WHEN new_upa.upa_id IS NOT NULL 
            THEN new_upa.virtual_upa_state 
        ELSE acc.upa_ad_state
    END                                     AS upa_ad_state,

    CASE 
        WHEN new_upa.upa_id IS NOT NULL 
            THEN new_upa.virtual_upa_zip_code 
        ELSE acc.upa_ad_zip_code
    END                                     AS upa_ad_zip_code,

    -- Account User Owner fields
    CASE 
        WHEN new_upa.upa_id IS NOT NULL 
            THEN new_upa.virtual_upa_segment
        ELSE acc.upa_user_segment
    END                                     AS upa_user_segment,
    CASE 
        WHEN new_upa.upa_id IS NOT NULL 
            THEN new_upa.virtual_upa_geo
        ELSE acc.upa_user_geo
    END                                     AS upa_user_geo,
    CASE 
        WHEN new_upa.upa_id IS NOT NULL 
            THEN new_upa.virtual_upa_region 
        ELSE acc.upa_user_region
    END                                     AS upa_user_region,
    CASE 
        WHEN new_upa.upa_id IS NOT NULL 
            THEN new_upa.virtual_upa_area
        ELSE acc.upa_user_area
    END                                     AS upa_user_area,
    CASE 
        WHEN new_upa.upa_id IS NOT NULL 
            THEN new_upa.virtual_upa_country 
        ELSE acc.upa_ad_country
    END                                     AS upa_user_country,
    CASE 
        WHEN new_upa.upa_id IS NOT NULL 
            THEN new_upa.virtual_upa_zip_code 
        ELSE acc.upa_ad_zip_code
    END                                     AS upa_user_zip_code,
    
    
    acc.lam_dev_count_bin_rank,
    acc.lam_dev_count_bin_name,
    -- Public Sector
    CASE
        WHEN MAX(acc.is_public_sector_flag) = 1
            THEN 'Public'
        ELSE 'Private'
    END                             AS sector_type,
    MAX(acc.is_public_sector_flag)      AS is_public_sector_flag,
    
    
    SUM(CASE WHEN acc.account_forbes_rank IS NOT NULL THEN 1 ELSE 0 END)   AS count_forbes_accounts,
    MIN(account_forbes_rank)      AS forbes_rank,
    MAX(acc.potential_users)          AS potential_users,
    MAX(acc.licenses)                 AS licenses,
    MAX(acc.linkedin_developer)       AS linkedin_developer,
    MAX(acc.zi_developers)            AS zi_developers,
    MAX(acc.zi_revenue)               AS zi_revenue,
    MAX(acc.employees)                AS employees,
    MAX(acc.upa_lam_dev_count)        AS upa_lam_dev_count,

    SUM(acc.has_technical_account_manager_flag) AS count_technical_account_managers,

    -- atr for current fy
    SUM(acc.fy_atr)  AS fy_atr,
    -- next fiscal year atr base reported at fy
    SUM(acc.nfy_atr) AS nfy_atr,

    -- arr by fy
    SUM(acc.arr) AS arr,

    CASE 
        WHEN  MAX(acc.is_customer_flag) = 1
        THEN 0
    ELSE 1
    END                                   AS is_prospect_flag,
    MAX(acc.is_customer_flag)             AS is_customer_flag,
    MAX(acc.is_over_5k_customer_flag)     AS is_over_5k_customer_flag,
    MAX(acc.is_over_10k_customer_flag)    AS is_over_10k_customer_flag,
    MAX(acc.is_over_50k_customer_flag)    AS is_over_50k_customer_flag,
    MAX(acc.is_over_500k_customer_flag)   AS is_over_500k_customer_flag,
    SUM(acc.is_over_5k_customer_flag)     AS count_over_5k_customers,
    SUM(acc.is_over_10k_customer_flag)    AS count_over_10k_customers,
    SUM(acc.is_over_50k_customer_flag)    AS count_over_50k_customers,
    SUM(acc.is_over_500k_customer_flag)   AS count_over_500k_customers,
    SUM(acc.is_prospect_flag)             AS count_of_prospects,
    SUM(acc.is_customer_flag)             AS count_of_customers,

    SUM(acc.arr_channel)                  AS arr_channel,
    SUM(acc.arr_direct)                   AS arr_direct,

    SUM(acc.product_starter_arr)          AS product_starter_arr,
    SUM(acc.product_premium_arr)          AS product_premium_arr,
    SUM(acc.product_ultimate_arr)         AS product_ultimate_arr,
    SUM(acc.delivery_self_managed_arr)    AS delivery_self_managed_arr,
    SUM(acc.delivery_saas_arr)            AS delivery_saas_arr,


    -- rolling last 12 months bokked net arr
    SUM(last_12m_booked_net_arr)                      AS last_12m_booked_net_arr,
    SUM(acc.last_12m_booked_non_web_net_arr)              AS last_12m_booked_non_web_net_arr,
    SUM(acc.last_12m_booked_web_direct_sourced_net_arr)   AS last_12m_booked_web_direct_sourced_net_arr,
    SUM(acc.last_12m_booked_channel_sourced_net_arr)      AS last_12m_booked_channel_sourced_net_arr,
    SUM(acc.last_12m_booked_sdr_sourced_net_arr)          AS last_12m_booked_sdr_sourced_net_arr,
    SUM(acc.last_12m_booked_ae_sourced_net_arr)           AS last_12m_booked_ae_sourced_net_arr,
    SUM(acc.last_12m_booked_churn_contraction_net_arr)    AS last_12m_booked_churn_contraction_net_arr,
    SUM(acc.last_12m_booked_fo_net_arr)                   AS last_12m_booked_fo_net_arr,
    SUM(acc.last_12m_booked_new_connected_net_arr)        AS last_12m_booked_new_connected_net_arr,
    SUM(acc.last_12m_booked_growth_net_arr)               AS last_12m_booked_growth_net_arr,
    SUM(acc.last_12m_booked_deal_count)                   AS last_12m_booked_deal_count,
    SUM(acc.last_12m_booked_direct_net_arr)               AS last_12m_booked_direct_net_arr,
    SUM(acc.last_12m_booked_channel_net_arr)              AS last_12m_booked_channel_net_arr,
    SUM(acc.last_12m_atr)                                 AS last_12m_atr,

    -- fy booked net arr
    SUM(acc.fy_booked_net_arr)                   AS fy_booked_net_arr,
    SUM(acc.fy_booked_web_direct_sourced_net_arr) AS fy_booked_web_direct_sourced_net_arr,
    SUM(acc.fy_booked_channel_sourced_net_arr)   AS fy_booked_channel_sourced_net_arr,
    SUM(acc.fy_booked_sdr_sourced_net_arr)       AS fy_booked_sdr_sourced_net_arr,
    SUM(acc.fy_booked_ae_sourced_net_arr)        AS fy_booked_ae_sourced_net_arr,
    SUM(acc.fy_booked_churn_contraction_net_arr) AS fy_booked_churn_contraction_net_arr,
    SUM(acc.fy_booked_fo_net_arr)                AS fy_booked_fo_net_arr,
    SUM(acc.fy_booked_new_connected_net_arr)     AS fy_booked_new_connected_net_arr,
    SUM(acc.fy_booked_growth_net_arr)            AS fy_booked_growth_net_arr,
    SUM(acc.fy_booked_deal_count)                AS fy_booked_deal_count,
    SUM(acc.fy_booked_direct_net_arr)            AS fy_booked_direct_net_arr,
    SUM(acc.fy_booked_channel_net_arr)           AS fy_booked_channel_net_arr,
    SUM(acc.fy_booked_direct_deal_count)         AS fy_booked_direct_deal_count,
    SUM(acc.fy_booked_channel_deal_count)        AS fy_booked_channel_deal_count,

    -- open pipe forward looking
    SUM(acc.total_open_pipe)              AS total_open_pipe,
    SUM(acc.total_count_open_deals_pipe)  AS total_count_open_deals_pipe,
    SUM(acc.customer_has_open_pipe_flag)  AS customer_has_open_pipe_flag,
    SUM(acc.prospect_has_open_pipe_flag)  AS prospect_has_open_pipe_flag,

    -- pipe generation
    SUM(acc.pg_ytd_net_arr) AS pg_ytd_net_arr,
    SUM(acc.pg_ytd_web_direct_sourced_net_arr)    AS pg_ytd_web_direct_sourced_net_arr,
    SUM(acc.pg_ytd_channel_sourced_net_arr)       AS pg_ytd_channel_sourced_net_arr,
    SUM(acc.pg_ytd_sdr_sourced_net_arr)           AS pg_ytd_sdr_sourced_net_arr,
    SUM(acc.pg_ytd_ae_sourced_net_arr)            AS pg_ytd_ae_sourced_net_arr,

    SUM(acc.pg_last_12m_net_arr) AS pg_last_12m_net_arr,
    SUM(acc.pg_last_12m_web_direct_sourced_net_arr)   AS pg_last_12m_web_direct_sourced_net_arr,
    SUM(acc.pg_last_12m_channel_sourced_net_arr)      AS pg_last_12m_channel_sourced_net_arr,
    SUM(acc.pg_last_12m_sdr_sourced_net_arr)          AS pg_last_12m_sdr_sourced_net_arr,
    SUM(acc.pg_last_12m_ae_sourced_net_arr)           AS pg_last_12m_ae_sourced_net_arr,
    
    SUM(acc.last_12m_sao_deal_count)                    AS last_12m_sao_deal_count,
    SUM(acc.last_12m_sao_net_arr)                       AS last_12m_sao_net_arr,
    SUM(acc.last_12m_sao_booked_net_arr)                AS last_12m_sao_booked_net_arr, 
    SUM(acc.fy_sao_deal_count)                          AS fy_sao_deal_count,
    SUM(acc.fy_sao_net_arr)                             AS fy_sao_net_arr,
    SUM(acc.fy_sao_booked_net_arr)                      AS fy_sao_booked_net_arr
    
  FROM consolidated_accounts acc
    LEFT JOIN final_virtual_upa new_upa
        ON new_upa.account_id = acc.account_id
        AND new_upa.report_fiscal_year = acc.report_fiscal_year
  GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23

)
, final AS (

  SELECT
      
    acc.*, 
    upa.arr             AS upa_arr,
    
    -- 2022-06-28 JK: account_family_arr - temp solution adding a column with upa_arr for all accounts within the same upa family
    -- this will be updated with ARR bucket based solution later (e.g. 0-50k, 50k-100k, etc.)
    -- upa.arr AS account_family_arr,

    COALESCE(upa.potential_users,0)                 AS upa_potential_users,
    COALESCE(upa.licenses,0)                        AS upa_licenses,
    COALESCE(upa.linkedin_developer,0)              AS upa_linkedin_developer,
    COALESCE(upa.zi_developers,0)                   AS upa_zi_developers,
    COALESCE(upa.zi_revenue,0)                      AS upa_zi_revenue,
    COALESCE(upa.employees,0)                       AS upa_employees,
    COALESCE(upa.count_of_customers,0)              AS upa_count_of_customers,

    CASE
        WHEN upa.upa_id = acc.account_id
            THEN 1
        ELSE 0
    END                                     AS is_upa_flag,

    upa.is_customer_flag                    AS hierarchy_is_customer_flag,
    
    COALESCE(virtual.virtual_upa_id,acc.upa_id)                             AS virtual_upa_id,
    COALESCE(virtual.virtual_upa_name,acc.upa_name)                         AS virtual_upa_name,
    COALESCE(virtual.virtual_upa_segment,acc.upa_user_segment)              AS virtual_upa_segment,
    COALESCE(virtual.virtual_upa_geo,acc.upa_user_geo)                      AS virtual_upa_geo,
    COALESCE(virtual.virtual_upa_region,acc.upa_user_region)                AS virtual_upa_region,
    COALESCE(virtual.virtual_upa_area,acc.upa_user_area)                    AS virtual_upa_area,
    COALESCE(virtual.virtual_upa_country,acc.upa_ad_country)                AS virtual_upa_country,
    COALESCE(virtual.virtual_upa_state,acc.upa_ad_state)                    AS virtual_upa_state,
    COALESCE(virtual.virtual_upa_zip_code,acc.upa_ad_zip_code)              AS virtual_upa_zip_code,
    COALESCE(virtual.virtual_upa_industry,acc.upa_industry)                 AS virtual_upa_industry,
    COALESCE(virtual.virtual_upa_owner_name,acc.upa_owner_name)             AS virtual_upa_owner_name,
    COALESCE(virtual.virtual_upa_owner_title_category,acc.upa_owner_title_category)   AS virtual_upa_owner_title_category,
    COALESCE(virtual.virtual_upa_owner_id,acc.upa_owner_id)                 AS virtual_upa_owner_id, 
    CASE 
        WHEN virtual.upa_id IS NOT NULL 
            THEN 'Virtual'
        ELSE 'Real'
    END                                                                     AS virtual_upa_type

  FROM consolidated_accounts acc
    LEFT JOIN consolidated_upa upa
        ON upa.upa_id = acc.upa_id
        AND upa.report_fiscal_year = acc.report_fiscal_year
    LEFT JOIN final_virtual_upa virtual
        ON virtual.account_id = acc.account_id
        AND virtual.report_fiscal_year = acc.report_fiscal_year

)

SELECT *
FROM final