WITH base AS (
  SELECT 
 -- o.account_name,
  o.close_fiscal_year,
  o.account_id,
  a.tsp_max_hierarchy_sales_segment,
  --o.sales_team_cro_level,
  a.tsp_address_country,
  SUM(o.booked_net_arr)                     AS booked_net_arr,
  SUM(CASE WHEN o.deal_path = 'Direct'
          THEN o.booked_net_arr
          ELSE 0 END)                       AS direct_booked_net_arr,
  SUM(CASE WHEN o.deal_path = 'Channel'
          THEN o.booked_net_arr
          ELSE 0 END)                       AS channel_booked_net_arr,
  SUM(CASE WHEN o.deal_path = 'Channel'
          AND o.sales_qualified_source = 'Channel Generated'
          THEN o.booked_net_arr
          ELSE 0 END)                       AS channel_sourced_booked_net_arr,
  SUM(CASE WHEN o.deal_path = 'Channel'
           AND o.sales_qualified_source != 'Channel Generated'
          THEN o.booked_net_arr
          ELSE 0 END)                       AS channel_cosell_booked_net_arr,
  SUM(CASE WHEN o.deal_path = 'Channel'
            AND o.sales_qualified_source != 'Channel Generated'
            AND o.dr_partner_deal_type = 'Resale'
          THEN o.booked_net_arr
          ELSE 0 END)                       AS resale_booked_net_arr,
  SUM(CASE WHEN o.deal_path = 'Channel'
            AND o.sales_qualified_source != 'Channel Generated'
            AND o.dr_partner_deal_type = 'Referral'
          THEN o.booked_net_arr
          ELSE 0 END)                       AS referral_booked_net_arr,
  SUM(CASE WHEN o.close_fiscal_year = 2022
            AND o.deal_path = 'Channel'      
            AND o.sales_qualified_source != 'Channel Generated'
            AND o.dr_partner_deal_type = 'MSP'
          THEN o.booked_net_arr
          ELSE 0 END)                       AS msp_booked_net_arr,
  SUM(CASE WHEN o.close_fiscal_year = 2022
            AND o.deal_path = 'Channel'
            AND o.sales_qualified_source != 'Channel Generated'
            AND o.dr_partner_deal_type NOT IN ('MSP','Resale','Referral')
          THEN o.booked_net_arr
          ELSE 0 END)                       AS rest_booked_net_arr



FROM nfiguera_prod.restricted_safe_workspace_sales.sfdc_opportunity_xf o
  INNER JOIN nfiguera_prod.restricted_safe_legacy.sfdc_accounts_xf a
    ON a.account_id = o.account_id
WHERE o.is_deleted = 0
  AND o.is_edu_oss = 0
  AND o.close_fiscal_quarter_name LIKE ANY ('%Q1','%Q2')
  AND a.tsp_max_hierarchy_sales_segment in ('Large')
  AND o.booked_net_arr <> 0
  and (is_won = 1 OR (is_renewal = 1 and is_lost = 1))
  AND o.close_fiscal_year <= 2022
  and close_fiscal_year > 2018
GROUP BY 1, 2, 3, 4
)
SELECT *,
    CASE 
        WHEN direct_booked_net_arr >= channel_booked_net_arr 
            THEN 1 
        ELSE 0 
    END AS is_direct_account_flag,
    CASE 
        WHEN direct_booked_net_arr < channel_booked_net_arr 
            THEN 1 
        ELSE 0 
    END AS is_channel_account_flag,
    CASE 
        WHEN direct_booked_net_arr < channel_booked_net_arr
        AND channel_sourced_booked_net_arr > channel_cosell_booked_net_arr 
            THEN 1 
        ELSE 0 
    END AS is_channel_sourced_account_flag,
    CASE 
        WHEN direct_booked_net_arr < channel_booked_net_arr
        AND channel_sourced_booked_net_arr < channel_cosell_booked_net_arr 
            THEN 1 
        ELSE 0 
    END AS is_channel_cosell_account_flag
FROM base;