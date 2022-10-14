WITH sfdc_opportunity_xf AS (
  
  SELECT 
        o.*,
        p.account_owner AS partner_account_owner,
        f.account_owner AS resale_partner_owner,
        COALESCE(p.billing_country,f.billing_country) AS partner_country,
        a.billing_country AS account_country
        
  FROM nfiguera_prod.restricted_safe_workspace_sales.sfdc_opportunity_xf o
    LEFT JOIN nfiguera_prod.restricted_safe_legacy.sfdc_accounts_xf p
        ON o.partner_account = p.account_id
    LEFT JOIN nfiguera_prod.restricted_safe_legacy.sfdc_accounts_xf f
        ON o.resale_partner_id = f.account_id
    LEFT JOIN nfiguera_prod.restricted_safe_legacy.sfdc_accounts_xf a
        ON a.account_id = o.account_id
  WHERE o.is_edu_oss = 0
      AND o.is_deleted = 0
      AND o.close_fiscal_year = 2022
      --AND booked_net_arr > 0
      AND o.is_web_portal_purchase = 0
      AND o.is_duplicate_flag = 0
      AND o.is_won = 1
      --AND opportunity_category in ('Standard','Decommissioned')
      --AND sales_team_cro_level not in ('Other')
      --AND order_type_stamped IN ('1. New - First Order','2. New - Connected','3. Growth')
      --AND net_arr <= 250000
    
      -- exclusions
      AND opportunity_id NOT IN ('0064M00000ZJ1C2QAL','0064M00000XZEgZQAX') -- Wrong opty category for debooked deal https://gitlab.my.salesforce.com/0064M00000XZEgZQAX 
      AND opportunity_id != '0064M00000ZFEX3QAP' -- Ent - EAST - Paycom Debooked deal https://gitlab.my.salesforce.com/0064M00000ZFEX3QAP
      AND opportunity_id != '0064M00000ZsI4KQAV' -- Ent - EMEA Thales Debooked deal https://gitlab.my.salesforce.com/0064M00000ZsI4K
      AND opportunity_id != '0064M00000YMyKTQA1' -- PubSec USSCOM Debooked deal https://gitlab.my.salesforce.com/0064M00000YMyKTQA1
      AND opportunity_id != '0064M00000Zr0W4QAJ' -- Ent - EAST HERE Debooked deal https://gitlab.my.salesforce.com/0064M00000Zr0W4QAJ
      AND opportunity_id != '0064M00000ZGP6tQAH' -- Dr First MM - Debooked deal https://gitlab.my.salesforce.com/0064M00000ZGP6tQAH
      AND opportunity_id != '0064M00000ZHS5PQAX' -- CGI Inc. Debooked deal https://gitlab.my.salesforce.com/0064M00000ZHS5PQAX
  
)
SELECT account_id,
  account_name,
  ultimate_parent_account_id,
  --ultimate_parent_account_name,
  coalesce(partner_account,resale_partner_id)           AS dr_resale_partner_id,
  coalesce(partner_account_name,resale_partner_name)    AS dr_resale_partner_name,
  partner_account_name,
  resale_partner_name,
  partner_account,
  resale_partner_id,
  resale_partner_owner
  partner_account_owner,
  partner_country,
  account_country,
  opportunity_id,
  opportunity_name,
  opportunity_owner,
  sales_team_cro_level,
  sales_team_rd_asm_level,
  order_type_stamped,
  sales_type,
  opportunity_category,
  sales_qualified_source,
  stage_name,
  calculated_deal_size as deal_size,
  -- dates
  created_date,
  close_date,
  sales_qualified_date,
  stage_1_discovery_date,
  sales_accepted_date,
  net_arr_created_date,
  --channel fields
  calculated_partner_track,
  deal_path,
  deal_path_engagement,
  partner_gitlab_program,
  -- metrics
  net_arr,
  booked_net_arr,
  calculated_deal_count,
  CASE 
    WHEN sales_type = 'Renewal'
      THEN datediff(day,net_arr_created_date, close_date) 
    ELSE datediff(day,COALESCE(stage_1_discovery_date,sales_accepted_date, net_arr_created_date, created_date), close_date)
  END                       AS calculated_age,
  is_eligible_asp_analysis_flag,
  is_eligible_age_analysis_flag
      
  
FROM sfdc_opportunity_xf