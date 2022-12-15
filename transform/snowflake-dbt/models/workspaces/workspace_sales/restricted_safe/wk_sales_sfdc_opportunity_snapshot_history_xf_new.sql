{{ config(alias='sfdc_opportunity_snapshot_history_xf_new') }}
-- TODO
-- Add CS churn fields into model from wk_sales_opportunity object

WITH date_details AS (

    SELECT *
    FROM {{ ref('wk_sales_date_details') }}
    -- FROM prod.workspace_sales.date_details

), sfdc_accounts_xf AS (

    SELECT *
    -- FROM PROD.restricted_safe_workspace_sales.sfdc_accounts_xf
    FROM {{ref('wk_sales_sfdc_accounts_xf')}}

), sfdc_opportunity_xf AS (

    SELECT 
      opportunity_id,
      owner_id,
      account_id,
      order_type_stamped,
      deal_category,
      opportunity_category,
      deal_group,
      opportunity_owner_manager,
      is_edu_oss,
      -- account_owner_team_stamped, 

      -- Opportunity Owner Stamped fields
      opportunity_owner_user_segment,
      opportunity_owner_user_region,
      opportunity_owner_user_area,
      opportunity_owner_user_geo,

      -------------------
      --  NF 2022-01-28 TO BE DEPRECATED once pipeline velocity reports in Sisense are updated
      sales_team_rd_asm_level,
      -------------------

      sales_team_cro_level,
      sales_team_vp_level,
      sales_team_avp_rd_level,
      sales_team_asm_level,

      -- this fields use the opportunity owner version for current FY and account fields for previous years
      report_opportunity_user_segment,
      report_opportunity_user_geo,
      report_opportunity_user_region,
      report_opportunity_user_area,
      report_user_segment_geo_region_area,
      report_user_segment_geo_region_area_sqs_ot,

      -- NF 2022-02-17 new aggregated keys 
      key_sqs,
      key_ot,

      key_segment,
      key_segment_sqs,                 
      key_segment_ot,    

      key_segment_geo,
      key_segment_geo_sqs,
      key_segment_geo_ot,      

      key_segment_geo_region,
      key_segment_geo_region_sqs,
      key_segment_geo_region_ot,   

      key_segment_geo_region_area,
      key_segment_geo_region_area_sqs,
      key_segment_geo_region_area_ot,

      key_segment_geo_area,
      
      -------------------------------------
      -- NF: These fields are not exposed yet in opty history, just for check
      -- I am adding this logic

      stage_1_date,
      stage_1_date_month,
      stage_1_fiscal_year,
      stage_1_fiscal_quarter_name,
      stage_1_fiscal_quarter_date,
      --------------------------------------

      is_won,
      is_duplicate_flag,
      raw_net_arr,
      sales_qualified_source

      -- Channel Org. fields
      -- this fields should be changed to this historical version
      --deal_path,
      --dr_partner_deal_type,
      --dr_partner_engagement,
      --partner_account,
      --dr_status,
      --distributor,
      --influence_partner,
      --fulfillment_partner,
      --platform_partner,
      --partner_track,
      --is_public_sector_opp,
      --is_registration_from_portal,
      --calculated_discount,
      --partner_discount,
      --partner_discount_calc,
      --comp_channel_neutral

    FROM {{ref('wk_sales_sfdc_opportunity_xf')}}

), sfdc_users_xf AS (

    SELECT * 
    FROM {{ref('wk_sales_sfdc_users_xf')}}  

), sfdc_opportunity_snapshot_history AS (

    SELECT 
      edm_snapshot_opty.crm_opportunity_snapshot_id                 AS opportunity_snapshot_id,
      edm_snapshot_opty.dim_crm_opportunity_id                      AS opportunity_id,
      edm_snapshot_opty.opportunity_name,
      edm_snapshot_opty.owner_id,
      edm_snapshot_opty.opportunity_owner_department,

      edm_snapshot_opty.close_date,
      edm_snapshot_opty.created_date,
      edm_snapshot_opty.sales_qualified_date,
      edm_snapshot_opty.sales_accepted_date,

      edm_snapshot_opty.opportunity_sales_development_representative,
      edm_snapshot_opty.opportunity_business_development_representative,
      edm_snapshot_opty.opportunity_development_representative,

      edm_snapshot_opty.order_type                                  AS snapshot_order_type_stamped,
      edm_snapshot_opty.sales_qualified_source_name                 AS snapshot_sales_qualified_source,
      edm_snapshot_opty.is_edu_oss                                  AS snapshot_is_edu_oss,
      edm_snapshot_opty.opportunity_category                        AS snapshot_opportunity_category,

      -- Accounts might get deleted or merged, I am selecting the latest account id from the opty object
      -- to avoid showing non-valid account ids
      edm_snapshot_opty.dim_crm_account_id                          AS raw_account_id,
      edm_snapshot_opty.raw_net_arr,
      edm_snapshot_opty.net_arr,

      edm_snapshot_opty.deployment_preference,
      edm_snapshot_opty.merged_opportunity_id,
      edm_snapshot_opty.sales_path,
      edm_snapshot_opty.sales_type,
      edm_snapshot_opty.stage_name,
      edm_snapshot_opty.competitors,
      edm_snapshot_opty.forecast_category_name,
      edm_snapshot_opty.invoice_number,
      edm_snapshot_opty.primary_campaign_source_id,
      edm_snapshot_opty.professional_services_value,
      edm_snapshot_opty.total_contract_value,
      edm_snapshot_opty.is_web_portal_purchase,
      edm_snapshot_opty.opportunity_term,
      edm_snapshot_opty.arr_basis,
      edm_snapshot_opty.arr,
      edm_snapshot_opty.amount,
      edm_snapshot_opty.recurring_amount,
      edm_snapshot_opty.true_up_amount,
      edm_snapshot_opty.proserv_amount,
      edm_snapshot_opty.renewal_amount,
      edm_snapshot_opty.other_non_recurring_amount,
      edm_snapshot_opty.subscription_start_date                    AS quote_start_date,
      edm_snapshot_opty.subscription_end_date                      AS quote_end_date,
      
      edm_snapshot_opty.cp_champion,
      edm_snapshot_opty.cp_close_plan,
      edm_snapshot_opty.cp_competition,
      edm_snapshot_opty.cp_decision_criteria,
      edm_snapshot_opty.cp_decision_process,
      edm_snapshot_opty.cp_economic_buyer,
      edm_snapshot_opty.cp_identify_pain,
      edm_snapshot_opty.cp_metrics,
      edm_snapshot_opty.cp_risks,
      edm_snapshot_opty.cp_use_cases,
      edm_snapshot_opty.cp_value_driver,
      edm_snapshot_opty.cp_why_do_anything_at_all,
      edm_snapshot_opty.cp_why_gitlab,
      edm_snapshot_opty.cp_why_now,
      edm_snapshot_opty.cp_score,

      edm_snapshot_opty.dbt_updated_at                            AS _last_dbt_run,
      edm_snapshot_opty.is_deleted,
      edm_snapshot_opty.last_activity_date,

      -- Channel Org. fields
      -- this fields should be changed to this historical version
      edm_snapshot_opty.deal_path_name                            AS deal_path,
      edm_snapshot_opty.dr_partner_deal_type,
      edm_snapshot_opty.dr_partner_engagement,
      edm_snapshot_opty.partner_account,
      edm_snapshot_opty.dr_status,
      edm_snapshot_opty.distributor,
      edm_snapshot_opty.influence_partner,
      edm_snapshot_opty.fulfillment_partner,
      edm_snapshot_opty.platform_partner,
      edm_snapshot_opty.partner_track,
      edm_snapshot_opty.is_public_sector_opp,
      edm_snapshot_opty.is_registration_from_portal,
      edm_snapshot_opty.calculated_discount,
      edm_snapshot_opty.partner_discount,
      edm_snapshot_opty.partner_discount_calc,
      edm_snapshot_opty.comp_channel_neutral,
      edm_snapshot_opty.fpa_master_bookings_flag,
      
      -- stage dates
      -- dates in stage fields
      edm_snapshot_opty.stage_0_pending_acceptance_date,
      edm_snapshot_opty.stage_1_discovery_date,
      edm_snapshot_opty.stage_2_scoping_date,
      edm_snapshot_opty.stage_3_technical_evaluation_date,
      edm_snapshot_opty.stage_4_proposal_date,
      edm_snapshot_opty.stage_5_negotiating_date,
      edm_snapshot_opty.stage_6_awaiting_signature_date_date      AS stage_6_awaiting_signature_date,
      edm_snapshot_opty.stage_6_closed_won_date,
      edm_snapshot_opty.stage_6_closed_lost_date,
      
      edm_snapshot_opty.deal_path_engagement,

      ------------------------------------------------------------------------------------------------------
      ------------------------------------------------------------------------------------------------------
      -- Base helpers for reporting
      edm_snapshot_opty.stage_name_3plus,
      edm_snapshot_opty.stage_name_4plus,
      edm_snapshot_opty.is_stage_1_plus,
      edm_snapshot_opty.is_stage_3_plus,
      edm_snapshot_opty.is_stage_4_plus,
      edm_snapshot_opty.is_won,
      edm_snapshot_opty.is_lost,
      edm_snapshot_opty.is_open,

      CASE edm_snapshot_opty.is_closed 
        WHEN TRUE THEN 1 
        ELSE 0 
      END                                                        AS is_closed,
      
      edm_snapshot_opty.is_renewal,
      edm_snapshot_opty.is_credit                                AS is_credit_flag,
      
      edm_snapshot_opty.is_refund,

      -- existing is_refund logic in XF
      -- CASE
      --   WHEN sfdc_opportunity_snapshot_history.opportunity_category IN ('Decommission')
      --     THEN 1
      --   ELSE 0
      -- END                                                          AS is_refund,
      
      edm_snapshot_opty.is_contract_reset                        AS is_contract_reset_flag,

      -- NF: 20210827 Fields for competitor analysis
      edm_snapshot_opty.competitors_other_flag,
      edm_snapshot_opty.competitors_gitlab_core_flag,
      edm_snapshot_opty.competitors_none_flag,
      edm_snapshot_opty.competitors_github_enterprise_flag,
      edm_snapshot_opty.competitors_bitbucket_server_flag,
      edm_snapshot_opty.competitors_unknown_flag,
      edm_snapshot_opty.competitors_github_flag,
      edm_snapshot_opty.competitors_gitlab_flag,
      edm_snapshot_opty.competitors_jenkins_flag,
      edm_snapshot_opty.competitors_azure_devops_flag,
      edm_snapshot_opty.competitors_svn_flag,
      edm_snapshot_opty.competitors_bitbucket_flag,
      edm_snapshot_opty.competitors_atlassian_flag,
      edm_snapshot_opty.competitors_perforce_flag,
      edm_snapshot_opty.competitors_visual_studio_flag,
      edm_snapshot_opty.competitors_azure_flag,
      edm_snapshot_opty.competitors_amazon_code_commit_flag,
      edm_snapshot_opty.competitors_circleci_flag,
      edm_snapshot_opty.competitors_bamboo_flag,
      edm_snapshot_opty.competitors_aws_flag,

      edm_snapshot_opty.stage_category,
      edm_snapshot_opty.calculated_deal_count                   AS calculated_deal_count,

      -- calculated age field
      -- if open, use the diff between created date and snapshot date
      -- if closed, a) the close date is later than snapshot date, use snapshot date
      -- if closed, b) the close is in the past, use close date
      edm_snapshot_opty.calculated_age_in_days,

      --date helpers
      edm_snapshot_opty.snapshot_date,
      edm_snapshot_opty.snapshot_month                          AS snapshot_date_month,
      edm_snapshot_opty.snapshot_fiscal_year,
      edm_snapshot_opty.snapshot_fiscal_quarter_name,
      edm_snapshot_opty.snapshot_fiscal_quarter_date,
      edm_snapshot_opty.snapshot_day_of_fiscal_quarter_normalised,
      edm_snapshot_opty.snapshot_day_of_fiscal_year_normalised,

      edm_snapshot_opty.close_month                             AS close_date_month,
      edm_snapshot_opty.close_fiscal_year,
      edm_snapshot_opty.close_fiscal_quarter_name,
      edm_snapshot_opty.close_fiscal_quarter_date,

      -- This refers to the closing quarter perspective instead of the snapshot quarter
      90 - DATEDIFF(day, edm_snapshot_opty.snapshot_date, close_date_detail.last_day_of_fiscal_quarter)           AS close_day_of_fiscal_quarter_normalised,

      edm_snapshot_opty.created_month                           AS created_date_month,
      edm_snapshot_opty.created_fiscal_year,
      edm_snapshot_opty.created_fiscal_quarter_name,
      edm_snapshot_opty.created_fiscal_quarter_date,

      edm_snapshot_opty.net_arr_created_date,
      edm_snapshot_opty.net_arr_created_month                   AS net_arr_created_date_month,
      edm_snapshot_opty.net_arr_created_fiscal_year,
      edm_snapshot_opty.net_arr_created_fiscal_quarter_name,
      edm_snapshot_opty.net_arr_created_fiscal_quarter_date,

      edm_snapshot_opty.pipeline_created_date,
      edm_snapshot_opty.pipeline_created_month                  AS pipeline_created_date_month,
      edm_snapshot_opty.pipeline_created_fiscal_year,
      edm_snapshot_opty.pipeline_created_fiscal_quarter_name,
      edm_snapshot_opty.pipeline_created_fiscal_quarter_date,

      edm_snapshot_opty.sales_accepted_month,
      edm_snapshot_opty.sales_accepted_fiscal_year,
      edm_snapshot_opty.sales_accepted_fiscal_quarter_name,
      edm_snapshot_opty.sales_accepted_fiscal_quarter_date,

      edm_snapshot_opty.lead_source,
      edm_snapshot_opty.net_new_source_categories,           
      edm_snapshot_opty.record_type_id,
      

      -- JK 2022-10-25 they are being calculated in a CTE later for now
      -- edm_snapshot_opty.churned_contraction_deal_count,
      -- edm_snapshot_opty.open_1plus_net_arr,
      -- edm_snapshot_opty.open_3plus_net_arr,
      -- edm_snapshot_opty.open_4plus_net_arr,
      -- edm_snapshot_opty.churned_contraction_net_arr,
      -- edm_snapshot_opty.booked_net_arr,
      -- edm_snapshot_opty.is_booked_net_arr                       AS is_booked_net_arr_flag,
      -- edm_snapshot_opty.is_eligible_asp_analysis                AS is_eligible_asp_analysis_flag,
      -- edm_snapshot_opty.is_eligible_age_analysis                AS is_eligible_age_analysis_flag,
      -- edm_snapshot_opty.is_eligible_churn_contraction           AS is_eligible_churn_contraction_flag,
      -- edm_snapshot_opty.is_excluded_from_pipeline_created       AS is_excluded_flag,

      -- CASE edm_snapshot_opty.is_sao 
      --   WHEN TRUE THEN 1 
      --   ELSE 0 
      -- END                                                        AS is_eligible_sao_flag,

      -- edm_snapshot_opty.created_in_snapshot_quarter_net_arr,
      -- edm_snapshot_opty.created_and_won_same_quarter_net_arr,

      -- edm_snapshot_opty.open_1plus_deal_count,
      -- edm_snapshot_opty.open_3plus_deal_count,
      -- edm_snapshot_opty.open_4plus_deal_count,
      -- edm_snapshot_opty.booked_deal_count,
      -- edm_snapshot_opty.deal_size,
      -- edm_snapshot_opty.calculated_deal_size,
      -- edm_snapshot_opty.is_eligible_open_pipeline               AS is_eligible_open_pipeline_flag,
      -- edm_snapshot_opty.created_in_snapshot_quarter_deal_count,

      edm_snapshot_opty.dim_crm_account_id                       AS account_id,
      edm_snapshot_opty.account_owner_team_stamped,
      edm_snapshot_opty.account_owner_team_stamped_cro_level,
      edm_snapshot_opty.crm_account_name                         AS account_name,
      
      edm_snapshot_opty.dim_parent_crm_account_id                AS ultimate_parent_account_id,
      edm_snapshot_opty.parent_crm_account_name                  AS ultimate_parent_account_name,
      edm_snapshot_opty.is_jihu_account,
      edm_snapshot_opty.account_owner_user_segment,
      edm_snapshot_opty.account_owner_user_geo,
      edm_snapshot_opty.account_owner_user_region,
      edm_snapshot_opty.account_owner_user_area,
      edm_snapshot_opty.account_demographics_segment,
      edm_snapshot_opty.account_demographics_geo,
      edm_snapshot_opty.account_demographics_region,
      edm_snapshot_opty.account_demographics_area,
      edm_snapshot_opty.account_demographics_territory
      

    FROM {{ref('mart_crm_opportunity_daily_snapshot')}} AS edm_snapshot_opty
    INNER JOIN date_details AS close_date_detail
      ON edm_snapshot_opty.close_date::DATE = close_date_detail.date_actual

), sfdc_opportunity_snapshot_history_xf AS (

    SELECT DISTINCT
      opp_snapshot.*,
      
      -- using current opportunity perspective instead of historical
      -- NF 2021-01-26: this might change to order type live 2.1    
      -- NF 2022-01-28: Update to OT 2.3 will be stamped directly  
      
      -- fields from sfdc_opportunity_xf 
      sfdc_opportunity_xf.order_type_stamped,     
      sfdc_opportunity_xf.is_duplicate_flag                               AS current_is_duplicate_flag,
      
      sfdc_opportunity_xf.opportunity_owner_user_segment,
      sfdc_opportunity_xf.opportunity_owner_user_region,
      sfdc_opportunity_xf.opportunity_owner_user_area,
      sfdc_opportunity_xf.opportunity_owner_user_geo,
      
      sfdc_opportunity_xf.sales_team_rd_asm_level,
      sfdc_opportunity_xf.sales_team_cro_level,
      sfdc_opportunity_xf.sales_team_vp_level,
      sfdc_opportunity_xf.sales_team_avp_rd_level,
      sfdc_opportunity_xf.sales_team_asm_level,

      sfdc_opportunity_xf.report_opportunity_user_segment,
      sfdc_opportunity_xf.report_opportunity_user_geo,
      sfdc_opportunity_xf.report_opportunity_user_region,
      sfdc_opportunity_xf.report_opportunity_user_area,
      sfdc_opportunity_xf.report_user_segment_geo_region_area,
      sfdc_opportunity_xf.report_user_segment_geo_region_area_sqs_ot,
      
      sfdc_opportunity_xf.key_sqs,
      sfdc_opportunity_xf.key_ot,
      sfdc_opportunity_xf.key_segment,
      sfdc_opportunity_xf.key_segment_sqs,
      sfdc_opportunity_xf.key_segment_ot,
      sfdc_opportunity_xf.key_segment_geo,
      sfdc_opportunity_xf.key_segment_geo_sqs,
      sfdc_opportunity_xf.key_segment_geo_ot,
      sfdc_opportunity_xf.key_segment_geo_region,
      sfdc_opportunity_xf.key_segment_geo_region_sqs,
      sfdc_opportunity_xf.key_segment_geo_region_ot,
      sfdc_opportunity_xf.key_segment_geo_region_area,
      sfdc_opportunity_xf.key_segment_geo_region_area_sqs,
      sfdc_opportunity_xf.key_segment_geo_region_area_ot,
      sfdc_opportunity_xf.key_segment_geo_area,
      sfdc_opportunity_xf.sales_qualified_source,
      sfdc_opportunity_xf.deal_category,
      sfdc_opportunity_xf.opportunity_category,
      sfdc_opportunity_xf.deal_group,
      sfdc_opportunity_xf.opportunity_owner_manager,
      sfdc_opportunity_xf.is_edu_oss,
      
      -- fields calculated with both live and snapshot fields
      CASE 
        WHEN sfdc_opportunity_xf.stage_1_date <= opp_snapshot.snapshot_date
          THEN sfdc_opportunity_xf.stage_1_date 
        ELSE NULL
      END                                               AS stage_1_date,

      CASE 
        WHEN sfdc_opportunity_xf.stage_1_date <= opp_snapshot.snapshot_date
          THEN sfdc_opportunity_xf.stage_1_date_month 
        ELSE NULL
      END                                               AS stage_1_date_month,

      CASE 
        WHEN sfdc_opportunity_xf.stage_1_date <= opp_snapshot.snapshot_date
          THEN sfdc_opportunity_xf.stage_1_fiscal_year 
        ELSE NULL
      END                                               AS stage_1_fiscal_year,

      CASE 
        WHEN sfdc_opportunity_xf.stage_1_date <= opp_snapshot.snapshot_date
          THEN sfdc_opportunity_xf.stage_1_fiscal_quarter_name 
        ELSE NULL
      END                                               AS stage_1_fiscal_quarter_name,

      CASE 
        WHEN sfdc_opportunity_xf.stage_1_date <= opp_snapshot.snapshot_date
          THEN sfdc_opportunity_xf.stage_1_fiscal_quarter_date 
        ELSE NULL
      END                                               AS stage_1_fiscal_quarter_date,
      
      ------------------------------------------------------------------------------------------------------
      ------------------------------------------------------------------------------------------------------

      -- account driven fields
      sfdc_accounts_xf.tsp_region,
      sfdc_accounts_xf.tsp_sub_region,
      sfdc_accounts_xf.ultimate_parent_sales_segment,
      sfdc_accounts_xf.tsp_max_hierarchy_sales_segment,

      opportunity_owner.name                                     AS opportunity_owner,
      
      sfdc_accounts_xf.ultimate_parent_id, -- same is ultimate_parent_account_id?

      upa.account_demographics_sales_segment                     AS upa_demographics_segment,
      upa.account_demographics_geo                               AS upa_demographics_geo,
      upa.account_demographics_region                            AS upa_demographics_region,
      upa.account_demographics_area                              AS upa_demographics_area,
      upa.account_demographics_territory                         AS upa_demographics_territory,

      opportunity_owner.is_rep_flag

      
    FROM sfdc_opportunity_snapshot_history AS opp_snapshot
    INNER JOIN sfdc_opportunity_xf    
      ON sfdc_opportunity_xf.opportunity_id = opp_snapshot.opportunity_id
    LEFT JOIN sfdc_accounts_xf
      ON sfdc_opportunity_xf.account_id = sfdc_accounts_xf.account_id 
    LEFT JOIN sfdc_accounts_xf AS upa
      ON upa.account_id = sfdc_accounts_xf.ultimate_parent_account_id
    LEFT JOIN sfdc_users_xf AS account_owner
      ON account_owner.user_id = sfdc_accounts_xf.owner_id
    LEFT JOIN sfdc_users_xf AS opportunity_owner
      ON opportunity_owner.user_id = opp_snapshot.owner_id
    
    WHERE opp_snapshot.raw_account_id NOT IN ('0014M00001kGcORQA0')                           -- remove test account
      AND (sfdc_accounts_xf.ultimate_parent_account_id NOT IN ('0016100001YUkWVAA1')
            OR sfdc_accounts_xf.account_id IS NULL)                                        -- remove test account
      AND opp_snapshot.is_deleted = 0
      -- NF 20210906 remove JiHu opties from the models
      AND sfdc_accounts_xf.is_jihu_account = 0

)
-- in Q2 FY21 a few deals where created in the wrong stage, and as they were purely aspirational, 
-- they needed to be removed from stage 1, eventually by the end of the quarter they were removed
-- The goal of this list is to use in the Created Pipeline flag, to exclude those deals that at 
-- day 90 had stages of less than 1, that should smooth the chart
, vision_opps  AS (
  
    SELECT 
      opp_snapshot.opportunity_id,
      opp_snapshot.stage_name,
      opp_snapshot.snapshot_fiscal_quarter_date
    FROM sfdc_opportunity_snapshot_history_xf opp_snapshot
    WHERE opp_snapshot.snapshot_fiscal_quarter_name = 'FY21-Q2'
      And opp_snapshot.pipeline_created_fiscal_quarter_date = opp_snapshot.snapshot_fiscal_quarter_date
      AND opp_snapshot.snapshot_day_of_fiscal_quarter_normalised = 90
      AND opp_snapshot.stage_name in ('00-Pre Opportunity', '0-Pending Acceptance', '0-Qualifying')
    GROUP BY 1, 2, 3


), add_compound_metrics AS (

    SELECT 
      opp_snapshot.*,
      CASE
        WHEN opp_snapshot.order_type_stamped IN ('1. New - First Order' ,'2. New - Connected', '3. Growth')
          AND opp_snapshot.is_edu_oss = 0
          AND opp_snapshot.pipeline_created_fiscal_quarter_date IS NOT NULL
          AND opp_snapshot.opportunity_category IN ('Standard','Internal Correction','Ramp Deal','Credit','Contract Reset')
          AND opp_snapshot.stage_name NOT IN ('00-Pre Opportunity','10-Duplicate', '9-Unqualified','0-Pending Acceptance')
          AND (opp_snapshot.net_arr > 0
            OR opp_snapshot.opportunity_category = 'Credit')
          -- exclude vision opps from FY21-Q2
          AND (opp_snapshot.pipeline_created_fiscal_quarter_name != 'FY21-Q2'
                OR vision_opps.opportunity_id IS NULL)
          -- 20220128 Updated to remove webdirect SQS deals
          AND opp_snapshot.sales_qualified_source  != 'Web Direct Generated'
              THEN 1
         ELSE 0
      END                                                      AS is_eligible_created_pipeline_flag,

      -- 20201021 NF: This should be replaced by a table that keeps track of excluded deals for forecasting purposes
      CASE 
        WHEN opp_snapshot.ultimate_parent_id IN ('001610000111bA3','0016100001F4xla','0016100001CXGCs','00161000015O9Yn','0016100001b9Jsc') 
          AND opp_snapshot.close_date < '2020-08-01' 
            THEN 1
        -- NF 2021 - Pubsec extreme deals
        WHEN opp_snapshot.opportunity_id IN ('0064M00000WtZKUQA3','0064M00000Xb975QAB')
          AND opp_snapshot.snapshot_date < '2021-05-01' 
          THEN 1
        -- exclude vision opps from FY21-Q2
        WHEN opp_snapshot.pipeline_created_fiscal_quarter_name = 'FY21-Q2'
            AND vision_opps.opportunity_id IS NOT NULL
          THEN 1
        -- NF 20220415 PubSec duplicated deals on Pipe Gen -- Lockheed Martin GV - 40000 Ultimate Renewal
        WHEN opp_snapshot.opportunity_id IN ('0064M00000ZGpfQQAT','0064M00000ZGpfVQAT','0064M00000ZGpfGQAT')
          THEN 1
        ELSE 0
      END                                                         AS is_excluded_flag,

      CASE 
        WHEN lower(opp_snapshot.deal_group) LIKE ANY ('%growth%', '%new%')
          AND opp_snapshot.is_edu_oss = 0
          AND opp_snapshot.is_stage_1_plus = 1
          AND opp_snapshot.forecast_category_name != 'Omitted'
          AND opp_snapshot.is_open = 1
         THEN 1
         ELSE 0
      END                                                   AS is_eligible_open_pipeline_flag

    FROM sfdc_opportunity_snapshot_history_xf opp_snapshot
    LEFT JOIN vision_opps
      ON vision_opps.opportunity_id = opp_snapshot.opportunity_id
      AND vision_opps.snapshot_fiscal_quarter_date = opp_snapshot.snapshot_fiscal_quarter_date


/*
  JK 2022-12-15: The following fields in the below CTE are calculated within the model instead of taking the fields 
  directly from EDM mart. There have been some discrepancies potentially due to the usage of unadjusted nARR in the mart. 
*/

), temp_calculations AS (

    SELECT
      *,

      CASE
        WHEN (
            is_won = 1 
            OR (is_renewal = 1 AND is_lost = 1)
            )
          THEN net_arr
        ELSE 0 
      END                                                   AS booked_net_arr,

      CASE
        WHEN is_edu_oss = 0
          AND is_deleted = 0
          AND (is_won = 1 
              OR (is_renewal = 1 AND is_lost = 1))
          AND order_type_stamped IN ('1. New - First Order','2. New - Connected','3. Growth','4. Contraction','6. Churn - Final','5. Churn - Partial')
          -- Not JiHu
            THEN 1
        ELSE 0
      END                                                   AS is_booked_net_arr_flag,

      CASE 
        WHEN is_eligible_open_pipeline_flag = 1
          THEN net_arr
        ELSE 0                                                                                              
      END                                                   AS open_1plus_net_arr,

      CASE 
        WHEN is_eligible_open_pipeline_flag = 1
          AND is_stage_3_plus = 1   
            THEN net_arr
        ELSE 0
      END                                                   AS open_3plus_net_arr,

      CASE 
        WHEN is_eligible_open_pipeline_flag = 1  
          AND is_stage_4_plus = 1
            THEN net_arr
        ELSE 0
      END                                                   AS open_4plus_net_arr,

      CASE
        WHEN ((is_renewal = 1
            AND is_lost = 1)
            OR is_won = 1 )
            AND order_type_stamped IN ('5. Churn - Partial' ,'6. Churn - Final', '4. Contraction')
        THEN net_arr
        ELSE 0
      END                                                   AS churned_contraction_net_arr,

      CASE
        WHEN pipeline_created_fiscal_quarter_name = snapshot_fiscal_quarter_name
          AND is_eligible_created_pipeline_flag = 1
            THEN net_arr
        ELSE 0 
      END                                                 AS created_in_snapshot_quarter_net_arr,

      CASE 
        WHEN pipeline_created_fiscal_quarter_name = close_fiscal_quarter_name
           AND is_won = 1
           AND is_eligible_created_pipeline_flag = 1
            THEN net_arr
        ELSE 0
      END                                                 AS created_and_won_same_quarter_net_arr,

      CASE 
        WHEN is_eligible_open_pipeline_flag = 1
          AND is_stage_1_plus = 1
            THEN calculated_deal_count  
        ELSE 0                                                                                              
      END                                               AS open_1plus_deal_count,

      CASE 
        WHEN is_eligible_open_pipeline_flag = 1
          AND is_stage_3_plus = 1
            THEN calculated_deal_count
        ELSE 0
      END                                               AS open_3plus_deal_count,

      CASE 
        WHEN is_eligible_open_pipeline_flag = 1
          AND is_stage_4_plus = 1
            THEN calculated_deal_count
        ELSE 0
      END                                               AS open_4plus_deal_count,

      CASE 
        WHEN is_won = 1
          THEN calculated_deal_count
        ELSE 0
      END                                               AS booked_deal_count,

      CASE
        WHEN ((is_renewal = 1
            AND is_lost = 1)
            OR is_won = 1 )
            AND order_type_stamped IN ('5. Churn - Partial' ,'6. Churn - Final', '4. Contraction')
        THEN calculated_deal_count
        ELSE 0
      END                                                   AS churned_contraction_deal_count,

      CASE
        WHEN pipeline_created_fiscal_quarter_name = snapshot_fiscal_quarter_name
          AND is_eligible_created_pipeline_flag = 1
            THEN calculated_deal_count
        ELSE 0 
      END                                                 AS created_in_snapshot_quarter_deal_count,

      -- ASP Analysis eligibility issue: https://gitlab.com/gitlab-com/sales-team/field-operations/sales-operations/-/issues/2606
      CASE 
        WHEN is_edu_oss = 0
          AND is_deleted = 0
          -- For ASP we care mainly about add on, new business, excluding contraction / churn
          AND order_type_stamped IN ('1. New - First Order','2. New - Connected','3. Growth')
          -- Exclude Decomissioned as they are not aligned to the real owner
          -- Contract Reset, Decomission
          AND opportunity_category IN ('Standard','Ramp Deal','Internal Correction')
          -- Exclude Deals with nARR < 0
          AND net_arr > 0
          -- Not JiHu
            THEN 1
          ELSE 0
      END                                                   AS is_eligible_asp_analysis_flag,

       -- Age eligibility issue: https://gitlab.com/gitlab-com/sales-team/field-operations/sales-operations/-/issues/2606
      CASE 
        WHEN is_edu_oss = 0
          AND is_deleted = 0
          -- Renewals are not having the same motion as rest of deals
          AND is_renewal = 0
          -- For stage age we exclude only ps/other
          AND order_type_stamped IN ('1. New - First Order','2. New - Connected','3. Growth','4. Contraction','6. Churn - Final','5. Churn - Partial')
          -- Only include deal types with meaningful journeys through the stages
          AND opportunity_category IN ('Standard','Ramp Deal','Decommissioned')
          -- Web Purchase have a different dynamic and should not be included
          AND is_web_portal_purchase = 0
          -- Not JiHu
            THEN 1
          ELSE 0
      END                                                   AS is_eligible_age_analysis_flag,

      CASE
        WHEN is_edu_oss = 0
          AND is_deleted = 0
          AND order_type_stamped IN ('4. Contraction','6. Churn - Final','5. Churn - Partial')
          -- Not JiHu
            THEN 1
          ELSE 0
      END                                                   AS is_eligible_churn_contraction_flag,

      -- SAO alignment issue: https://gitlab.com/gitlab-com/sales-team/field-operations/sales-operations/-/issues/2656
      -- 2022-08-23 JK: using the central is_sao logic
      CASE
        WHEN sales_accepted_date IS NOT NULL
          AND is_edu_oss = 0
          AND stage_name NOT IN ('10-Duplicate')
            THEN 1
        ELSE 0
      END                                                   AS is_eligible_sao_flag,

      CASE 
        WHEN net_arr > 0 AND net_arr < 5000 
          THEN '1 - Small (<5k)'
        WHEN net_arr >=5000 AND net_arr < 25000 
          THEN '2 - Medium (5k - 25k)'
        WHEN net_arr >=25000 AND net_arr < 100000 
          THEN '3 - Big (25k - 100k)'
        WHEN net_arr >= 100000 
          THEN '4 - Jumbo (>100k)'
        ELSE 'Other' 
      END                                                          AS deal_size,

      -- extended version of the deal size
      CASE 
        WHEN net_arr > 0 AND net_arr < 1000 
          THEN '1. (0k -1k)'
        WHEN net_arr >=1000 AND net_arr < 10000 
          THEN '2. (1k - 10k)'
        WHEN net_arr >=10000 AND net_arr < 50000 
          THEN '3. (10k - 50k)'
        WHEN net_arr >=50000 AND net_arr < 100000 
          THEN '4. (50k - 100k)'
        WHEN net_arr >= 100000 AND net_arr < 250000 
          THEN '5. (100k - 250k)'
        WHEN net_arr >= 250000 AND net_arr < 500000 
          THEN '6. (250k - 500k)'
        WHEN net_arr >= 500000 AND net_arr < 1000000 
          THEN '7. (500k-1000k)'
        WHEN net_arr >= 1000000 
          THEN '8. (>1000k)'
        ELSE 'Other' 
      END                                                           AS calculated_deal_size

    FROM add_compound_metrics
)

SELECT *
FROM temp_calculations

