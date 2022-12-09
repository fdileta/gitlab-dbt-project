{{ config(alias='sfdc_opportunity_snapshot_history_xf_edm_marts') }}
-- TODO
-- Add CS churn fields into model from wk_sales_opportunity object

WITH date_details AS (

    SELECT *
    FROM {{ ref('dim_date') }}


), edm_account_live AS (

    SELECT *
    FROM {{ ref('mart_crm_account') }}


), edm_user_live AS (

    SELECT * 
    FROM {{ref('dim_crm_user')}}

), edm_snapshot_opty AS (

   SELECT *
   FROM {{ref('mart_crm_opportunity_snapshot_vs_live')}}

), vision_opps  AS (
  
  SELECT 
    edm_snapshot_opty.dim_crm_opportunity_id,
    edm_snapshot_opty.stage_name_snapshot,
    edm_snapshot_opty.snapshot_fiscal_quarter_date
  FROM edm_snapshot_opty
  WHERE edm_snapshot_opty.snapshot_fiscal_quarter_name = 'FY21-Q2'
    AND edm_snapshot_opty.pipeline_created_fiscal_quarter_date = edm_snapshot_opty.snapshot_fiscal_quarter_date
    AND edm_snapshot_opty.snapshot_day_of_fiscal_quarter_normalised = 90
    AND edm_snapshot_opty.stage_name_snapshot in ('00-Pre Opportunity', '0-Pending Acceptance', '0-Qualifying')
  GROUP BY 1, 2, 3


-- all the fields are sourcing from edm opp snapshot
), edm_snapshot AS (
    SELECT 
      edm_snapshot_opty.crm_opportunity_snapshot_id                                         AS opportunity_snapshot_id,
      edm_snapshot_opty.dim_crm_opportunity_id                                              AS opportunity_id,
      edm_snapshot_opty.opportunity_name_snapshot                                           AS opportunity_name,
      edm_snapshot_opty.owner_id_snapshot                                                   AS owner_id,
      edm_snapshot_opty.opportunity_owner_department_snapshot                               AS opportunity_owner_department,

      edm_snapshot_opty.close_date_snapshot                                                 AS close_date,
      edm_snapshot_opty.created_date_snapshot                                               AS created_date,
      edm_snapshot_opty.sales_qualified_date_snapshot                                       AS sales_qualified_date,
      edm_snapshot_opty.sales_accepted_date_snapshot                                        AS sales_accepted_date,

      edm_snapshot_opty.opportunity_sales_development_representative_snapshot               AS opportunity_sales_development_representative,
      edm_snapshot_opty.opportunity_business_development_representative_snapshot            AS opportunity_business_development_representative,
      edm_snapshot_opty.opportunity_development_representative_snapshot                     AS opportunity_development_representative,

      -- sfdc_opportunity_snapshot_history.order_type_stamped          AS snapshot_order_type_stamped,
      edm_snapshot_opty.order_type_snapshot                                                 AS snapshot_order_type_stamped,
      edm_snapshot_opty.sales_qualified_source_name_snapshot                                AS snapshot_sales_qualified_source,
      edm_snapshot_opty.is_edu_oss_snapshot                                                 AS snapshot_is_edu_oss,
      edm_snapshot_opty.opportunity_category_snapshot                                       AS snapshot_opportunity_category,

      -- Accounts might get deleted or merged, I am selecting the latest account id from the opty object
      -- to avoid showing non-valid account ids
      edm_snapshot_opty.dim_crm_account_id_snapshot                                         AS raw_account_id,
      edm_snapshot_opty.raw_net_arr_snapshot                                                AS raw_net_arr,
      edm_snapshot_opty.net_arr_snapshot                                                    AS net_arr,
      edm_snapshot_opty.calculated_from_ratio_net_arr_snapshot                              AS calculated_from_ratio_net_arr,
      edm_snapshot_opty.opportunity_based_iacv_to_net_arr_ratio_snapshot                    AS opportunity_based_iacv_to_net_arr_ratio,
      edm_snapshot_opty.segment_order_type_iacv_to_net_arr_ratio_snapshot                   AS segment_order_type_iacv_to_net_arr_ratio,

      edm_snapshot_opty.incremental_acv_snapshot                                            AS incremental_acv,
      edm_snapshot_opty.net_incremental_acv_snapshot                                        AS net_incremental_acv,

      edm_snapshot_opty.source_buckets_snapshot                                             AS source_buckets,
      edm_snapshot_opty.deployment_preference_snapshot                                      AS deployment_preference,
      edm_snapshot_opty.merged_opportunity_id_snapshot                                      AS merged_opportunity_id,
      edm_snapshot_opty.sales_path_snapshot                                                 AS sales_path,
      edm_snapshot_opty.sales_type_snapshot                                                 AS sales_type,
      edm_snapshot_opty.stage_name_snapshot                                                 AS stage_name,
      edm_snapshot_opty.competitors_snapshot                                                AS competitors,
      edm_snapshot_opty.forecast_category_name_snapshot                                     AS forecast_category_name,
      edm_snapshot_opty.invoice_number_snapshot                                             AS invoice_number,
      edm_snapshot_opty.primary_campaign_source_id_snapshot                                 AS primary_campaign_source_id,
      edm_snapshot_opty.professional_services_value_snapshot                                AS professional_services_value,
      edm_snapshot_opty.total_contract_value_snapshot                                       AS total_contract_value,
      edm_snapshot_opty.is_web_portal_purchase_snapshot                                     AS is_web_portal_purchase,
      edm_snapshot_opty.opportunity_term_snapshot                                           AS opportunity_term,
      edm_snapshot_opty.arr_basis_snapshot                                                  AS arr_basis,
      edm_snapshot_opty.arr_snapshot                                                        AS arr,
      edm_snapshot_opty.amount_snapshot                                                     AS amount,
      edm_snapshot_opty.recurring_amount_snapshot                                           AS recurring_amount,
      edm_snapshot_opty.true_up_amount_snapshot                                             AS true_up_amount,
      edm_snapshot_opty.proserv_amount_snapshot                                             AS proserv_amount,
      edm_snapshot_opty.renewal_amount_snapshot                                             AS renewal_amount,
      edm_snapshot_opty.other_non_recurring_amount_snapshot                                 AS other_non_recurring_amount,
      edm_snapshot_opty.subscription_start_date_snapshot                                    AS quote_start_date,
      edm_snapshot_opty.subscription_end_date_snapshot                                      AS quote_end_date,
      
      edm_snapshot_opty.cp_champion_snapshot                                                AS cp_champion,
      edm_snapshot_opty.cp_close_plan_snapshot                                              AS cp_close_plan,
      edm_snapshot_opty.cp_competition_snapshot                                             AS cp_competition,
      edm_snapshot_opty.cp_decision_criteria_snapshot                                       AS cp_decision_criteria,
      edm_snapshot_opty.cp_decision_process_snapshot                                        AS cp_decision_process,
      edm_snapshot_opty.cp_economic_buyer_snapshot                                          AS cp_economic_buyer,
      edm_snapshot_opty.cp_identify_pain_snapshot                                           AS cp_identify_pain,
      edm_snapshot_opty.cp_metrics_snapshot                                                 AS cp_metrics,
      edm_snapshot_opty.cp_risks_snapshot                                                   AS cp_risks,
      edm_snapshot_opty.cp_use_cases_snapshot                                               AS cp_use_cases,
      edm_snapshot_opty.cp_value_driver_snapshot                                            AS cp_value_driver,
      edm_snapshot_opty.cp_why_do_anything_at_all_snapshot                                  AS cp_why_do_anything_at_all,
      edm_snapshot_opty.cp_why_gitlab_snapshot                                              AS cp_why_gitlab,
      edm_snapshot_opty.cp_why_now_snapshot                                                 AS cp_why_now,
      edm_snapshot_opty.cp_score_snapshot                                                   AS cp_score,

      edm_snapshot_opty.dbt_updated_at_snapshot                                             AS _last_dbt_run,
      edm_snapshot_opty.is_deleted_snapshot                                                 AS is_deleted,
      edm_snapshot_opty.last_activity_date_snapshot                                         AS last_activity_date,

      -- Channel Org. fields
      -- this fields should be changed to this historical version
      edm_snapshot_opty.deal_path_name_snapshot                                             AS deal_path,
      edm_snapshot_opty.dr_partner_deal_type_snapshot                                       AS dr_partner_deal_type,
      edm_snapshot_opty.dr_partner_engagement_snapshot                                      AS dr_partner_engagement,
      edm_snapshot_opty.partner_account_snapshot                                            AS partner_account,
      edm_snapshot_opty.dr_status_snapshot                                                  AS dr_status,
      edm_snapshot_opty.distributor_snapshot                                                AS distributor,
      edm_snapshot_opty.influence_partner_snapshot                                          AS influence_partner,
      edm_snapshot_opty.fulfillment_partner_snapshot                                        AS fulfillment_partner,
      edm_snapshot_opty.platform_partner_snapshot                                           AS platform_partner,
      edm_snapshot_opty.partner_track_snapshot                                              AS partner_track,
      edm_snapshot_opty.is_public_sector_opp_snapshot                                       AS is_public_sector_opp,
      edm_snapshot_opty.is_registration_from_portal_snapshot                                AS is_registration_from_portal,
      edm_snapshot_opty.calculated_discount_snapshot                                        AS calculated_discount,
      edm_snapshot_opty.partner_discount_snapshot                                           AS partner_discount,
      edm_snapshot_opty.partner_discount_calc_snapshot                                      AS partner_discount_calc,
      edm_snapshot_opty.comp_channel_neutral_snapshot                                       AS comp_channel_neutral,
      edm_snapshot_opty.fpa_master_bookings_flag_snapshot                                   AS fpa_master_bookings_flag,
      
      -- stage dates
      -- dates in stage fields
      edm_snapshot_opty.stage_0_pending_acceptance_date_snapshot                            AS stage_0_pending_acceptance_date,
      edm_snapshot_opty.stage_1_discovery_date_snapshot                                     AS stage_1_discovery_date,
      edm_snapshot_opty.stage_2_scoping_date_snapshot                                       AS stage_2_scoping_date,
      edm_snapshot_opty.stage_3_technical_evaluation_date_snapshot                          AS stage_3_technical_evaluation_date,
      edm_snapshot_opty.stage_4_proposal_date_snapshot                                      AS stage_4_proposal_date,
      edm_snapshot_opty.stage_5_negotiating_date_snapshot                                   AS stage_5_negotiating_date,
      edm_snapshot_opty.stage_6_awaiting_signature_date_snapshot                            AS stage_6_awaiting_signature_date,
      edm_snapshot_opty.stage_6_closed_won_date_snapshot                                    AS stage_6_closed_won_date,
      edm_snapshot_opty.stage_6_closed_lost_date_snapshot                                   AS stage_6_closed_lost_date,
      
      edm_snapshot_opty.deal_path_engagement_snapshot                                       AS deal_path_engagement,

      ------------------------------------------------------------------------------------------------------
      ------------------------------------------------------------------------------------------------------
      -- Base helpers for reporting
      edm_snapshot_opty.stage_name_3plus_snapshot                                           AS stage_name_3plus,
      edm_snapshot_opty.stage_name_4plus_snapshot                                           AS stage_name_4plus,
      edm_snapshot_opty.is_stage_1_plus_snapshot                                            AS is_stage_1_plus,
      edm_snapshot_opty.is_stage_3_plus_snapshot                                            AS is_stage_3_plus,
      edm_snapshot_opty.is_stage_4_plus_snapshot                                            AS is_stage_4_plus,
      CASE edm_snapshot_opty.is_won_snapshot 
        WHEN TRUE THEN 1 
        ELSE 0 
      END                                                                                   AS is_won,
      edm_snapshot_opty.is_lost_snapshot                                                    AS is_lost,
      edm_snapshot_opty.is_open_snapshot                                                    AS is_open,
      edm_snapshot_opty.is_closed_snapshot                                                  AS is_closed,
      edm_snapshot_opty.is_renewal_snapshot                                                 AS is_renewal,

      edm_snapshot_opty.is_credit_snapshot                                                  AS is_credit_flag,
      edm_snapshot_opty.is_refund_snapshot                                                  AS is_refund,
      edm_snapshot_opty.is_contract_reset_snapshot                                          AS is_contract_reset_flag,

      -- NF: 20210827 Fields for competitor analysis
      edm_snapshot_opty.competitors_other_flag_snapshot                                     AS competitors_other_flag,
      edm_snapshot_opty.competitors_gitlab_core_flag_snapshot                               AS competitors_gitlab_core_flag,
      edm_snapshot_opty.competitors_none_flag_snapshot                                      AS competitors_none_flag,
      edm_snapshot_opty.competitors_github_enterprise_flag_snapshot                         AS competitors_github_enterprise_flag,
      edm_snapshot_opty.competitors_bitbucket_server_flag_snapshot                          AS competitors_bitbucket_server_flag,
      edm_snapshot_opty.competitors_unknown_flag_snapshot                                   AS competitors_unknown_flag,
      edm_snapshot_opty.competitors_github_flag_snapshot                                    AS competitors_github_flag,
      edm_snapshot_opty.competitors_gitlab_flag_snapshot                                    AS competitors_gitlab_flag,
      edm_snapshot_opty.competitors_jenkins_flag_snapshot                                   AS competitors_jenkins_flag,
      edm_snapshot_opty.competitors_azure_devops_flag_snapshot                              AS competitors_azure_devops_flag,
      edm_snapshot_opty.competitors_svn_flag_snapshot                                       AS competitors_svn_flag,
      edm_snapshot_opty.competitors_bitbucket_flag_snapshot                                 AS competitors_bitbucket_flag,
      edm_snapshot_opty.competitors_atlassian_flag_snapshot                                 AS competitors_atlassian_flag,
      edm_snapshot_opty.competitors_perforce_flag_snapshot                                  AS competitors_perforce_flag,
      edm_snapshot_opty.competitors_visual_studio_flag_snapshot                             AS competitors_visual_studio_flag,
      edm_snapshot_opty.competitors_azure_flag_snapshot                                     AS competitors_azure_flag,
      edm_snapshot_opty.competitors_amazon_code_commit_flag_snapshot                        AS competitors_amazon_code_commit_flag,
      edm_snapshot_opty.competitors_circleci_flag_snapshot                                  AS competitors_circleci_flag,
      edm_snapshot_opty.competitors_bamboo_flag_snapshot                                    AS competitors_bamboo_flag,
      edm_snapshot_opty.competitors_aws_flag_snapshot                                       AS competitors_aws_flag,

      edm_snapshot_opty.stage_category_snapshot                                             AS stage_category,
      edm_snapshot_opty.calculated_deal_count_snapshot                                      AS calculated_deal_count,
      -- calculated age field
      -- if open, use the diff between created date and snapshot date
      -- if closed, a) the close date is later than snapshot date, use snapshot date
      -- if closed, b) the close is in the past, use close date
      edm_snapshot_opty.calculated_age_in_days_snapshot                                     AS calculated_age_in_days,

      --date helpers
      edm_snapshot_opty.snapshot_date,
      edm_snapshot_opty.snapshot_month_snapshot                                             AS snapshot_date_month,
      edm_snapshot_opty.snapshot_fiscal_year,
      edm_snapshot_opty.snapshot_fiscal_quarter_name,
      edm_snapshot_opty.snapshot_fiscal_quarter_date,
      edm_snapshot_opty.snapshot_day_of_fiscal_quarter_normalised,
      edm_snapshot_opty.snapshot_day_of_fiscal_year_normalised,

      edm_snapshot_opty.close_month_snapshot                                                AS close_date_month,
      edm_snapshot_opty.close_fiscal_year_snapshot                                          AS close_fiscal_year,
      edm_snapshot_opty.close_fiscal_quarter_name_snapshot                                  AS close_fiscal_quarter_name,
      edm_snapshot_opty.close_fiscal_quarter_date_snapshot                                  AS close_fiscal_quarter_date,

      -- This refers to the closing quarter perspective instead of the snapshot quarter
      90 - DATEDIFF(day, edm_snapshot_opty.snapshot_date, close_date_detail.last_day_of_fiscal_quarter)           AS close_day_of_fiscal_quarter_normalised,

      edm_snapshot_opty.created_month_snapshot                                              AS created_date_month,
      edm_snapshot_opty.created_fiscal_year                                                 AS created_fiscal_year,
      edm_snapshot_opty.created_fiscal_quarter_name                                         AS created_fiscal_quarter_name,
      edm_snapshot_opty.created_fiscal_quarter_date                                         AS created_fiscal_quarter_date,

      edm_snapshot_opty.net_arr_created_date_snapshot                                       AS net_arr_created_date,
      edm_snapshot_opty.net_arr_created_month_snapshot                                      AS net_arr_created_date_month,
      edm_snapshot_opty.net_arr_created_fiscal_year                                         AS net_arr_created_fiscal_year,
      edm_snapshot_opty.net_arr_created_fiscal_quarter_name                                 AS net_arr_created_fiscal_quarter_name,
      edm_snapshot_opty.net_arr_created_fiscal_quarter_date                                 AS net_arr_created_fiscal_quarter_date,

      edm_snapshot_opty.pipeline_created_date_snapshot                                      AS pipeline_created_date,
      edm_snapshot_opty.pipeline_created_month_snapshot                                     AS pipeline_created_date_month,
      edm_snapshot_opty.pipeline_created_fiscal_year_snapshot                               AS pipeline_created_fiscal_year,
      edm_snapshot_opty.pipeline_created_fiscal_quarter_name_snapshot                       AS pipeline_created_fiscal_quarter_name,
      edm_snapshot_opty.pipeline_created_fiscal_quarter_date_snapshot                       AS pipeline_created_fiscal_quarter_date,

      edm_snapshot_opty.arr_created_date_snapshot                                           AS iacv_created_date,
      edm_snapshot_opty.arr_created_month_snapshot                                          AS iacv_created_date_month,
      edm_snapshot_opty.arr_created_fiscal_year_snapshot                                    AS iacv_created_fiscal_year,
      edm_snapshot_opty.arr_created_fiscal_quarter_name_snapshot                            AS iacv_created_fiscal_quarter_name,
      edm_snapshot_opty.arr_created_fiscal_quarter_date_snapshot                            AS iacv_created_fiscal_quarter_date,

      edm_snapshot_opty.sales_accepted_month_snapshot                                       AS sales_accepted_month,
      edm_snapshot_opty.sales_accepted_fiscal_year_snapshot                                 AS sales_accepted_fiscal_year,
      edm_snapshot_opty.sales_accepted_fiscal_quarter_name_snapshot                         AS sales_accepted_fiscal_quarter_name,
      edm_snapshot_opty.sales_accepted_fiscal_quarter_date_snapshot                         AS sales_accepted_fiscal_quarter_date,

    --------------------------------------------

      edm_snapshot_opty.lead_source_snapshot                                                AS lead_source,
      edm_snapshot_opty.net_new_source_categories_snapshot                                  AS net_new_source_categories,
      edm_snapshot_opty.record_type_id_snapshot                                             AS record_type_id,
      
      --edm_snapshot_opty.deal_size_snapshot                                                  AS deal_size,
      --edm_snapshot_opty.calculated_deal_size_snapshot                                       AS calculated_deal_size,
      --edm_snapshot_opty.is_eligible_open_pipeline_snapshot                                  AS is_eligible_open_pipeline_flag,
      --edm_snapshot_opty.is_eligible_asp_analysis_snapshot                                   AS is_eligible_asp_analysis_flag,
      --edm_snapshot_opty.is_eligible_age_analysis_snapshot                                   AS is_eligible_age_analysis_flag,
      --edm_snapshot_opty.is_booked_net_arr_snapshot                                          AS is_booked_net_arr_flag,
      --edm_snapshot_opty.is_eligible_churn_contraction_snapshot                              AS is_eligible_churn_contraction_flag,
      --edm_snapshot_opty.created_in_snapshot_quarter_net_arr_snapshot                        AS created_in_snapshot_quarter_net_arr,
      --edm_snapshot_opty.created_and_won_same_quarter_net_arr_snapshot                       AS created_and_won_same_quarter_net_arr,
      --edm_snapshot_opty.created_in_snapshot_quarter_deal_count_snapshot                     AS created_in_snapshot_quarter_deal_count,
      --edm_snapshot_opty.open_1plus_deal_count_snapshot                                      AS open_1plus_deal_count,
      --edm_snapshot_opty.open_3plus_deal_count_snapshot                                      AS open_3plus_deal_count,
      --edm_snapshot_opty.open_4plus_deal_count_snapshot                                      AS open_4plus_deal_count,
      edm_snapshot_opty.booked_deal_count_snapshot                                          AS booked_deal_count,
      -- JK 2022-10-25 they are being calculated in a CTE later for now
      --edm_snapshot_opty.churned_contraction_deal_count_snapshot                             AS churned_contraction_deal_count,
      --edm_snapshot_opty.open_1plus_net_arr_snapshot                                         AS open_1plus_net_arr,
      --edm_snapshot_opty.open_3plus_net_arr_snapshot                                         AS open_3plus_net_arr,
      --edm_snapshot_opty.open_4plus_net_arr_snapshot                                         AS open_4plus_net_arr,
      --edm_snapshot_opty.churned_contraction_net_arr_snapshot                                AS churned_contraction_net_arr,
      --edm_snapshot_opty.booked_net_arr_snapshot                                             AS booked_net_arr,
      --edm_snapshot_opty.is_excluded_from_pipeline_created_snapshot                          AS is_excluded_flag,

            ------------------------------
      -- compound metrics for reporting
      ------------------------------

      -- current deal size field, it was creasted by the data team and the original doesn't work
      CASE 
        WHEN edm_snapshot_opty.net_arr_snapshot > 0 AND edm_snapshot_opty.net_arr_snapshot < 5000 
          THEN '1 - Small (<5k)'
        WHEN edm_snapshot_opty.net_arr_snapshot >=5000 AND net_arr < 25000 
          THEN '2 - Medium (5k - 25k)'
        WHEN edm_snapshot_opty.net_arr_snapshot >=25000 AND net_arr < 100000 
          THEN '3 - Big (25k - 100k)'
        WHEN edm_snapshot_opty.net_arr_snapshot >= 100000 
          THEN '4 - Jumbo (>100k)'
        ELSE 'Other' 
      END                                                                                   AS deal_size,

      -- extended version of the deal size
      CASE 
        WHEN edm_snapshot_opty.net_arr_snapshot > 0 
          AND edm_snapshot_opty.net_arr_snapshot < 1000 
            THEN '1. (0k -1k)'
        WHEN edm_snapshot_opty.net_arr_snapshot >=1000 
          AND net_arr < 10000 
            THEN '2. (1k - 10k)'
        WHEN edm_snapshot_opty.net_arr_snapshot >=10000 
          AND edm_snapshot_opty.net_arr_snapshot < 50000 
            THEN '3. (10k - 50k)'
        WHEN edm_snapshot_opty.net_arr_snapshot >=50000 
          AND edm_snapshot_opty.net_arr_snapshot < 100000 
            THEN '4. (50k - 100k)'
        WHEN edm_snapshot_opty.net_arr_snapshot >= 100000 
          AND edm_snapshot_opty.net_arr_snapshot < 250000 
            THEN '5. (100k - 250k)'
        WHEN edm_snapshot_opty.net_arr_snapshot >= 250000 
          AND edm_snapshot_opty.net_arr_snapshot < 500000 
            THEN '6. (250k - 500k)'
        WHEN edm_snapshot_opty.net_arr_snapshot >= 500000 
          AND edm_snapshot_opty.net_arr_snapshot < 1000000 
            THEN '7. (500k-1000k)'
        WHEN edm_snapshot_opty.net_arr_snapshot >= 1000000 
          THEN '8. (>1000k)'
        ELSE 'Other' 
      END                                                                                   AS calculated_deal_size,

      -- Open pipeline eligibility definition
      CASE 
        WHEN LOWER(edm_snapshot_opty.deal_group_live) LIKE ANY ('%growth%', '%new%')
          AND edm_snapshot_opty.is_edu_oss_live = 0
          AND edm_snapshot_opty.is_stage_1_plus_snapshot = 1
          AND edm_snapshot_opty.forecast_category_name_snapshot != 'Omitted'
          AND edm_snapshot_opty.is_open_snapshot = 1
         THEN 1
         ELSE 0
      END                                                                                   AS is_eligible_open_pipeline_flag,

      -- Created pipeline eligibility definition
      CASE 
        WHEN edm_snapshot_opty.order_type_live IN ('1. New - First Order' ,'2. New - Connected', '3. Growth')
          AND edm_snapshot_opty.is_edu_oss_live = 0
          AND edm_snapshot_opty.pipeline_created_fiscal_quarter_date_snapshot IS NOT NULL
          AND edm_snapshot_opty.opportunity_category_live IN ('Standard','Internal Correction','Ramp Deal','Credit','Contract Reset')  
          AND edm_snapshot_opty.stage_name_snapshot NOT IN ('00-Pre Opportunity','10-Duplicate', '9-Unqualified','0-Pending Acceptance')
          AND (edm_snapshot_opty.net_arr_snapshot > 0 
            OR edm_snapshot_opty.opportunity_category_live = 'Credit')
          -- exclude vision opps from FY21-Q2
          AND (edm_snapshot_opty.pipeline_created_fiscal_quarter_name_snapshot != 'FY21-Q2'
                OR vision_opps.opportunity_id IS NULL)
          -- 20220128 Updated to remove webdirect SQS deals 
          AND edm_snapshot_opty.sales_qualified_source_live != 'Web Direct Generated'
              THEN 1
         ELSE 0
      END                                                                                   AS is_eligible_created_pipeline_flag,

      -- SAO alignment issue: https://gitlab.com/gitlab-com/sales-team/field-operations/sales-operations/-/issues/2656
      -- 2022-08-23 JK: using the central is_sao logic
      CASE
        WHEN edm_snapshot_opty.sales_accepted_date_snapshot IS NOT NULL
          AND edm_snapshot_opty.is_edu_oss_live = 0
          AND edm_snapshot_opty.stage_name_snapshot NOT IN ('10-Duplicate')
            THEN 1
        ELSE 0
      END                                                                                   AS is_eligible_sao_flag,

      -- ASP Analysis eligibility issue: https://gitlab.com/gitlab-com/sales-team/field-operations/sales-operations/-/issues/2606
      CASE 
        WHEN edm_snapshot_opty.is_edu_oss_live = 0
          AND edm_snapshot_opty.is_deleted_snapshot = 0
          -- For ASP we care mainly about add on, new business, excluding contraction / churn
          AND edm_snapshot_opty.order_type_live IN ('1. New - First Order','2. New - Connected','3. Growth')
          -- Exclude Decomissioned as they are not aligned to the real owner
          -- Contract Reset, Decomission
          AND edm_snapshot_opty.opportunity_category_live IN ('Standard','Ramp Deal','Internal Correction')
          -- Exclude Deals with nARR < 0
          AND edm_snapshot_opty.net_arr_snapshot > 0
          -- Not JiHu
            THEN 1
          ELSE 0
      END                                                                                   AS is_eligible_asp_analysis_flag,
      
      -- Age eligibility issue: https://gitlab.com/gitlab-com/sales-team/field-operations/sales-operations/-/issues/2606
      CASE 
        WHEN edm_snapshot_opty.is_edu_oss_live = 0
          AND edm_snapshot_opty.is_deleted_snapshot = 0
          -- Renewals are not having the same motion as rest of deals
          AND edm_snapshot_opty.is_renewal_snapshot = 0
          -- For stage age we exclude only ps/other
          AND edm_snapshot_opty.order_type_live IN ('1. New - First Order','2. New - Connected','3. Growth','4. Contraction','6. Churn - Final','5. Churn - Partial')
          -- Only include deal types with meaningful journeys through the stages
          AND edm_snapshot_opty.opportunity_category_live IN ('Standard','Ramp Deal','Decommissioned')
          -- Web Purchase have a different dynamic and should not be included
          AND edm_snapshot_opty.is_web_portal_purchase_snapshot = 0
          -- Not JiHu
            THEN 1
          ELSE 0
      END                                                                                   AS is_eligible_age_analysis_flag,

      -- TODO: This is the same as FP&A Boookings Flag
      CASE
        WHEN edm_snapshot_opty.is_edu_oss_live = 0
          AND edm_snapshot_opty.is_deleted_snapshot = 0
          AND (edm_snapshot_opty.is_won_snapshot = 1 
              OR (edm_snapshot_opty.is_renewal_snapshot = 1 AND edm_snapshot_opty.is_lost_snapshot = 1))
          AND edm_snapshot_opty.order_type_live IN ('1. New - First Order','2. New - Connected','3. Growth','4. Contraction','6. Churn - Final','5. Churn - Partial')
          -- Not JiHu
            THEN 1
          ELSE 0
      END                                                                                  AS is_booked_net_arr_flag,

      CASE
        WHEN edm_snapshot_opty.is_edu_oss_live = 0
          AND edm_snapshot_opty.is_deleted_snapshot = 0
          AND edm_snapshot_opty.order_type_live IN ('4. Contraction','6. Churn - Final','5. Churn - Partial')
          -- Not JiHu
            THEN 1
          ELSE 0
      END                                                                                   AS is_eligible_churn_contraction_flag,

      -- created within quarter
      CASE
        WHEN edm_snapshot_opty.pipeline_created_fiscal_quarter_name_snapshot = edm_snapshot_opty.snapshot_fiscal_quarter_name
          AND is_eligible_created_pipeline_flag = 1
            THEN edm_snapshot_opty.net_arr_snapshot
        ELSE 0 
      END                                                                                   AS created_in_snapshot_quarter_net_arr,

   -- created and closed within the quarter net arr
      CASE 
        WHEN edm_snapshot_opty.pipeline_created_fiscal_quarter_name_snapshot = edm_snapshot_opty.close_fiscal_quarter_name_snapshot
           AND edm_snapshot_opty.is_won_snapshot = 1
           AND is_eligible_created_pipeline_flag = 1
            THEN edm_snapshot_opty.net_arr_snapshot
        ELSE 0
      END                                                                                   AS created_and_won_same_quarter_net_arr,


      CASE
        WHEN edm_snapshot_opty.pipeline_created_fiscal_quarter_name_snapshot = edm_snapshot_opty.snapshot_fiscal_quarter_name
          AND is_eligible_created_pipeline_flag = 1
            THEN edm_snapshot_opty.calculated_deal_count_snapshot
        ELSE 0 
      END                                                                                   AS created_in_snapshot_quarter_deal_count,

      -- Fields created to simplify report building down the road. Specially the pipeline velocity.

      -- deal count
      CASE 
        WHEN is_eligible_open_pipeline_flag = 1
          AND edm_snapshot_opty.is_stage_1_plus_snapshot = 1
            THEN edm_snapshot_opty.calculated_deal_count_snapshot
        ELSE 0                                                                                              
      END                                                                                   AS open_1plus_deal_count,

      CASE 
        WHEN is_eligible_open_pipeline_flag = 1
          AND edm_snapshot_opty.is_stage_3_plus_snapshot = 1
            THEN edm_snapshot_opty.calculated_deal_count_snapshot
        ELSE 0
      END                                                                                   AS open_3plus_deal_count,

      CASE 
        WHEN is_eligible_open_pipeline_flag = 1
          AND edm_snapshot_opty.is_stage_4_plus_snapshot = 1
            THEN edm_snapshot_opty.calculated_deal_count_snapshot
        ELSE 0
      END                                                                                   AS open_4plus_deal_count,

      -- booked deal count
      CASE 
        WHEN edm_snapshot_opty.is_won_snapshot = 1
          THEN edm_snapshot_opty.calculated_deal_count_snapshot
        ELSE 0
      END                                                                                   AS booked_deal_count,
    
      -- churned contraction deal count as OT
      CASE
        WHEN ((edm_snapshot_opty.is_renewal_snapshot = 1
            AND edm_snapshot_optyt.is_lost_snapshot = 1)
            OR edm_snapshot_opty.is_won_snapshot = 1 )
            AND edm_snapshot_opty.order_type_live IN ('5. Churn - Partial' ,'6. Churn - Final', '4. Contraction')
        THEN edm_snapshot_opty.calculated_deal_count_snapshot
        ELSE 0
      END                                                                                   AS churned_contraction_deal_count,

      -----------------
      -- Net ARR

      CASE 
        WHEN is_eligible_open_pipeline_flag = 1
          THEN edm_snapshot_opty.net_arr_snapshot
        ELSE 0
      END                                                                                   AS open_1plus_net_arr,

      CASE 
        WHEN is_eligible_open_pipeline_flag = 1
          AND edm_snapshot_opty.is_stage_3_plus_snapshot = 1
            THEN edm_snapshot_opty.net_arr_snapshot
        ELSE 0
      END                                                                                   AS open_3plus_net_arr,
  
      CASE 
        WHEN is_eligible_open_pipeline_flag = 1  
          AND edm_snapshot_opty.is_stage_4_plus_snapshot = 1
            THEN edm_snapshot_opty.net_arr_snapshot
        ELSE 0
      END                                                                                   AS open_4plus_net_arr,

      -- booked net arr (won + renewals / lost)
      CASE
        WHEN (edm_snapshot_opty.is_won_snapshot = 1 
            OR (edm_snapshot_opty.is_renewal_snapshot = 1 
                  AND edm_snapshot_opty.is_lost_snapshot = 1))
          THEN edm_snapshot_opty.net_arr_snapshot
        ELSE 0 
      END                                                 AS booked_net_arr,

      -- churned contraction deal count as OT
      CASE
        WHEN ((edm_snapshot_opty.is_renewal_snapshot = 1
            AND edm_snapshot_opty.is_lost_snapshot = 1)
            OR edm_snapshot_opty.is_won_snapshot = 1 )
            AND edm_snapshot_opty.order_type_live IN ('5. Churn - Partial' ,'6. Churn - Final', '4. Contraction')
        THEN net_arr
        ELSE 0
      END                                                 AS churned_contraction_net_arr,

      -- 20201021 NF: This should be replaced by a table that keeps track of excluded deals for forecasting purposes
      CASE 
        WHEN LEFT(edm_account_live.dim_parent_crm_account_id,15) IN ('001610000111bA3','0016100001F4xla','0016100001CXGCs','00161000015O9Yn','0016100001b9Jsc') 
          AND edm_snapshot_opty.close_date_snapshot < '2020-08-01' 
            THEN 1
        -- NF 2021 - Pubsec extreme deals
        WHEN edm_snapshot_opty.dim_crm_opportunity_id IN ('0064M00000WtZKUQA3','0064M00000Xb975QAB')
          AND edm_snapshot_opty.snapshot_date < '2021-05-01' 
          THEN 1
        -- exclude vision opps from FY21-Q2
        WHEN edm_snapshot_opty.pipeline_created_fiscal_quarter_name_snapshot = 'FY21-Q2'
                AND vision_opps.dim_crm_opportunity_id IS NOT NULL
          THEN 1
        -- NF 20220415 PubSec duplicated deals on Pipe Gen -- Lockheed Martin GV - 40000 Ultimate Renewal
        WHEN edm_snapshot_opty.dim_crm_opportunity_id IN ('0064M00000ZGpfQQAT','0064M00000ZGpfVQAT','0064M00000ZGpfGQAT')
          THEN 1
      
        ELSE 0
      END                                                         AS is_excluded_flag,

      --------------------------------

      edm_snapshot_opty.is_edu_oss_snapshot                                                 AS is_edu_oss,
      --edm_snapshot_opty.sales_qualified_source_name             AS sales_qualified_source,
      edm_snapshot_opty.dim_crm_account_id_snapshot                                         AS account_id,
      edm_snapshot_opty.opportunity_category_snapshot                                       AS opportunity_category,

      edm_snapshot_opty.account_owner_team_stamped_snapshot                                 AS account_owner_team_stamped,
      edm_snapshot_opty.account_owner_team_stamped_cro_level_snapshot                       AS account_owner_team_stamped_cro_level,
      edm_snapshot_opty.stage_1_discovery_date_snapshot                                     AS stage_1_date,
      edm_snapshot_opty.stage_1_discovery_month_snapshot                                    AS stage_1_date_month,
      edm_snapshot_opty.stage_1_discovery_fiscal_year_snapshot                              AS stage_1_fiscal_year,
      edm_snapshot_opty.stage_1_discovery_fiscal_quarter_name_snapshot                      AS stage_1_fiscal_quarter_name,
      edm_snapshot_opty.stage_1_discovery_fiscal_quarter_date_snapshot                      AS stage_1_fiscal_quarter_date,
      -- CASE edm_snapshot_opty.is_sao_snapshot
      --   WHEN TRUE THEN 1 
      --   ELSE 0 
      -- END                                                                                   AS is_eligible_sao_flag,

      edm_snapshot_opty.crm_account_name_live                                               AS account_name,
      -- double check regarding parent crm account = ultimate parent account?
      edm_snapshot_opty.is_jihu_account_live                                                AS is_jihu_account,
      edm_snapshot_opty.account_owner_user_segment_live                                     AS account_owner_user_segment,
      edm_snapshot_opty.account_owner_user_geo_live                                         AS account_owner_user_geo,
      edm_snapshot_opty.account_owner_user_region_live                                      AS account_owner_user_region,
      edm_snapshot_opty.account_owner_user_area_live                                        AS account_owner_user_area,
      edm_snapshot_opty.account_demographics_segment_live                                   AS account_demographics_segment,
      edm_snapshot_opty.account_demographics_geo_live                                       AS account_demographics_geo,
      edm_snapshot_opty.account_demographics_region_live                                    AS account_demographics_region,
      edm_snapshot_opty.account_demographics_area_live                                      AS account_demographics_area,
      edm_snapshot_opty.account_demographics_territory_live                                 AS account_demographics_territory,
      
      edm_account_live.dim_parent_crm_account_id                                            AS ultimate_parent_account_id,
      LEFT(edm_account_live.dim_parent_crm_account_id,15)                                   AS ultimate_parent_id, -- same is ultimate_parent_account_id?
      edm_account_live.crm_account_tsp_region                                               AS tsp_region,
      edm_account_live.crm_account_tsp_sub_region                                           AS tsp_sub_region,
      edm_account_live.parent_crm_account_sales_segment                                     AS ultimate_parent_sales_segment,
      edm_account_live.tsp_max_hierarchy_sales_segment,
      -- account_owner_subarea_stamped

      edm_snapshot_opty.order_type_live                                                     AS order_type_stamped,

      -- duplicates flag
      edm_snapshot_opty.is_duplicate _live                                                  AS current_is_duplicate_flag,

      -- JK 2022-10-25: using live fields instead of the edm snapshot opp table directly
      opportunity_owner.user_name                                                           AS opportunity_owner,

      edm_snapshot_opty.opportunity_owner_user_segment_live                                 AS opportunity_owner_user_segment,
      edm_snapshot_opty.opportunity_owner_user_region_live                                  AS opportunity_owner_user_region,
      edm_snapshot_opty.opportunity_owner_user_area_live                                    AS opportunity_owner_user_area,
      edm_snapshot_opty.opportunity_owner_user_geo_live                                     AS opportunity_owner_user_geo,
      edm_snapshot_opty.opportunity_owner_manager_live                                      AS opportunity_owner_manager,
      edm_snapshot_opty.sales_team_rd_asm_level_live                                        AS sales_team_rd_asm_level,
      edm_snapshot_opty.sales_team_cro_level_live                                           AS sales_team_cro_level,
      edm_snapshot_opty.sales_team_vp_level_live                                            AS sales_team_vp_level,
      edm_snapshot_opty.sales_team_avp_rd_level_live                                        AS sales_team_avp_rd_level,
      edm_snapshot_opty.sales_team_asm_level_live                                           AS sales_team_asm_level,
      edm_snapshot_opty.report_opportunity_user_segment_live                                AS report_opportunity_user_segment,
      edm_snapshot_opty.report_opportunity_user_geo_live                                    AS report_opportunity_user_geo,
      edm_snapshot_opty.report_opportunity_user_region_live                                 AS report_opportunity_user_region,
      edm_snapshot_opty.report_opportunity_user_area_live                                   AS report_opportunity_user_area,
      edm_snapshot_opty.report_user_segment_geo_region_area_live                            AS report_user_segment_geo_region_area,
      edm_snapshot_opty.report_user_segment_geo_region_area_sqs_ot_live                     AS report_user_segment_geo_region_area_sqs_ot,
      edm_snapshot_opty.key_sqs_live                                                        AS key_sqs,
      edm_snapshot_opty.key_ot_live                                                         AS key_ot,
      edm_snapshot_opty.key_segment_live                                                    AS key_segment,
      edm_snapshot_opty.key_segment_sqs_live                                                AS key_segment_sqs,
      edm_snapshot_opty.key_segment_ot_live                                                 AS key_segment_ot,
      edm_snapshot_opty.key_segment_geo_live                                                AS key_segment_geo,
      edm_snapshot_opty.key_segment_geo_sqs_live                                            AS key_segment_geo_sqs,
      edm_snapshot_opty.key_segment_geo_ot_live                                             AS key_segment_geo_ot,
      edm_snapshot_opty.key_segment_geo_region_live                                         AS key_segment_geo_region,
      edm_snapshot_opty.key_segment_geo_region_sqs_live                                     AS key_segment_geo_region_sqs,
      edm_snapshot_opty.key_segment_geo_region_ot_live                                      AS key_segment_geo_region_ot,
      edm_snapshot_opty.key_segment_geo_region_area_live                                    AS key_segment_geo_region_area,
      edm_snapshot_opty.key_segment_geo_region_area_sqs_live                                AS key_segment_geo_region_area_sqs,
      edm_snapshot_opty.key_segment_geo_region_area_ot_live                                 AS key_segment_geo_region_area_ot,
      edm_snapshot_opty.key_segment_geo_area_live                                           AS key_segment_geo_area,
      edm_snapshot_opty.sales_qualified_source_name_live                                    AS sales_qualified_source,
      edm_snapshot_opty.deal_group_live                                                     AS deal_group,
      edm_snapshot_opty.deal_category_live                                                  AS deal_category,

      upa.crm_account_name                                                                  AS ultimate_parent_account_name,

      upa.account_demographics_sales_segment                                                AS upa_demographics_segment,
      upa.account_demographics_geo                                                          AS upa_demographics_geo,
      upa.account_demographics_region                                                       AS upa_demographics_region,
      upa.account_demographics_area                                                         AS upa_demographics_area,
      upa.account_demographics_territory                                                    AS upa_demographics_territory,

      opportunity_owner.is_rep_flag

    FROM edm_snapshot_opty
    INNER JOIN date_details AS close_date_detail
      ON edm_snapshot_opty.close_date::DATE = close_date_detail.date_actual
    LEFT JOIN edm_account_live
      ON edm_snapshot_opty.dim_crm_account_id = edm_account_live.dim_crm_account_id
    LEFT JOIN edm_account_live AS upa
      ON upa.dim_crm_account_id = edm_account_live.dim_parent_crm_account_id
    LEFT JOIN edm_user_live opportunity_owner 
      ON edm_snapshot_opty.owner_id = opportunity_owner.dim_crm_user_id
    LEFT JOIN vision_opps
      ON vision_opps.dim_crm_opportunity_id = edm_snapshot_opty.dim_crm_opportunity_id
        AND vision_opps.snapshot_fiscal_quarter_date = edm_snapshot_opty.snapshot_fiscal_quarter_date
    WHERE edm_snapshot_opty.dim_crm_account_id NOT IN ('0014M00001kGcORQA0')  -- remove test account
      AND (edm_account_live.dim_parent_crm_account_id NOT IN ('0016100001YUkWVAA1')
            OR edm_account_live.dim_crm_account_id IS NULL) -- remove test account
      AND edm_snapshot_opty.is_deleted = 0
      -- NF 20210906 remove JiHu opties from the models
      AND edm_account_live.is_jihu_account = 0


)

SELECT *
FROM  edm_snapshot
