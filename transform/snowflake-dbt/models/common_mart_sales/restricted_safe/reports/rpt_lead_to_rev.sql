 {{ simple_cte([
    ('mart_crm_opportunity','mart_crm_opportunity'),
    ('mart_crm_person','mart_crm_person'),
    ('dim_crm_account','dim_crm_account'),
    ('rpt_crm_touchpoint_combined', 'rpt_crm_touchpoint_combined'), 
    ('dim_date', 'dim_date'),
    ('map_alternative_lead_demographics','map_alternative_lead_demographics'),
    ('dim_crm_user', 'dim_crm_user')
]) }}

, upa_base AS ( 
    SELECT 
      dim_parent_crm_account_id,
      dim_crm_account_id
    FROM dim_crm_account

), first_order_opps AS ( 

    SELECT
      dim_parent_crm_account_id,
      dim_crm_account_id,
      dim_crm_opportunity_id,
      close_date,
      is_sao,
      sales_accepted_date
    FROM mart_crm_opportunity
    WHERE is_won = true
      AND order_type = '1. New - First Order'

), accounts_with_first_order_opps AS ( 

    SELECT
      upa_base.dim_parent_crm_account_id,
      upa_base.dim_crm_account_id,
      first_order_opps.dim_crm_opportunity_id,
      FALSE AS is_first_order_available
    FROM upa_base 
    LEFT JOIN first_order_opps
      ON upa_base.dim_crm_account_id = first_order_opps.dim_crm_account_id
    WHERE dim_crm_opportunity_id IS NOT NULL

), person_order_type_base AS (

    SELECT DISTINCT
      mart_crm_person.email_hash, 
      mart_crm_person.sfdc_record_id,
      mart_crm_person.dim_crm_account_id,
      mart_crm_person.mql_date_lastest_pt,
      upa_base.dim_parent_crm_account_id,
      mart_crm_opportunity.dim_crm_opportunity_id,
      mart_crm_opportunity.close_date,
      mart_crm_opportunity.order_type,
      CASE 
        WHEN is_first_order_available = False AND mart_crm_opportunity.order_type = '1. New - First Order' 
          THEN '3. Growth'
        WHEN is_first_order_available = False AND mart_crm_opportunity.order_type != '1. New - First Order' 
          THEN mart_crm_opportunity.order_type
        ELSE '1. New - First Order'
      END AS person_order_type,
      ROW_NUMBER() OVER( PARTITION BY email_hash ORDER BY person_order_type) AS person_order_type_number
    FROM mart_crm_person
    FULL JOIN upa_base
      ON mart_crm_person.dim_crm_account_id = upa_base.dim_crm_account_id
    LEFT JOIN accounts_with_first_order_opps
      ON upa_base.dim_parent_crm_account_id = accounts_with_first_order_opps.dim_parent_crm_account_id
    FULL JOIN mart_crm_opportunity
      ON upa_base.dim_parent_crm_account_id = mart_crm_opportunity.dim_parent_crm_account_id

), person_order_type_final AS (

    SELECT DISTINCT
      person_order_type_base.email_hash,
      person_order_type_base.sfdc_record_id,
      person_order_type_base.mql_date_lastest_pt,
      person_order_type_base.dim_crm_opportunity_id,
      person_order_type_base.close_date,
      person_order_type_base.order_type,
      person_order_type_base.dim_parent_crm_account_id,
      person_order_type_base.dim_crm_account_id,
      person_order_type_base.person_order_type
    FROM person_order_type_base
    WHERE person_order_type_number = 1

), mql_order_type_base AS (

    SELECT DISTINCT
      mart_crm_person.sfdc_record_id,
      mart_crm_person.email_hash, 
      CASE 
        WHEN mql_date_lastest_pt < mart_crm_opportunity.close_date 
          THEN mart_crm_opportunity.order_type
        WHEN mql_date_lastest_pt > mart_crm_opportunity.close_date 
          THEN '3. Growth'
        ELSE NULL
      END AS mql_order_type_historical,
      ROW_NUMBER() OVER( PARTITION BY mart_crm_person.email_hash ORDER BY mql_order_type_historical) AS mql_order_type_number
    FROM mart_crm_person
    FULL JOIN upa_base 
      ON mart_crm_person.dim_crm_account_id = upa_base.dim_crm_account_id
    LEFT JOIN accounts_with_first_order_opps 
      ON upa_base.dim_parent_crm_account_id = accounts_with_first_order_opps.dim_parent_crm_account_id
    FULL JOIN mart_crm_opportunity 
      ON upa_base.dim_parent_crm_account_id = mart_crm_opportunity.dim_parent_crm_account_id
    
), mql_order_type_final AS (
  
  SELECT *
  FROM mql_order_type_base
  WHERE mql_order_type_number = 1
    
), inquiry_order_type_base AS (

    SELECT DISTINCT
      mart_crm_person.sfdc_record_id,
      mart_crm_person.email_hash, 
      CASE 
         WHEN true_inquiry_date < mart_crm_opportunity.close_date THEN mart_crm_opportunity.order_type
         WHEN true_inquiry_date > mart_crm_opportunity.close_date THEN '3. Growth'
      ELSE NULL
      END AS inquiry_order_type_historical,
      ROW_NUMBER() OVER( PARTITION BY mart_crm_person.email_hash ORDER BY inquiry_order_type_historical) AS inquiry_order_type_number
    FROM mart_crm_person
    FULL JOIN upa_base 
      ON mart_crm_person.dim_crm_account_id = upa_base.dim_crm_account_id
    LEFT JOIN accounts_with_first_order_opps 
      ON upa_base.dim_parent_crm_account_id = accounts_with_first_order_opps.dim_parent_crm_account_id
    FULL JOIN mart_crm_opportunity 
      ON upa_base.dim_parent_crm_account_id = mart_crm_opportunity.dim_parent_crm_account_id

), inquiry_order_type_final AS (
  
  SELECT *
  FROM inquiry_order_type_base
  WHERE inquiry_order_type_number=1
  
), order_type_final AS (
  
  SELECT 
    person_order_type_final.sfdc_record_id,
    person_order_type_final.email_hash,
    person_order_type_final.dim_crm_account_id,
    person_order_type_final.mql_date_lastest_pt,
    person_order_type_final.close_date,
    person_order_type_final.dim_parent_crm_account_id,
    person_order_type_final.dim_crm_opportunity_id,
    person_order_type_final.order_type,
    person_order_type_final.person_order_type,
    inquiry_order_type_final.inquiry_order_type_historical,
    mql_order_type_final.mql_order_type_historical
  FROM person_order_type_final
  LEFT JOIN inquiry_order_type_final 
    ON person_order_type_final.email_hash=inquiry_order_type_final.email_hash
  LEFT JOIN mql_order_type_final 
    ON person_order_type_final.email_hash=mql_order_type_final.email_hash

  ), cohort_base AS (

    SELECT DISTINCT
      mart_crm_person.email_hash,
      mart_crm_person.email_domain_type,
      mart_crm_person.is_valuable_signup,
      mart_crm_person.true_inquiry_date,
      mart_crm_person.mql_date_lastest_pt,
      mart_crm_person.status,
      mart_crm_person.lead_source,
      mart_crm_person.dim_crm_person_id,
      mart_crm_person.dim_crm_account_id,
      mart_crm_person.dim_crm_user_id,
      mart_crm_person.mql_date_first_pt,
      mart_crm_person.created_date_pt,
      mart_crm_person.created_date,
      mart_crm_person.mql_date_first,
      mart_crm_person.lead_created_date,
      mart_crm_person.lead_created_date_pt,
      mart_crm_person.contact_created_date,
      mart_crm_person.contact_created_date_pt,
      mart_crm_person.inquiry_date,
      mart_crm_person.inquiry_date_pt,
      mart_crm_person.inquiry_inferred_date,
      mart_crm_person.inquiry_inferred_date_pt,
      mart_crm_person.accepted_date,
      mart_crm_person.accepted_date_pt,
      mart_crm_person.qualifying_date,
      mart_crm_person.qualifying_date_pt,
      mart_crm_person.converted_date,
      mart_crm_person.converted_date_pt,
      mart_crm_person.worked_date,
      mart_crm_person.worked_date_pt,
      mart_crm_person.email_domain,
      mart_crm_person.was_converted_lead,
      mart_crm_person.source_buckets,
      mart_crm_person.crm_partner_id,
      mart_crm_person.partner_prospect_id,
      mart_crm_person.sales_segment_name AS person_sales_segment_name,
      mart_crm_person.sales_segment_grouped AS person_sales_segment_grouped,
      mart_crm_person.person_score,
      mart_crm_person.behavior_score,
      mart_crm_person.marketo_last_interesting_moment,
      mart_crm_person.marketo_last_interesting_moment_date,
      mart_crm_person.account_demographics_segment_region_grouped,
      mart_crm_person.account_demographics_upa_country,
      mart_crm_person.account_demographics_upa_state,
      mart_crm_person.account_demographics_upa_postal_code,
      mart_crm_person.zoominfo_company_employee_count,
      mart_crm_person.is_lead_source_trial,
      mart_crm_person.is_inquiry,
      mart_crm_person.is_mql,
      mart_crm_person.sfdc_record_id,
      mart_crm_person.account_demographics_sales_segment,
      mart_crm_person.account_demographics_sales_segment_grouped,
      mart_crm_person.account_demographics_region,
      mart_crm_person.account_demographics_geo,
      mart_crm_person.account_demographics_area,
      mart_crm_person.account_demographics_territory,
      mart_crm_person.employee_bucket,
      mart_crm_person.leandata_matched_account_sales_segment,
      map_alternative_lead_demographics.employee_count_segment_custom,
      map_alternative_lead_demographics.employee_bucket_segment_custom,
      COALESCE(map_alternative_lead_demographics.employee_count_segment_custom, map_alternative_lead_demographics.employee_bucket_segment_custom) AS inferred_employee_segment,
      map_alternative_lead_demographics.geo_custom,
      UPPER(map_alternative_lead_demographics.geo_custom) AS inferred_geo,
      accounts_with_first_order_opps.is_first_order_available,
      order_type_final.person_order_type,
      order_type_final.inquiry_order_type_historical,
      order_type_final.mql_order_type_historical,
      mart_crm_opportunity.order_type AS opp_order_type,
      mart_crm_opportunity.sales_qualified_source_name,
      mart_crm_opportunity.deal_path_name,
      mart_crm_opportunity.sales_type,
      mart_crm_opportunity.dim_crm_opportunity_id,
      mart_crm_opportunity.dim_parent_crm_account_id AS opp_dim_parent_crm_account_id,
      mart_crm_opportunity.sales_accepted_date,
      mart_crm_opportunity.created_date AS opp_created_date,
      mart_crm_opportunity.close_date,
      mart_crm_opportunity.is_won,
      mart_crm_opportunity.is_sao,
      mart_crm_opportunity.new_logo_count,
      mart_crm_opportunity.net_arr,
      mart_crm_opportunity.amount,
      mart_crm_opportunity.record_type_name,
      mart_crm_opportunity.invoice_number,
      mart_crm_opportunity.is_net_arr_closed_deal,
      mart_crm_opportunity.crm_opp_owner_sales_segment_stamped,
      mart_crm_opportunity.crm_opp_owner_region_stamped,
      mart_crm_opportunity.crm_opp_owner_area_stamped,
      mart_crm_opportunity.crm_opp_owner_geo_stamped,
      mart_crm_opportunity.parent_crm_account_demographics_upa_country,
      mart_crm_opportunity.parent_crm_account_demographics_territory,
      mart_crm_opportunity.dim_crm_user_id AS opp_dim_crm_user_id,
      mart_crm_opportunity.duplicate_opportunity_id,
      mart_crm_opportunity.merged_crm_opportunity_id,
      mart_crm_opportunity.record_type_id,
      mart_crm_opportunity.ssp_id,
      mart_crm_opportunity.dim_crm_account_id AS opp_dim_crm_account_id,
      mart_crm_opportunity.crm_account_name,
      mart_crm_opportunity.parent_crm_account_name,
      mart_crm_opportunity.opportunity_name,
      mart_crm_opportunity.stage_name,
      mart_crm_opportunity.reason_for_loss,
      mart_crm_opportunity.reason_for_loss_details,
      mart_crm_opportunity.risk_type,
      mart_crm_opportunity.risk_reasons,
      mart_crm_opportunity.downgrade_reason,
      mart_crm_opportunity.closed_buckets,
      mart_crm_opportunity.opportunity_category,
      mart_crm_opportunity.source_buckets AS opp_source_buckets,
      mart_crm_opportunity.opportunity_sales_development_representative,
      mart_crm_opportunity.opportunity_business_development_representative,
      mart_crm_opportunity.opportunity_development_representative,
      mart_crm_opportunity.sdr_or_bdr,
      mart_crm_opportunity.sdr_pipeline_contribution,
      mart_crm_opportunity.sales_path,
      mart_crm_opportunity.opportunity_deal_size,
      mart_crm_opportunity.primary_campaign_source_id AS opp_primary_campaign_source_id,
      mart_crm_opportunity.net_new_source_categories AS opp_net_new_source_categories,
      mart_crm_opportunity.deal_path_engagement,
      mart_crm_opportunity.forecast_category_name,
      mart_crm_opportunity.opportunity_owner,
      mart_crm_opportunity.churn_contraction_type,
      mart_crm_opportunity.churn_contraction_net_arr_bucket,
      mart_crm_opportunity.owner_id AS opp_owner_id,
      mart_crm_opportunity.order_type_grouped AS opp_order_type_grouped,
      mart_crm_opportunity.sales_qualified_source_grouped,
      mart_crm_opportunity.crm_account_gtm_strategy,
      mart_crm_opportunity.crm_account_focus_account,
      mart_crm_opportunity.crm_account_zi_technologies,
      mart_crm_opportunity.is_jihu_account,
      mart_crm_opportunity.fy22_new_logo_target_list,
      mart_crm_opportunity.is_closed,
      mart_crm_opportunity.is_edu_oss,
      mart_crm_opportunity.is_ps_opp,
      mart_crm_opportunity.is_win_rate_calc,
      mart_crm_opportunity.is_net_arr_pipeline_created,
      mart_crm_opportunity.is_new_logo_first_order,
      mart_crm_opportunity.is_closed_won,
      mart_crm_opportunity.is_web_portal_purchase,
      mart_crm_opportunity.is_lost,
      mart_crm_opportunity.is_open,
      mart_crm_opportunity.is_renewal,
      mart_crm_opportunity.is_duplicate,
      mart_crm_opportunity.is_refund,
      mart_crm_opportunity.is_deleted,
      mart_crm_opportunity.is_excluded_from_pipeline_created,
      mart_crm_opportunity.is_contract_reset,
      mart_crm_opportunity.is_booked_net_arr,
      mart_crm_opportunity.is_downgrade,
      mart_crm_opportunity.critical_deal_flag,
      mart_crm_opportunity.crm_user_sales_segment AS opp_crm_user_sales_segment,
      mart_crm_opportunity.crm_user_sales_segment_grouped AS opp_crm_user_sales_segment_grouped,
      mart_crm_opportunity.crm_user_geo AS opp_crm_user_geo,
      mart_crm_opportunity.crm_user_region AS opp_crm_user_region,
      mart_crm_opportunity.crm_user_area AS opp_crm_user_area,
      mart_crm_opportunity.crm_user_sales_segment_region_grouped AS opp_crm_user_sales_segment_region_grouped,
      mart_crm_opportunity.crm_account_user_sales_segment,
      mart_crm_opportunity.crm_account_user_sales_segment_grouped,
      mart_crm_opportunity.crm_account_user_geo,
      mart_crm_opportunity.crm_account_user_region,
      mart_crm_opportunity.crm_account_user_area,
      mart_crm_opportunity.crm_account_user_sales_segment_region_grouped,
      mart_crm_opportunity.sao_crm_opp_owner_sales_segment_stamped,
      mart_crm_opportunity.sao_crm_opp_owner_sales_segment_stamped_grouped,
      mart_crm_opportunity.sao_crm_opp_owner_geo_stamped,
      mart_crm_opportunity.sao_crm_opp_owner_region_stamped,
      mart_crm_opportunity.sao_crm_opp_owner_area_stamped,
      mart_crm_opportunity.sao_crm_opp_owner_segment_region_stamped_grouped,
      mart_crm_opportunity.crm_opp_owner_sales_segment_stamped_grouped,
      mart_crm_opportunity.crm_opp_owner_sales_segment_region_stamped_grouped,
      mart_crm_opportunity.opportunity_owner_user_segment,
      mart_crm_opportunity.opportunity_owner_user_geo,
      mart_crm_opportunity.opportunity_owner_user_region,
      mart_crm_opportunity.opportunity_owner_user_area,
      mart_crm_opportunity.report_opportunity_user_segment,
      mart_crm_opportunity.report_opportunity_user_geo,
      mart_crm_opportunity.report_opportunity_user_region,
      mart_crm_opportunity.report_opportunity_user_area,
      mart_crm_opportunity.report_user_segment_geo_region_area,
      mart_crm_opportunity.lead_source AS opp_lead_source,
      mart_crm_opportunity.is_public_sector_opp,
      mart_crm_opportunity.is_registration_from_portal,
      mart_crm_opportunity.stage_0_pending_acceptance_date,
      mart_crm_opportunity.stage_1_discovery_date,
      mart_crm_opportunity.stage_2_scoping_date,
      mart_crm_opportunity.stage_3_technical_evaluation_date,
      mart_crm_opportunity.stage_4_proposal_date,
      mart_crm_opportunity.stage_5_negotiating_date,
      mart_crm_opportunity.stage_6_awaiting_signature_date_date,
      mart_crm_opportunity.stage_6_closed_won_date,
      mart_crm_opportunity.stage_6_closed_lost_date,
      mart_crm_opportunity.subscription_start_date,
      mart_crm_opportunity.subscription_end_date,
      mart_crm_opportunity.pipeline_created_date,
      mart_crm_opportunity.churned_contraction_deal_count,
      mart_crm_opportunity.churned_contraction_net_arr,
      mart_crm_opportunity.calculated_deal_count,
      mart_crm_opportunity.days_in_stage,
      opp_user.user_role_name AS opp_user_role_name
    FROM mart_crm_person
    LEFT JOIN upa_base
    ON mart_crm_person.dim_crm_account_id = upa_base.dim_crm_account_id
    LEFT JOIN accounts_with_first_order_opps
      ON upa_base.dim_parent_crm_account_id = accounts_with_first_order_opps.dim_parent_crm_account_id
    FULL JOIN mart_crm_opportunity
      ON upa_base.dim_parent_crm_account_id=mart_crm_opportunity.dim_parent_crm_account_id
    LEFT JOIN order_type_final
      ON mart_crm_person.email_hash = order_type_final.email_hash
    LEFT JOIN map_alternative_lead_demographics
      ON mart_crm_person.dim_crm_person_id = map_alternative_lead_demographics.dim_crm_person_id
    LEFT JOIN dim_crm_user opp_user 
      ON mart_crm_opportunity.dim_crm_user_id = opp_user.dim_crm_user_id

), cohort AS (
  
  SELECT DISTINCT
  
    -- surrogate keys
    cohort_base.dim_crm_person_id,
    cohort_base.dim_crm_opportunity_id,
    rpt_crm_touchpoint_combined.dim_crm_touchpoint_id,
    cohort_base.dim_crm_account_id,
    cohort_base.opp_dim_crm_account_id,
    cohort_base.opp_dim_parent_crm_account_id,
    cohort_base.sfdc_record_id,
  
    --person attributes
    cohort_base.email_hash,
    CASE 
      WHEN cohort_base.person_order_type IS NULL AND cohort_base.opp_order_type IS NULL 
        THEN 'Missing order_type_name'
      WHEN cohort_base.person_order_type IS NULL 
        THEN cohort_base.opp_order_type
      ELSE person_order_type
    END                                                                                   AS person_order_type,
    cohort_base.inquiry_order_type_historical,
    cohort_base.mql_order_type_historical,
    cohort_base.lead_source,    
    cohort_base.status                                                                    AS crm_person_status,
    cohort_base.email_domain_type,
    cohort_base.is_valuable_signup,
    cohort_base.is_mql,
    cohort_base.account_demographics_sales_segment,
    cohort_base.account_demographics_geo,
    cohort_base.account_demographics_region,
    cohort_base.account_demographics_area,
    cohort_base.account_demographics_upa_country,
    cohort_base.account_demographics_territory,
    cohort_base.true_inquiry_date,
    cohort_base.mql_date_lastest_pt,
    cohort_base.dim_crm_user_id,
    cohort_base.mql_date_first_pt,
    cohort_base.created_date_pt,
    cohort_base.created_date,
    cohort_base.mql_date_first,
    cohort_base.lead_created_date,
    cohort_base.lead_created_date_pt,
    cohort_base.contact_created_date,
    cohort_base.contact_created_date_pt,
    cohort_base.inquiry_date,
    cohort_base.inquiry_date_pt,
    cohort_base.inquiry_inferred_date,
    cohort_base.inquiry_inferred_date_pt,
    cohort_base.accepted_date,
    cohort_base.accepted_date_pt,
    cohort_base.qualifying_date,
    cohort_base.qualifying_date_pt,
    cohort_base.converted_date,
    cohort_base.converted_date_pt,
    cohort_base.worked_date,
    cohort_base.worked_date_pt,
    cohort_base.email_domain,
    cohort_base.was_converted_lead,
    cohort_base.source_buckets,
    cohort_base.crm_partner_id,
    cohort_base.partner_prospect_id,
    cohort_base.person_sales_segment_name,
    cohort_base.person_sales_segment_grouped,
    cohort_base.person_score,
    cohort_base.behavior_score,
    cohort_base.marketo_last_interesting_moment,
    cohort_base.marketo_last_interesting_moment_date,
    cohort_base.account_demographics_segment_region_grouped,
    cohort_base.account_demographics_sales_segment_grouped,
    cohort_base.account_demographics_upa_state,
    cohort_base.account_demographics_upa_postal_code,
    cohort_base.zoominfo_company_employee_count,
    cohort_base.is_lead_source_trial,
    cohort_base.is_inquiry,
    cohort_base.employee_count_segment_custom,
    cohort_base.employee_bucket_segment_custom,
    cohort_base.inferred_employee_segment,
    cohort_base.geo_custom,
    cohort_base.inferred_geo,
   
    --opportunity attributes
    cohort_base.opp_created_date,
    cohort_base.sales_accepted_date,
    cohort_base.close_date,
    cohort_base.is_sao,
    cohort_base.is_won,
    cohort_base.new_logo_count,
    cohort_base.net_arr,
    cohort_base.amount,
    cohort_base.invoice_number,
    cohort_base.is_net_arr_closed_deal,
    cohort_base.opp_order_type,
    cohort_base.sales_qualified_source_name,
    cohort_base.deal_path_name,
    cohort_base.sales_type,
    cohort_base.crm_opp_owner_geo_stamped,
    cohort_base.crm_opp_owner_sales_segment_stamped,
    cohort_base.crm_opp_owner_region_stamped,
    cohort_base.crm_opp_owner_area_stamped,
    cohort_base.parent_crm_account_demographics_upa_country,
    cohort_base.parent_crm_account_demographics_territory,
    cohort_base.opp_dim_crm_user_id,
    cohort_base.duplicate_opportunity_id,
    cohort_base.merged_crm_opportunity_id,
    cohort_base.record_type_id,
    cohort_base.ssp_id,
    cohort_base.opportunity_name,
    cohort_base.crm_account_name,
    cohort_base.parent_crm_account_name,
    cohort_base.stage_name,
    cohort_base.reason_for_loss,
    cohort_base.reason_for_loss_details,
    cohort_base.risk_type,
    cohort_base.risk_reasons,
    cohort_base.downgrade_reason,
    cohort_base.closed_buckets,
    cohort_base.opportunity_category,
    cohort_base.opp_source_buckets,
    cohort_base.opportunity_sales_development_representative,
    cohort_base.opportunity_business_development_representative,
    cohort_base.opportunity_development_representative,
    cohort_base.sdr_or_bdr,
    cohort_base.sdr_pipeline_contribution,
    cohort_base.sales_path,
    cohort_base.opportunity_deal_size,
    cohort_base.opp_primary_campaign_source_id,
    cohort_base.opp_net_new_source_categories,
    cohort_base.deal_path_engagement,
    cohort_base.forecast_category_name,
    cohort_base.opportunity_owner,
    cohort_base.churn_contraction_type,
    cohort_base.churn_contraction_net_arr_bucket,
    cohort_base.opp_owner_id,
    cohort_base.opp_order_type_grouped,
    cohort_base.sales_qualified_source_grouped,
    cohort_base.crm_account_gtm_strategy,
    cohort_base.crm_account_focus_account,
    cohort_base.crm_account_zi_technologies,
    cohort_base.is_jihu_account,
    cohort_base.fy22_new_logo_target_list,
    cohort_base.is_closed,
    cohort_base.is_edu_oss,
    cohort_base.is_ps_opp,
    cohort_base.is_win_rate_calc,
    cohort_base.is_net_arr_pipeline_created,
    cohort_base.is_new_logo_first_order,
    cohort_base.is_closed_won,
    cohort_base.is_web_portal_purchase,
    cohort_base.is_lost,
    cohort_base.is_open,
    cohort_base.is_renewal,
    cohort_base.is_duplicate,
    cohort_base.is_refund,
    cohort_base.is_deleted,
    cohort_base.is_excluded_from_pipeline_created,
    cohort_base.is_contract_reset,
    cohort_base.is_booked_net_arr,
    cohort_base.is_downgrade,
    cohort_base.critical_deal_flag,
    cohort_base.opp_crm_user_sales_segment,
    cohort_base.opp_crm_user_sales_segment_grouped,
    cohort_base.opp_crm_user_geo,
    cohort_base.opp_crm_user_region,
    cohort_base.opp_crm_user_area,
    cohort_base.opp_crm_user_sales_segment_region_grouped,
    cohort_base.crm_account_user_sales_segment,
    cohort_base.crm_account_user_sales_segment_grouped,
    cohort_base.crm_account_user_geo,
    cohort_base.crm_account_user_region,
    cohort_base.crm_account_user_area,
    cohort_base.crm_account_user_sales_segment_region_grouped,
    cohort_base.sao_crm_opp_owner_sales_segment_stamped,
    cohort_base.sao_crm_opp_owner_sales_segment_stamped_grouped,
    cohort_base.sao_crm_opp_owner_geo_stamped,
    cohort_base.sao_crm_opp_owner_region_stamped,
    cohort_base.sao_crm_opp_owner_area_stamped,
    cohort_base.sao_crm_opp_owner_segment_region_stamped_grouped,
    cohort_base.crm_opp_owner_sales_segment_stamped_grouped,
    cohort_base.crm_opp_owner_sales_segment_region_stamped_grouped,
    cohort_base.opportunity_owner_user_segment,
    cohort_base.opportunity_owner_user_geo,
    cohort_base.opportunity_owner_user_region,
    cohort_base.opportunity_owner_user_area,
    cohort_base.report_opportunity_user_segment,
    cohort_base.report_opportunity_user_geo,
    cohort_base.report_opportunity_user_region,
    cohort_base.report_opportunity_user_area,
    cohort_base.report_user_segment_geo_region_area,
    cohort_base.opp_lead_source,
    cohort_base.is_public_sector_opp,
    cohort_base.is_registration_from_portal,
    cohort_base.stage_0_pending_acceptance_date,
    cohort_base.stage_1_discovery_date,
    cohort_base.stage_2_scoping_date,
    cohort_base.stage_3_technical_evaluation_date,
    cohort_base.stage_4_proposal_date,
    cohort_base.stage_5_negotiating_date,
    cohort_base.stage_6_awaiting_signature_date_date,
    cohort_base.stage_6_closed_won_date,
    cohort_base.stage_6_closed_lost_date,
    cohort_base.subscription_start_date,
    cohort_base.subscription_end_date,
    cohort_base.pipeline_created_date,
    cohort_base.churned_contraction_deal_count,
    cohort_base.churned_contraction_net_arr,
    cohort_base.calculated_deal_count,
    cohort_base.days_in_stage,
    cohort_base.opp_user_role_name,
    cohort_base.record_type_name,
    CASE
      WHEN rpt_crm_touchpoint_combined.dim_crm_touchpoint_id IS NOT NULL 
        THEN cohort_base.dim_crm_opportunity_id
      ELSE NULL
    END AS influenced_opportunity_id,
  
    --touchpoint attributes
    rpt_crm_touchpoint_combined.bizible_touchpoint_date,
    rpt_crm_touchpoint_combined.gtm_motion,
    rpt_crm_touchpoint_combined.bizible_integrated_campaign_grouping,
    rpt_crm_touchpoint_combined.bizible_marketing_channel_path,
    rpt_crm_touchpoint_combined.bizible_marketing_channel,
    rpt_crm_touchpoint_combined.bizible_ad_campaign_name,
    rpt_crm_touchpoint_combined.bizible_form_url,
    rpt_crm_touchpoint_combined.bizible_landing_page,
    rpt_crm_touchpoint_combined.is_dg_influenced,
    rpt_crm_touchpoint_combined.is_fmm_influenced,
    rpt_crm_touchpoint_combined.mql_sum,
    rpt_crm_touchpoint_combined.inquiry_sum,
    rpt_crm_touchpoint_combined.accepted_sum,
    rpt_crm_touchpoint_combined.linear_opp_created,
    rpt_crm_touchpoint_combined.linear_net_arr,
    rpt_crm_touchpoint_combined.linear_sao,
    rpt_crm_touchpoint_combined.pipeline_linear_net_arr,
    rpt_crm_touchpoint_combined.won_linear,
    rpt_crm_touchpoint_combined.won_linear_net_arr,
    rpt_crm_touchpoint_combined.w_shaped_sao,
    rpt_crm_touchpoint_combined.pipeline_w_net_arr,
    rpt_crm_touchpoint_combined.won_w,
    rpt_crm_touchpoint_combined.won_w_net_arr,
    rpt_crm_touchpoint_combined.u_shaped_sao,
    rpt_crm_touchpoint_combined.pipeline_u_net_arr,
    rpt_crm_touchpoint_combined.won_u,
    rpt_crm_touchpoint_combined.won_u_net_arr,
    rpt_crm_touchpoint_combined.first_sao,
    rpt_crm_touchpoint_combined.pipeline_first_net_arr,
    rpt_crm_touchpoint_combined.won_first,
    rpt_crm_touchpoint_combined.won_first_net_arr,
    rpt_crm_touchpoint_combined.custom_sao,
    rpt_crm_touchpoint_combined.pipeline_custom_net_arr,
    rpt_crm_touchpoint_combined.won_custom,
    rpt_crm_touchpoint_combined.won_custom_net_arr,
    rpt_crm_touchpoint_combined.bizible_touchpoint_position,
    rpt_crm_touchpoint_combined.bizible_touchpoint_source,
    rpt_crm_touchpoint_combined.bizible_touchpoint_type,
    rpt_crm_touchpoint_combined.bizible_ad_content,
    rpt_crm_touchpoint_combined.bizible_ad_group_name,
    rpt_crm_touchpoint_combined.bizible_form_url_raw,
    rpt_crm_touchpoint_combined.bizible_landing_page_raw,
    rpt_crm_touchpoint_combined.bizible_medium,
    rpt_crm_touchpoint_combined.bizible_referrer_page,
    rpt_crm_touchpoint_combined.bizible_referrer_page_raw,
    rpt_crm_touchpoint_combined.bizible_salesforce_campaign,
    rpt_crm_touchpoint_combined.dim_campaign_id,
    rpt_crm_touchpoint_combined.campaign_rep_role_name,
    rpt_crm_touchpoint_combined.touchpoint_segment,
    rpt_crm_touchpoint_combined.pipe_name,

     --inquiry_date fields
    inquiry_date.fiscal_year                     AS inquiry_date_range_year,
    inquiry_date.fiscal_quarter_name_fy          AS inquiry_date_range_quarter,
    DATE_TRUNC(month, inquiry_date.date_actual)  AS inquiry_date_range_month,
    inquiry_date.first_day_of_week               AS inquiry_date_range_week,
    inquiry_date.date_id                         AS inquiry_date_range_id,
  
    --mql_date fields
    mql_date.fiscal_year                     AS mql_date_range_year,
    mql_date.fiscal_quarter_name_fy          AS mql_date_range_quarter,
    DATE_TRUNC(month, mql_date.date_actual)  AS mql_date_range_month,
    mql_date.first_day_of_week               AS mql_date_range_week,
    mql_date.date_id                         AS mql_date_range_id,
  
    --opp_create_date fields
    opp_create_date.fiscal_year                     AS opportunity_created_date_range_year,
    opp_create_date.fiscal_quarter_name_fy          AS opportunity_created_date_range_quarter,
    DATE_TRUNC(month, opp_create_date.date_actual)  AS opportunity_created_date_range_month,
    opp_create_date.first_day_of_week               AS opportunity_created_date_range_week,
    opp_create_date.date_id                         AS opportunity_created_date_range_id,
  
    --sao_date fields
    sao_date.fiscal_year                     AS sao_date_range_year,
    sao_date.fiscal_quarter_name_fy          AS sao_date_range_quarter,
    DATE_TRUNC(month, sao_date.date_actual)  AS sao_date_range_month,
    sao_date.first_day_of_week               AS sao_date_range_week,
    sao_date.date_id                         AS sao_date_range_id,
  
    --closed_date fields
    closed_date.fiscal_year                     AS closed_date_range_year,
    closed_date.fiscal_quarter_name_fy          AS closed_date_range_quarter,
    DATE_TRUNC(month, closed_date.date_actual)  AS closed_date_range_month,
    closed_date.first_day_of_week               AS closed_date_range_week,
    closed_date.date_id                         AS closed_date_range_id
  
  FROM cohort_base
  LEFT JOIN rpt_crm_touchpoint_combined
    ON rpt_crm_touchpoint_combined.email_hash = cohort_base.email_hash
  LEFT JOIN dim_date AS inquiry_date 
    ON cohort_base.true_inquiry_date = inquiry_date.date_day
  LEFT JOIN dim_date AS mql_date
    ON cohort_base.mql_date_lastest_pt = mql_date.date_day
  LEFT JOIN dim_date AS opp_create_date
    ON cohort_base.opp_created_date = opp_create_date.date_day
  LEFT JOIN dim_date AS sao_date
    ON cohort_base.sales_accepted_date = sao_date.date_day
  LEFT JOIN dim_date AS closed_date
    ON cohort_base.close_date=closed_date.date_day

), final AS (

    SELECT DISTINCT *
    FROM cohort

)


{{ dbt_audit(
    cte_ref="final",
    created_by="@michellecooper",
    updated_by="@michellecooper",
    created_date="2022-10-05",
    updated_date="2022-12-27",
  ) }}
