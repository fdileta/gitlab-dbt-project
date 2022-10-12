{{ simple_cte([
    ('mart_crm_opportunity','mart_crm_opportunity'),
    ('mart_crm_person','mart_crm_person'),
    ('mart_crm_touchpoint_combined','mart_crm_touchpoint_combined'),
    ('mart_crm_account','mart_crm_account')
]) }}

, upa_base AS ( 
    SELECT 
      dim_parent_crm_account_id,
      dim_crm_account_id
    FROM mart_crm_account

), accounts_with_first_order_opps AS ( 

    SELECT
      mart_crm_opportunity.dim_parent_crm_account_id,
      mart_crm_opportunity.dim_crm_account_id,
      mart_crm_opportunity.dim_crm_opportunity_id,
      FALSE AS is_first_order_available
    FROM mart_crm_opportunity 
    INNER JOIN mart_crm_account
      ON mart_crm_opportunity.dim_crm_account_id = mart_crm_opportunity.dim_crm_account_id
    WHERE mart_crm_account.is_first_order_available = TRUE

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
         WHEN is_first_order_available = False AND mart_crm_opportunity.order_type = '1. New - First Order' THEN '3. Growth'
         WHEN is_first_order_available = False AND mart_crm_opportunity.order_type != '1. New - First Order' THEN mart_crm_opportunity.order_type
      ELSE '1. New - First Order'
      END AS person_order_type,
      ROW_NUMBER() OVER( PARTITION BY email_hash ORDER BY person_order_type) AS person_order_type_number
    FROM mart_crm_person
    FULL JOIN upa_base
      ON mart_crm_person.dim_crm_account_id=upa_base.dim_crm_account_id
    LEFT JOIN accounts_with_first_order_opps
      ON upa_base.dim_parent_crm_account_id = accounts_with_first_order_opps.dim_parent_crm_account_id
    FULL JOIN mart_crm_opportunity
      ON upa_base.dim_parent_crm_account_id=mart_crm_opportunity.dim_parent_crm_account_id

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
    WHERE person_order_type_number=1

), mql_order_type_base AS (

    SELECT DISTINCT
      mart_crm_person.sfdc_record_id,
      mart_crm_person.email_hash, 
      CASE 
         WHEN mql_date_lastest_pt < mart_crm_opportunity.close_date THEN mart_crm_opportunity.order_type
         WHEN mql_date_lastest_pt > mart_crm_opportunity.close_date THEN '3. Growth'
      ELSE null
      END AS mql_order_type_historical,
      ROW_NUMBER() OVER( PARTITION BY mart_crm_person.email_hash ORDER BY mql_order_type_historical) AS mql_order_type_number
    FROM mart_crm_person
    FULL JOIN upa_base ON 
    mart_crm_person.dim_crm_account_id=upa_base.dim_crm_account_id
    LEFT JOIN accounts_with_first_order_opps ON
    upa_base.dim_parent_crm_account_id = accounts_with_first_order_opps.dim_parent_crm_account_id
    FULL JOIN mart_crm_opportunity ON
    upa_base.dim_parent_crm_account_id=mart_crm_opportunity.dim_parent_crm_account_id
    
), mql_order_type_final AS (
  
  SELECT *
  FROM mql_order_type_base
  WHERE mql_order_type_number=1
    
), inquiry_order_type_base AS (

    SELECT DISTINCT
      mart_crm_person.sfdc_record_id,
      mart_crm_person.email_hash, 
      CASE 
         WHEN true_inquiry_date < mart_crm_opportunity.close_date THEN mart_crm_opportunity.order_type
         WHEN true_inquiry_date > mart_crm_opportunity.close_date THEN '3. Growth'
      ELSE null
      END AS inquiry_order_type_historical,
      ROW_NUMBER() OVER( PARTITION BY mart_crm_person.email_hash ORDER BY inquiry_order_type_historical) AS inquiry_order_type_number -- move this back to dim_crm_person
    FROM mart_crm_person
    FULL JOIN upa_base ON 
    mart_crm_person.dim_crm_account_id=upa_base.dim_crm_account_id
    LEFT JOIN accounts_with_first_order_opps ON
    upa_base.dim_parent_crm_account_id = accounts_with_first_order_opps.dim_parent_crm_account_id
    FULL JOIN mart_crm_opportunity ON
    upa_base.dim_parent_crm_account_id=mart_crm_opportunity.dim_parent_crm_account_id

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
  LEFT JOIN inquiry_order_type_final ON
  person_order_type_final.email_hash=inquiry_order_type_final.email_hash
  LEFT JOIN mql_order_type_final ON
  person_order_type_final.email_hash=mql_order_type_final.email_hash

  ), cohort_base AS (

    SELECT DISTINCT
      mart_crm_person.email_hash,
      mart_crm_person.email_domain_type,
      mart_crm_person.true_inquiry_date,
      mart_crm_person.mql_date_lastest_pt,
      mart_crm_person.status,
      mart_crm_person.lead_source,
      mart_crm_person.dim_crm_person_id,
      mart_crm_person.dim_crm_account_id,
      mart_crm_person.is_mql,
      mart_crm_person.sfdc_record_id,
      mart_crm_person.account_demographics_sales_segment,
      mart_crm_person.account_demographics_region,
      mart_crm_person.account_demographics_geo,
      mart_crm_person.account_demographics_area,
      mart_crm_person.account_demographics_upa_country,
      mart_crm_person.account_demographics_territory,
      is_first_order_available,
      order_type_final.person_order_type,
      order_type_final.inquiry_order_type_historical,
      order_type_final.mql_order_type_historical,
      opp.order_type AS opp_order_type,
      opp.sales_qualified_source_name,
      opp.deal_path_name,
      opp.sales_type,
      opp.dim_crm_opportunity_id,
      opp.sales_accepted_date,
      opp.created_date AS opp_created_date,
      opp.close_date,
      opp.is_won,
      opp.is_sao,
      opp.new_logo_count,
      opp.net_arr,
      opp.is_net_arr_closed_deal,
      opp.crm_opp_owner_sales_segment_stamped,
      opp.crm_opp_owner_region_stamped,
      opp.crm_opp_owner_area_stamped,
      opp.crm_opp_owner_geo_stamped,
      opp.parent_crm_account_demographics_upa_country,
      opp.parent_crm_account_demographics_territory
    FROM mart_crm_person
    LEFT JOIN upa_base
    ON mart_crm_person.dim_crm_account_id=upa_base.dim_crm_account_id
    LEFT JOIN accounts_with_first_order_opps
      ON upa_base.dim_parent_crm_account_id = accounts_with_first_order_opps.dim_parent_crm_account_id
    FULL JOIN mart_crm_opportunity opp
      ON upa_base.dim_parent_crm_account_id=opp.dim_parent_crm_account_id
    LEFT JOIN order_type_final
      ON mart_crm_person.email_hash=order_type_final.email_hash

), fo_inquiry_with_tp AS (
  
  SELECT DISTINCT
  
    --Key IDs
    cohort_base.email_hash,
    cohort_base.dim_crm_person_id,
    cohort_base.dim_crm_opportunity_id,
    mart_crm_touchpoint_combined.dim_crm_touchpoint_id,
    cohort_base.sfdc_record_id,
  
    --person data
    CASE 
      WHEN cohort_base.person_order_type IS null AND cohort_base.opp_order_type IS null THEN 'Missing order_type_name'
      WHEN cohort_base.person_order_type IS null THEN cohort_base.opp_order_type
      ELSE person_order_type
    END AS person_order_type,
    cohort_base.inquiry_order_type_historical,
    cohort_base.mql_order_type_historical,
    cohort_base.lead_source,    
    cohort_base.email_domain_type,
    cohort_base.is_mql,
    cohort_base.account_demographics_sales_segment,
    cohort_base.account_demographics_geo,
    cohort_base.account_demographics_region,
    cohort_base.account_demographics_area,
    cohort_base.account_demographics_upa_country,
    cohort_base.account_demographics_territory,
    cohort_base.true_inquiry_date,
    cohort_base.mql_date_lastest_pt,
  
    --opportunity data
    cohort_base.opp_created_date,
    cohort_base.sales_accepted_date,
    cohort_base.close_date,
    cohort_base.is_sao,
    cohort_base.is_won,
    cohort_base.new_logo_count,
    cohort_base.net_arr,
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
    CASE
      WHEN mart_crm_touchpoint_combined.dim_crm_touchpoint_id IS NOT null 
          THEN cohort_base.dim_crm_opportunity_id
      ELSE null
    END AS influenced_opportunity_id,
  
    --touchpoint data
    mart_crm_touchpoint_combined.bizible_touchpoint_date,
    mart_crm_touchpoint_combined.gtm_motion,
    mart_crm_touchpoint_combined.bizible_integrated_campaign_grouping,
    mart_crm_touchpoint_combined.bizible_marketing_channel_path,
    mart_crm_touchpoint_combined.bizible_marketing_channel,
    mart_crm_touchpoint_combined.bizible_ad_campaign_name,
    mart_crm_touchpoint_combined.bizible_form_url,
    mart_crm_touchpoint_combined.bizible_landing_page,
    mart_crm_touchpoint_combined.is_dg_influenced,
    mart_crm_touchpoint_combined.is_fmm_influenced,
    mart_crm_touchpoint_combined.mql_sum,
    mart_crm_touchpoint_combined.inquiry_sum,
    mart_crm_touchpoint_combined.accepted_sum,
    mart_crm_touchpoint_combined.linear_opp_created,
    mart_crm_touchpoint_combined.linear_net_arr,
    mart_crm_touchpoint_combined.linear_sao,
    mart_crm_touchpoint_combined.pipeline_linear_net_arr,
    mart_crm_touchpoint_combined.won_linear,
    mart_crm_touchpoint_combined.won_linear_net_arr,
    mart_crm_touchpoint_combined.w_shaped_sao,
    mart_crm_touchpoint_combined.pipeline_w_net_arr,
    mart_crm_touchpoint_combined.won_w,
    mart_crm_touchpoint_combined.won_w_net_arr,
    mart_crm_touchpoint_combined.u_shaped_sao,
    mart_crm_touchpoint_combined.pipeline_u_net_arr,
    mart_crm_touchpoint_combined.won_u,
    mart_crm_touchpoint_combined.won_u_net_arr,
    mart_crm_touchpoint_combined.first_sao,
    mart_crm_touchpoint_combined.pipeline_first_net_arr,
    mart_crm_touchpoint_combined.won_first,
    mart_crm_touchpoint_combined.won_first_net_arr,
    mart_crm_touchpoint_combined.custom_sao,
    mart_crm_touchpoint_combined.pipeline_custom_net_arr,
    mart_crm_touchpoint_combined.won_custom,
    mart_crm_touchpoint_combined.won_custom_net_arr
  FROM cohort_base
  LEFT JOIN mart_crm_touchpoint_combined
    ON mart_crm_touchpoint_combined.email_hash=cohort_base.email_hash

), final AS (

    SELECT DISTINCT *
    FROM fo_inquiry_with_tp

)


{{ dbt_audit(
    cte_ref="final",
    created_by="@michellecooper",
    updated_by="@michellecooper",
    created_date="2022-10-05",
    updated_date="2022-10-05",
  ) }}