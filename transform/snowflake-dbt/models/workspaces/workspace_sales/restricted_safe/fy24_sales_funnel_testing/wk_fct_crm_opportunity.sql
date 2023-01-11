
{{ simple_cte([
    ('crm_account_dimensions', 'map_crm_account'),
    ('prep_crm_account', 'prep_crm_account'),
    ('order_type', 'prep_order_type'),
    ('sales_qualified_source', 'prep_sales_qualified_source'),
    ('deal_path', 'prep_deal_path'),
    ('sales_rep', 'prep_crm_user'),
    ('sales_segment', 'prep_sales_segment'),
    ('sfdc_campaigns', 'prep_campaign'),
    ('dr_partner_engagement', 'prep_dr_partner_engagement'),
    ('alliance_type', 'prep_alliance_type_scd'),
    ('channel_type', 'prep_channel_type'),
    ('prep_crm_opportunity', 'prep_crm_opportunity')

]) }}

), sfdc_opportunity AS (

    SELECT 
      *,
      'ENTG' AS crm_opp_owner_business_unit_stamped,
      CASE
        WHEN close_date BETWEEN '2021-02-01' AND '2022-01-31'
          THEN CONCAT(crm_opp_owner_sales_segment_stamped, crm_opp_owner_geo_stamped, crm_opp_owner_region_stamped, crm_opp_owner_area_stamped, fiscal_year)
        WHEN close_date BETWEEN '2022-02-01' AND '2023-01-31'
          THEN CONCAT(crm_opp_owner_sales_segment_stamped, crm_opp_owner_geo_stamped, crm_opp_owner_region_stamped, crm_opp_owner_area_stamped, fiscal_year)
        WHEN close_date >= '2023-02-01' AND LOWER(crm_opp_owner_business_unit_stamped) = 'comm'
          THEN CONCAT(crm_opp_owner_business_unit_stamped, crm_opp_owner_geo_stamped, crm_opp_owner_region_stamped, crm_opp_owner_sales_segment_stamped, crm_opp_owner_area_stamped, fiscal_year)
        WHEN close_date >= '2023-02-01' AND LOWER(crm_opp_owner_business_unit_stamped) = 'entg'
          THEN CONCAT(crm_opp_owner_business_unit_stamped, crm_opp_owner_geo_stamped, crm_opp_owner_region_stamped, crm_opp_owner_area_stamped, crm_opp_owner_sales_segment_stamped, fiscal_year)
      END AS crm_opp_owner_hierarchy_key
    FROM prep_crm_opportunity
    LEFT JOIN prod.common.dim_date
      ON prep_crm_opportunity.close_date = dim_date.date_actual
    
), prep_crm_user_hierarchy_stamped AS (
    
    SELECT 
      *,
      'ENTG' AS crm_opp_owner_business_unit_stamped,
      CASE 
        WHEN fiscal_year = 2022
          THEN CONCAT(crm_opp_owner_sales_segment_stamped, crm_opp_owner_geo_stamped, crm_opp_owner_region_stamped, crm_opp_owner_area_stamped, fiscal_year)
        WHEN fiscal_year = 2023
          THEN CONCAT(crm_opp_owner_sales_segment_stamped, crm_opp_owner_geo_stamped, crm_opp_owner_region_stamped, crm_opp_owner_area_stamped, fiscal_year)
        WHEN fiscal_year > 2023 AND LOWER(crm_opp_owner_business_unit_stamped) = 'comm'
          THEN CONCAT(crm_opp_owner_business_unit_stamped, crm_opp_owner_geo_stamped, crm_opp_owner_region_stamped, crm_opp_owner_sales_segment_stamped, crm_opp_owner_area_stamped, fiscal_year)
        WHEN fiscal_year > 2023 AND LOWER(crm_opp_owner_business_unit_stamped) = 'entg'
          THEN CONCAT(crm_opp_owner_business_unit_stamped, crm_opp_owner_geo_stamped, crm_opp_owner_region_stamped, crm_opp_owner_area_stamped, crm_opp_owner_sales_segment_stamped, fiscal_year)
      END AS crm_opp_owner_hierarchy_key
    FROM {{ ref('prep_crm_user_hierarchy_stamped') }}
    
), prep_crm_user_hierarchy_live AS (
    
    SELECT 
      *,
      'ENTG' AS crm_user_business_unit,
      CASE 
        WHEN LOWER(crm_user_business_unit) = 'comm'
          THEN CONCAT(crm_user_business_unit, crm_user_geo, crm_user_region, crm_user_sales_segment, crm_user_area)
        WHEN LOWER(crm_user_business_unit) = 'entg'
          THEN CONCAT(crm_user_business_unit, crm_user_geo, crm_user_region, crm_user_area, crm_user_sales_segment)
      END AS crm_opp_owner_hierarchy_key
    FROM {{ ref('prep_crm_user_hierarchy_live') }}

), final_opportunities AS (

    SELECT

      -- opportunity and person ids
      sfdc_opportunity.dim_crm_opportunity_id,
      sfdc_opportunity.merged_opportunity_id                                                                              AS merged_crm_opportunity_id,
      sfdc_opportunity.dim_crm_account_id,
      crm_account_dimensions.dim_parent_crm_account_id,
      sfdc_opportunity.dim_crm_person_id,
      sfdc_opportunity.sfdc_contact_id,
      sfdc_opportunity.record_type_id,

      -- dates
      sfdc_opportunity.created_date,
      sfdc_opportunity.created_date_id,
      sfdc_opportunity.sales_accepted_date,
      sfdc_opportunity.sales_accepted_date_id,
      sfdc_opportunity.close_date,
      sfdc_opportunity.close_date_id,
      sfdc_opportunity.arr_created_date_id,
      sfdc_opportunity.arr_created_date,
      sfdc_opportunity.stage_0_pending_acceptance_date,
      sfdc_opportunity.stage_0_pending_acceptance_date_id,
      sfdc_opportunity.stage_1_discovery_date,
      sfdc_opportunity.stage_1_discovery_date_id,
      sfdc_opportunity.stage_2_scoping_date,
      sfdc_opportunity.stage_2_scoping_date_id,
      sfdc_opportunity.stage_3_technical_evaluation_date,
      sfdc_opportunity.stage_3_technical_evaluation_date_id,
      sfdc_opportunity.stage_4_proposal_date,
      sfdc_opportunity.stage_4_proposal_date_id,
      sfdc_opportunity.stage_5_negotiating_date,
      sfdc_opportunity.stage_5_negotiating_date_id,
      sfdc_opportunity.stage_6_awaiting_signature_date,
      sfdc_opportunity.stage_6_awaiting_signature_date_id,
      sfdc_opportunity.stage_6_closed_won_date,
      sfdc_opportunity.stage_6_closed_won_date_id,
      sfdc_opportunity.stage_6_closed_lost_date,
      sfdc_opportunity.stage_6_closed_lost_date_id,
      sfdc_opportunity.days_in_0_pending_acceptance,
      sfdc_opportunity.days_in_1_discovery,
      sfdc_opportunity.days_in_2_scoping,
      sfdc_opportunity.days_in_3_technical_evaluation,
      sfdc_opportunity.days_in_4_proposal,
      sfdc_opportunity.days_in_5_negotiating,
      sfdc_opportunity.subscription_start_date_id,
      sfdc_opportunity.subscription_end_date_id,
      sfdc_opportunity.sales_qualified_date_id,
      sfdc_opportunity.last_activity_date,
      sfdc_opportunity.last_activity_date_id,
      sfdc_opportunity.technical_evaluation_date,
      sfdc_opportunity.technical_evaluation_date_id,

      -- common dimension keys
      COALESCE(sfdc_opportunity.dim_crm_user_id, MD5(-1))                                                                   AS dim_crm_user_id,
      COALESCE(prep_crm_account.dim_crm_user_id, MD5(-1))                                                                   AS dim_crm_account_user_id,
      COALESCE(order_type.dim_order_type_id, MD5(-1))                                                                       AS dim_order_type_id,
      COALESCE(order_type_live.dim_order_type_id, MD5(-1))                                                                  AS dim_order_type_live_id,
      COALESCE(dr_partner_engagement.dim_dr_partner_engagement_id, MD5(-1))                                                 AS dim_dr_partner_engagement_id,
      COALESCE(alliance_type.dim_alliance_type_id, MD5(-1))                                                                 AS dim_alliance_type_id,
      COALESCE(alliance_type_current.dim_alliance_type_id, MD5(-1))                                                         AS dim_alliance_type_current_id,
      COALESCE(channel_type.dim_channel_type_id, MD5(-1))                                                                   AS dim_channel_type_id,
      COALESCE(sales_qualified_source.dim_sales_qualified_source_id, MD5(-1))                                               AS dim_sales_qualified_source_id,
      COALESCE(deal_path.dim_deal_path_id, MD5(-1))                                                                         AS dim_deal_path_id,
      COALESCE(crm_account_dimensions.dim_parent_sales_segment_id,sales_segment.dim_sales_segment_id, MD5(-1))              AS dim_parent_sales_segment_id,
      crm_account_dimensions.dim_parent_sales_territory_id,
      crm_account_dimensions.dim_parent_industry_id,
      crm_account_dimensions.dim_parent_location_country_id,
      crm_account_dimensions.dim_parent_location_region_id,
      COALESCE(crm_account_dimensions.dim_account_sales_segment_id,sales_segment.dim_sales_segment_id, MD5(-1))             AS dim_account_sales_segment_id,
      crm_account_dimensions.dim_account_sales_territory_id,
      crm_account_dimensions.dim_account_industry_id,
      crm_account_dimensions.dim_account_location_country_id,
      crm_account_dimensions.dim_account_location_region_id,
      COALESCE(prep_crm_user_hierarchy_stamped.dim_crm_user_hierarchy_stamped_id, MD5(-1))                                  AS dim_crm_opp_owner_user_hierarchy_id,
      prep_crm_user_hierarchy_stamped.crm_opp_owner_sales_segment_stamped,
      sfdc_opportunity.crm_opp_owner_sales_segment_stamped                                                                  AS opp_crm_opp_owner_sales_segment_stamped,
      prep_crm_user_hierarchy_stamped.crm_opp_owner_geo_stamped,
      sfdc_opportunity.crm_opp_owner_geo_stamped                                                                            AS opp_crm_opp_owner_geo_stamped,
      prep_crm_user_hierarchy_stamped.crm_opp_owner_region_stamped,
      sfdc_opportunity.crm_opp_owner_region_stamped                                                                         AS opp_crm_opp_owner_region_stamped,
      prep_crm_user_hierarchy_stamped.crm_opp_owner_area_stamped,
      sfdc_opportunity.crm_opp_owner_area_stamped                                                                           AS opp_crm_opp_owner_area_stamped,
      COALESCE(prep_crm_user_hierarchy_stamped.dim_crm_opp_owner_sales_segment_stamped_id, MD5(-1))                         AS dim_crm_opp_owner_sales_segment_stamped_id,
      COALESCE(prep_crm_user_hierarchy_stamped.dim_crm_opp_owner_geo_stamped_id, MD5(-1))                                   AS dim_crm_opp_owner_geo_stamped_id,
      COALESCE(prep_crm_user_hierarchy_stamped.dim_crm_opp_owner_region_stamped_id, MD5(-1))                                AS dim_crm_opp_owner_region_stamped_id,
      COALESCE(prep_crm_user_hierarchy_stamped.dim_crm_opp_owner_area_stamped_id, MD5(-1))                                  AS dim_crm_opp_owner_area_stamped_id,
      COALESCE(sales_rep.dim_crm_user_sales_segment_id, MD5(-1))                                                            AS dim_crm_user_sales_segment_id,
      COALESCE(sales_rep.dim_crm_user_geo_id, MD5(-1))                                                                      AS dim_crm_user_geo_id,
      COALESCE(sales_rep.dim_crm_user_region_id, MD5(-1))                                                                   AS dim_crm_user_region_id,
      COALESCE(sales_rep.dim_crm_user_area_id, MD5(-1))                                                                     AS dim_crm_user_area_id,
      COALESCE(sales_rep_account.dim_crm_user_sales_segment_id, MD5(-1))                                                    AS dim_crm_account_user_sales_segment_id,
      COALESCE(sales_rep_account.dim_crm_user_geo_id, MD5(-1))                                                              AS dim_crm_account_user_geo_id,
      COALESCE(sales_rep_account.dim_crm_user_region_id, MD5(-1))                                                           AS dim_crm_account_user_region_id,
      COALESCE(sales_rep_account.dim_crm_user_area_id, MD5(-1))                                                             AS dim_crm_account_user_area_id,
      sfdc_opportunity.ssp_id,
      sfdc_opportunity.ga_client_id,

      -- flags
      sfdc_opportunity.is_closed,
      sfdc_opportunity.is_won,
      sfdc_opportunity.is_refund,
      sfdc_opportunity.is_downgrade,
      sfdc_opportunity.is_swing_deal,
      sfdc_opportunity.is_edu_oss,
      sfdc_opportunity.is_web_portal_purchase,
      sfdc_opportunity.fpa_master_bookings_flag,
      sfdc_opportunity.is_sao,
      sfdc_opportunity.is_sdr_sao,
      sfdc_opportunity.is_net_arr_closed_deal,
      sfdc_opportunity.is_new_logo_first_order,
      sfdc_opportunity.is_net_arr_pipeline_created,
      sfdc_opportunity.is_win_rate_calc,
      sfdc_opportunity.is_closed_won,
      sfdc_opportunity.is_stage_1_plus,
      sfdc_opportunity.is_stage_3_plus,
      sfdc_opportunity.is_stage_4_plus,
      sfdc_opportunity.is_lost,
      sfdc_opportunity.is_open,
      sfdc_opportunity.is_active,
      sfdc_opportunity.is_credit,
      sfdc_opportunity.is_renewal,
      sfdc_opportunity.is_deleted,
      sfdc_opportunity.is_excluded_from_pipeline_created,
      sfdc_opportunity.is_duplicate,
      sfdc_opportunity.is_contract_reset,
      sfdc_opportunity.is_comp_new_logo_override,
      sfdc_opportunity.is_eligible_open_pipeline,
      sfdc_opportunity.is_eligible_asp_analysis,
      sfdc_opportunity.is_eligible_age_analysis,
      sfdc_opportunity.is_eligible_churn_contraction,
      sfdc_opportunity.is_booked_net_arr,

      sfdc_opportunity.primary_solution_architect,
      sfdc_opportunity.product_details,
      sfdc_opportunity.product_category,
      sfdc_opportunity.products_purchased,
      sfdc_opportunity.growth_type,
      sfdc_opportunity.opportunity_deal_size,
      sfdc_opportunity.closed_buckets,

      -- channel fields
      sfdc_opportunity.lead_source,
      sfdc_opportunity.dr_partner_deal_type,
      sfdc_opportunity.dr_partner_engagement,
      sfdc_opportunity.partner_account,
      sfdc_opportunity.dr_status,
      sfdc_opportunity.dr_deal_id,
      sfdc_opportunity.dr_primary_registration,
      sfdc_opportunity.distributor,
      sfdc_opportunity.influence_partner,
      sfdc_opportunity.fulfillment_partner,
      sfdc_opportunity.platform_partner,
      sfdc_opportunity.partner_track,
      sfdc_opportunity.resale_partner_track,
      sfdc_opportunity.is_public_sector_opp,
      sfdc_opportunity.is_registration_from_portal,
      sfdc_opportunity.calculated_discount,
      sfdc_opportunity.partner_discount,
      sfdc_opportunity.partner_discount_calc,
      sfdc_opportunity.comp_channel_neutral,

      -- additive fields
      sfdc_opportunity.incremental_acv                                                                                      AS iacv,
      sfdc_opportunity.net_incremental_acv                                                                                  AS net_iacv,
      sfdc_opportunity.segment_order_type_iacv_to_net_arr_ratio,
      sfdc_opportunity.calculated_from_ratio_net_arr,
      sfdc_opportunity.net_arr,
      sfdc_opportunity.created_and_won_same_quarter_net_arr,
      sfdc_opportunity.new_logo_count,
      sfdc_opportunity.amount,
      sfdc_opportunity.recurring_amount,
      sfdc_opportunity.true_up_amount,
      sfdc_opportunity.proserv_amount,
      sfdc_opportunity.other_non_recurring_amount,
      sfdc_opportunity.arr_basis,
      sfdc_opportunity.arr,
      sfdc_opportunity.count_crm_attribution_touchpoints,
      sfdc_opportunity.weighted_linear_iacv,
      sfdc_opportunity.count_campaigns,
      sfdc_opportunity.probability,
      sfdc_opportunity.days_in_sao,
      sfdc_opportunity.open_1plus_deal_count,
      sfdc_opportunity.open_3plus_deal_count,
      sfdc_opportunity.open_4plus_deal_count,
      sfdc_opportunity.booked_deal_count,
      sfdc_opportunity.churned_contraction_deal_count,
      sfdc_opportunity.open_1plus_net_arr,
      sfdc_opportunity.open_3plus_net_arr,
      sfdc_opportunity.open_4plus_net_arr,
      sfdc_opportunity.booked_net_arr,
      sfdc_opportunity.churned_contraction_net_arr,
      sfdc_opportunity.calculated_deal_count,
      sfdc_opportunity.booked_churned_contraction_deal_count,
      sfdc_opportunity.booked_churned_contraction_net_arr,
      sfdc_opportunity.renewal_amount,
      sfdc_opportunity.total_contract_value,
      sfdc_opportunity.days_in_stage,
      sfdc_opportunity.calculated_age_in_days,
      sfdc_opportunity.days_since_last_activity,
      prep_crm_user_hierarchy_stamped.crm_opp_owner_hierarchy_key

    FROM sfdc_opportunity
    LEFT JOIN crm_account_dimensions
      ON sfdc_opportunity.dim_crm_account_id = crm_account_dimensions.dim_crm_account_id
    LEFT JOIN prep_crm_account
      ON sfdc_opportunity.dim_crm_account_id = prep_crm_account.dim_crm_account_id
    LEFT JOIN sales_qualified_source
      ON sfdc_opportunity.sales_qualified_source = sales_qualified_source.sales_qualified_source_name
    LEFT JOIN order_type
      ON sfdc_opportunity.order_type = order_type.order_type_name
    LEFT JOIN order_type AS order_type_live
      ON sfdc_opportunity.order_type_live = order_type_live.order_type_name
    LEFT JOIN deal_path
      ON sfdc_opportunity.deal_path = deal_path.deal_path_name
    LEFT JOIN sales_segment
      ON sfdc_opportunity.sales_segment = sales_segment.sales_segment_name
    LEFT JOIN prep_crm_user_hierarchy_stamped
      ON sfdc_opportunity.crm_opp_owner_hierarchy_key = prep_crm_user_hierarchy_stamped.crm_opp_owner_hierarchy_key
    LEFT JOIN prep_crm_user_hierarchy_live
      ON sfdc_opportunity.crm_opp_owner_hierarchy_key = prep_crm_user_hierarchy_live.crm_opp_owner_hierarchy_key
    LEFT JOIN dr_partner_engagement
      ON sfdc_opportunity.dr_partner_engagement = dr_partner_engagement.dr_partner_engagement_name
    LEFT JOIN alliance_type
      ON sfdc_opportunity.alliance_type = alliance_type.alliance_type_name
    LEFT JOIN alliance_type AS alliance_type_current
      ON sfdc_opportunity.alliance_type_current = alliance_type_current.alliance_type_name
    LEFT JOIN channel_type
      ON sfdc_opportunity.channel_type = channel_type.channel_type_name
    LEFT JOIN sales_rep
      ON sfdc_opportunity.dim_crm_user_id = sales_rep.dim_crm_user_id
    LEFT JOIN sales_rep AS sales_rep_account
      ON prep_crm_account.dim_crm_user_id = sales_rep_account.dim_crm_user_id

)

{{ dbt_audit(
    cte_ref="final_opportunities",
    created_by="@michellecooper",
    updated_by="@michellecooper",
    created_date="2023-01-12",
    updated_date="2023-01-12"
) }}