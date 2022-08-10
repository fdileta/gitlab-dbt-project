{{ simple_cte([
    ('dim_crm_account','dim_crm_account'),
    ('dim_crm_opportunity','dim_crm_opportunity'),
    ('dim_sales_qualified_source','dim_sales_qualified_source'),
    ('dim_order_type','dim_order_type'),
    ('dim_deal_path','dim_deal_path'),
    ('fct_crm_opportunity','fct_crm_opportunity'),
    ('dim_dr_partner_engagement', 'dim_dr_partner_engagement'),
    ('dim_alliance_type', 'dim_alliance_type_scd'),
    ('dim_channel_type', 'dim_channel_type'),
    ('dim_date', 'dim_date')
]) }}

, current_fiscal_year AS (
  
    SELECT fiscal_year AS current_fiscal_year
    FROM dim_date
    WHERE CURRENT_DATE = date_actual
  
), dim_date_extended AS (

    SELECT
      dim_date.*,
      current_fiscal_year.current_fiscal_year
    FROM dim_date
    LEFT JOIN current_fiscal_year
  
), dim_crm_user_hierarchy_live_sales_segment AS (

    SELECT DISTINCT
      dim_crm_user_sales_segment_id,
      crm_user_sales_segment,
      crm_user_sales_segment_grouped
    FROM {{ ref('dim_crm_user_hierarchy_live') }}

), dim_crm_user_hierarchy_live_geo AS (

    SELECT DISTINCT
      dim_crm_user_geo_id,
      crm_user_geo
    FROM {{ ref('dim_crm_user_hierarchy_live') }}

), dim_crm_user_hierarchy_live_region AS (

    SELECT DISTINCT
      dim_crm_user_region_id,
      crm_user_region
    FROM {{ ref('dim_crm_user_hierarchy_live') }}

), dim_crm_user_hierarchy_live_area AS (

    SELECT DISTINCT
      dim_crm_user_area_id,
      crm_user_area
    FROM {{ ref('dim_crm_user_hierarchy_live') }}

), dim_crm_user_hierarchy_stamped_sales_segment AS (

    SELECT DISTINCT
      dim_crm_opp_owner_sales_segment_stamped_id,
      crm_opp_owner_sales_segment_stamped,
      crm_opp_owner_sales_segment_stamped_grouped
    FROM {{ ref('dim_crm_user_hierarchy_stamped') }}

), dim_crm_user_hierarchy_stamped_geo AS (

    SELECT DISTINCT
      dim_crm_opp_owner_geo_stamped_id,
      crm_opp_owner_geo_stamped
    FROM {{ ref('dim_crm_user_hierarchy_stamped') }}

), dim_crm_user_hierarchy_stamped_region AS (

    SELECT DISTINCT
      dim_crm_opp_owner_region_stamped_id,
      crm_opp_owner_region_stamped
    FROM {{ ref('dim_crm_user_hierarchy_stamped') }}

), dim_crm_user_hierarchy_stamped_area AS (

    SELECT DISTINCT
      dim_crm_opp_owner_area_stamped_id,
      crm_opp_owner_area_stamped
    FROM {{ ref('dim_crm_user_hierarchy_stamped') }}

), final AS (

    SELECT
      fct_crm_opportunity.sales_accepted_date,
      DATE_TRUNC(month, fct_crm_opportunity.sales_accepted_date)           AS sales_accepted_month,
      fct_crm_opportunity.close_date,
      DATE_TRUNC(month, fct_crm_opportunity.close_date)                    AS close_month,
      fct_crm_opportunity.created_date,
      DATE_TRUNC(month, fct_crm_opportunity.created_date)                  AS created_month,
      arr_created_date.date_actual                                         AS arr_created_date,
      arr_created_date.date_actual                                         AS pipeline_created_date,
      fct_crm_opportunity.dim_crm_opportunity_id,
      dim_crm_opportunity.opportunity_name,
      dim_crm_account.parent_crm_account_name,
      dim_crm_account.dim_parent_crm_account_id,
      dim_crm_account.crm_account_name,
      dim_crm_account.dim_crm_account_id,
      dim_crm_opportunity.dim_crm_user_id,
      fct_crm_opportunity.ssp_id,

      -- opportunity attributes & additive fields
      fct_crm_opportunity.is_won,
      fct_crm_opportunity.is_closed,
      fct_crm_opportunity.days_in_sao,
      fct_crm_opportunity.arr_basis,
      fct_crm_opportunity.iacv,
      fct_crm_opportunity.net_iacv,
      fct_crm_opportunity.net_arr,
      fct_crm_opportunity.new_logo_count,
      fct_crm_opportunity.amount,
      dim_crm_opportunity.is_edu_oss,
      dim_crm_opportunity.is_ps_opp,
      dim_crm_opportunity.stage_name,
      dim_crm_opportunity.reason_for_loss,
      dim_crm_opportunity.sales_type,
      fct_crm_opportunity.is_sao,
      fct_crm_opportunity.is_net_arr_closed_deal,
      fct_crm_opportunity.is_new_logo_first_order,
      fct_crm_opportunity.is_net_arr_pipeline_created,
      fct_crm_opportunity.is_win_rate_calc,
      fct_crm_opportunity.is_closed_won,
      dim_deal_path.deal_path_name,
      dim_order_type.order_type_name                                       AS order_type,
      dim_order_type.order_type_grouped,
      dim_dr_partner_engagement.dr_partner_engagement_name,
      dim_alliance_type_current.alliance_type_name,
      dim_alliance_type_current.alliance_type_short_name,
      dim_channel_type.channel_type_name,
      dim_sales_qualified_source.sales_qualified_source_name,
      dim_sales_qualified_source.sales_qualified_source_grouped,
      dim_sales_qualified_source.sqs_bucket_engagement,
      dim_crm_account.is_jihu_account,
      dim_crm_account.fy22_new_logo_target_list,
      dim_crm_account.crm_account_gtm_strategy,
      dim_crm_account.crm_account_focus_account,
      dim_crm_account.crm_account_zi_technologies,
      dim_crm_account.parent_crm_account_gtm_strategy,
      dim_crm_account.parent_crm_account_focus_account,
      dim_crm_account.parent_crm_account_sales_segment,
      dim_crm_account.parent_crm_account_zi_technologies,
      dim_crm_account.parent_crm_account_demographics_sales_segment,
      dim_crm_account.parent_crm_account_demographics_geo,
      dim_crm_account.parent_crm_account_demographics_region,
      dim_crm_account.parent_crm_account_demographics_area,
      dim_crm_account.parent_crm_account_demographics_territory,
      dim_crm_account.crm_account_demographics_employee_count,
      dim_crm_account.parent_crm_account_demographics_max_family_employee,
      dim_crm_account.parent_crm_account_demographics_upa_country,
      dim_crm_account.parent_crm_account_demographics_upa_state,
      dim_crm_account.parent_crm_account_demographics_upa_city,
      dim_crm_account.parent_crm_account_demographics_upa_street,
      dim_crm_account.parent_crm_account_demographics_upa_postal_code,
      fct_crm_opportunity.closed_buckets,
      dim_crm_opportunity.duplicate_opportunity_id,
      dim_crm_opportunity.opportunity_category,
      dim_crm_opportunity.source_buckets,
      dim_crm_opportunity.opportunity_sales_development_representative,
      dim_crm_opportunity.opportunity_business_development_representative,
      dim_crm_opportunity.opportunity_development_representative,
      dim_crm_opportunity.sdr_or_bdr,
      dim_crm_opportunity.iqm_submitted_by_role,
      dim_crm_opportunity.sdr_pipeline_contribution,
      dim_crm_opportunity.is_web_portal_purchase,
      fct_crm_opportunity.fpa_master_bookings_flag,
      dim_crm_opportunity.sales_path,
      dim_crm_opportunity.professional_services_value,
      fct_crm_opportunity.primary_solution_architect,
      fct_crm_opportunity.product_details,
      fct_crm_opportunity.product_category,
      fct_crm_opportunity.products_purchased,
      fct_crm_opportunity.growth_type,
      fct_crm_opportunity.opportunity_deal_size,
      fct_crm_opportunity.ga_client_id,

      -- crm owner/sales rep live fields
      dim_crm_user_hierarchy_live_sales_segment.crm_user_sales_segment,
      dim_crm_user_hierarchy_live_sales_segment.crm_user_sales_segment_grouped,
      dim_crm_user_hierarchy_live_geo.crm_user_geo,
      dim_crm_user_hierarchy_live_region.crm_user_region,
      dim_crm_user_hierarchy_live_area.crm_user_area,
      {{ sales_segment_region_grouped('dim_crm_user_hierarchy_live_sales_segment.crm_user_sales_segment',
        'dim_crm_user_hierarchy_live_geo.crm_user_geo', 'dim_crm_user_hierarchy_live_region.crm_user_region') }}
                                                                                         AS crm_user_sales_segment_region_grouped,

       -- crm account owner/sales rep live fields
      dim_crm_account_user_hierarchy_live_sales_segment.crm_user_sales_segment           AS crm_account_user_sales_segment,
      dim_crm_account_user_hierarchy_live_sales_segment.crm_user_sales_segment_grouped   AS crm_account_user_sales_segment_grouped,
      dim_crm_account_user_hierarchy_live_geo.crm_user_geo                               AS crm_account_user_geo,
      dim_crm_account_user_hierarchy_live_region.crm_user_region                         AS crm_account_user_region,
      dim_crm_account_user_hierarchy_live_area.crm_user_area                             AS crm_account_user_area,
      {{ sales_segment_region_grouped('dim_crm_account_user_hierarchy_live_sales_segment.crm_user_sales_segment',
        'dim_crm_account_user_hierarchy_live_geo.crm_user_geo', 'dim_crm_account_user_hierarchy_live_region.crm_user_region') }}
                                                                                         AS crm_account_user_sales_segment_region_grouped,

      -- crm opp owner/account owner fields stamped at SAO date
      -- If the fiscal year of the SAO date is lower than the current fiscal year, use the sales hierarchy from the account owner
      -- If not, use the stamped hierarchy
      dim_crm_opportunity.sao_crm_opp_owner_stamped_name,
      dim_crm_opportunity.sao_crm_account_owner_stamped_name,
      IFF(dim_date_sao_date.fiscal_year < dim_date_sao_date.current_fiscal_year,
        crm_account_user_sales_segment, dim_crm_opportunity.sao_crm_opp_owner_sales_segment_stamped)
                                                                                         AS sao_crm_opp_owner_sales_segment_stamped,
      IFF(dim_date_sao_date.fiscal_year < dim_date_sao_date.current_fiscal_year,
        crm_account_user_sales_segment_grouped, dim_crm_opportunity.sao_crm_opp_owner_sales_segment_stamped_grouped)
                                                                                         AS sao_crm_opp_owner_sales_segment_stamped_grouped,
      IFF(dim_date_sao_date.fiscal_year < dim_date_sao_date.current_fiscal_year,
        crm_account_user_geo, dim_crm_opportunity.sao_crm_opp_owner_geo_stamped)
                                                                                         AS sao_crm_opp_owner_geo_stamped,
      IFF(dim_date_sao_date.fiscal_year < dim_date_sao_date.current_fiscal_year,
        crm_account_user_region, dim_crm_opportunity.sao_crm_opp_owner_region_stamped)
                                                                                         AS sao_crm_opp_owner_region_stamped,
      IFF(dim_date_sao_date.fiscal_year < dim_date_sao_date.current_fiscal_year,
        crm_account_user_area, dim_crm_opportunity.sao_crm_opp_owner_area_stamped)
                                                                                         AS sao_crm_opp_owner_area_stamped,
      IFF(dim_date_sao_date.fiscal_year < dim_date_sao_date.current_fiscal_year,
        crm_account_user_sales_segment_region_grouped, dim_crm_opportunity.sao_crm_opp_owner_segment_region_stamped_grouped)
                                                                                         AS sao_crm_opp_owner_segment_region_stamped_grouped,

      -- crm opp owner/account owner stamped fields stamped at close date
      -- If the fiscal year of close date is lower than the current fiscal year, use the sales hierarchy from the account owner
      -- If not, use the stamped hierarchy
      dim_crm_opportunity.crm_opp_owner_stamped_name,
      dim_crm_opportunity.crm_account_owner_stamped_name,

      IFF(dim_date_close_date.fiscal_year < dim_date_close_date.current_fiscal_year,
        crm_account_user_sales_segment, dim_crm_user_hierarchy_stamped_sales_segment.crm_opp_owner_sales_segment_stamped)
                                                                                         AS crm_opp_owner_sales_segment_stamped,
      IFF(dim_date_close_date.fiscal_year < dim_date_close_date.current_fiscal_year,
        crm_account_user_sales_segment_grouped, dim_crm_user_hierarchy_stamped_sales_segment.crm_opp_owner_sales_segment_stamped_grouped)
                                                                                         AS crm_opp_owner_sales_segment_stamped_grouped,
      IFF(dim_date_close_date.fiscal_year < dim_date_close_date.current_fiscal_year,
        crm_account_user_geo, dim_crm_user_hierarchy_stamped_geo.crm_opp_owner_geo_stamped)
                                                                                         AS crm_opp_owner_geo_stamped,
      IFF(dim_date_close_date.fiscal_year < dim_date_close_date.current_fiscal_year,
        crm_account_user_region, dim_crm_user_hierarchy_stamped_region.crm_opp_owner_region_stamped)
                                                                                         AS crm_opp_owner_region_stamped,
      IFF(dim_date_close_date.fiscal_year < dim_date_close_date.current_fiscal_year,
        crm_account_user_area, dim_crm_user_hierarchy_stamped_area.crm_opp_owner_area_stamped)
                                                                                         AS crm_opp_owner_area_stamped,
      IFF(dim_date_close_date.fiscal_year < dim_date_close_date.current_fiscal_year,
        crm_account_user_sales_segment_region_grouped,
          {{ sales_segment_region_grouped('dim_crm_user_hierarchy_stamped_sales_segment.crm_opp_owner_sales_segment_stamped',
        'dim_crm_user_hierarchy_stamped_geo.crm_opp_owner_geo_stamped', 'dim_crm_user_hierarchy_stamped_region.crm_opp_owner_region_stamped') }} )
                                                                                         AS crm_opp_owner_sales_segment_region_stamped_grouped,

      -- channel fields
      fct_crm_opportunity.lead_source,
      fct_crm_opportunity.dr_partner_deal_type,
      fct_crm_opportunity.partner_account,
      fct_crm_opportunity.dr_status,
      fct_crm_opportunity.distributor,
      fct_crm_opportunity.dr_deal_id,
      fct_crm_opportunity.dr_primary_registration,
      fct_crm_opportunity.influence_partner,
      fct_crm_opportunity.fulfillment_partner,
      fct_crm_opportunity.platform_partner,
      fct_crm_opportunity.partner_track,
      fct_crm_opportunity.resale_partner_track,
      fct_crm_opportunity.is_public_sector_opp,
      fct_crm_opportunity.is_registration_from_portal,
      fct_crm_opportunity.calculated_discount,
      fct_crm_opportunity.partner_discount,
      fct_crm_opportunity.partner_discount_calc,
      fct_crm_opportunity.comp_channel_neutral,
      fct_crm_opportunity.count_crm_attribution_touchpoints,
      fct_crm_opportunity.weighted_linear_iacv,
      fct_crm_opportunity.count_campaigns,

      -- Solutions-Architech fields
      dim_crm_opportunity.sa_tech_evaluation_close_status,
      dim_crm_opportunity.sa_tech_evaluation_end_date,
      dim_crm_opportunity.sa_tech_evaluation_start_date,


      -- Command Plan fields
      dim_crm_opportunity.cp_partner,
      dim_crm_opportunity.cp_paper_process,
      dim_crm_opportunity.cp_help,
      dim_crm_opportunity.cp_review_notes

    FROM fct_crm_opportunity
    LEFT JOIN dim_crm_opportunity
      ON fct_crm_opportunity.dim_crm_opportunity_id = dim_crm_opportunity.dim_crm_opportunity_id
    LEFT JOIN dim_crm_account
      ON dim_crm_opportunity.dim_crm_account_id = dim_crm_account.dim_crm_account_id
    LEFT JOIN dim_sales_qualified_source
      ON fct_crm_opportunity.dim_sales_qualified_source_id = dim_sales_qualified_source.dim_sales_qualified_source_id
    LEFT JOIN dim_deal_path
      ON fct_crm_opportunity.dim_deal_path_id = dim_deal_path.dim_deal_path_id
    LEFT JOIN dim_order_type
      ON fct_crm_opportunity.dim_order_type_id = dim_order_type.dim_order_type_id
    LEFT JOIN dim_dr_partner_engagement
      ON fct_crm_opportunity.dim_dr_partner_engagement_id = dim_dr_partner_engagement.dim_dr_partner_engagement_id
    LEFT JOIN dim_alliance_type AS dim_alliance_type_current
      ON fct_crm_opportunity.dim_alliance_type_current_id = dim_alliance_type_current.dim_alliance_type_id
    LEFT JOIN dim_channel_type
      ON fct_crm_opportunity.dim_channel_type_id = dim_channel_type.dim_channel_type_id
    LEFT JOIN dim_crm_user_hierarchy_stamped_sales_segment
      ON fct_crm_opportunity.dim_crm_opp_owner_sales_segment_stamped_id = dim_crm_user_hierarchy_stamped_sales_segment.dim_crm_opp_owner_sales_segment_stamped_id
    LEFT JOIN dim_crm_user_hierarchy_stamped_geo
      ON fct_crm_opportunity.dim_crm_opp_owner_geo_stamped_id = dim_crm_user_hierarchy_stamped_geo.dim_crm_opp_owner_geo_stamped_id
    LEFT JOIN dim_crm_user_hierarchy_stamped_region
      ON fct_crm_opportunity.dim_crm_opp_owner_region_stamped_id = dim_crm_user_hierarchy_stamped_region.dim_crm_opp_owner_region_stamped_id
    LEFT JOIN dim_crm_user_hierarchy_stamped_area
      ON fct_crm_opportunity.dim_crm_opp_owner_area_stamped_id = dim_crm_user_hierarchy_stamped_area.dim_crm_opp_owner_area_stamped_id
    LEFT JOIN dim_crm_user_hierarchy_live_sales_segment
      ON fct_crm_opportunity.dim_crm_user_sales_segment_id = dim_crm_user_hierarchy_live_sales_segment.dim_crm_user_sales_segment_id
    LEFT JOIN dim_crm_user_hierarchy_live_geo
      ON fct_crm_opportunity.dim_crm_user_geo_id = dim_crm_user_hierarchy_live_geo.dim_crm_user_geo_id
    LEFT JOIN dim_crm_user_hierarchy_live_region
      ON fct_crm_opportunity.dim_crm_user_region_id = dim_crm_user_hierarchy_live_region.dim_crm_user_region_id
    LEFT JOIN dim_crm_user_hierarchy_live_area
      ON fct_crm_opportunity.dim_crm_user_area_id = dim_crm_user_hierarchy_live_area.dim_crm_user_area_id
    LEFT JOIN dim_crm_user_hierarchy_live_sales_segment               AS dim_crm_account_user_hierarchy_live_sales_segment
      ON fct_crm_opportunity.dim_crm_account_user_sales_segment_id = dim_crm_account_user_hierarchy_live_sales_segment.dim_crm_user_sales_segment_id
    LEFT JOIN dim_crm_user_hierarchy_live_geo                         AS dim_crm_account_user_hierarchy_live_geo
      ON fct_crm_opportunity.dim_crm_account_user_geo_id = dim_crm_account_user_hierarchy_live_geo.dim_crm_user_geo_id
    LEFT JOIN dim_crm_user_hierarchy_live_region                      AS dim_crm_account_user_hierarchy_live_region
      ON fct_crm_opportunity.dim_crm_account_user_region_id = dim_crm_account_user_hierarchy_live_region.dim_crm_user_region_id
    LEFT JOIN dim_crm_user_hierarchy_live_area                        AS dim_crm_account_user_hierarchy_live_area
      ON fct_crm_opportunity.dim_crm_account_user_area_id = dim_crm_account_user_hierarchy_live_area.dim_crm_user_area_id
    LEFT JOIN dim_date_extended                                       AS dim_date_close_date
      ON fct_crm_opportunity.close_date = dim_date_close_date.date_day
    LEFT JOIN dim_date_extended                                       AS dim_date_sao_date
      ON fct_crm_opportunity.sales_accepted_date = dim_date_sao_date.date_day
    LEFT JOIN dim_date AS arr_created_date
      ON arr_created_date.date_id = fct_crm_opportunity.arr_created_date_id
)

{{ dbt_audit(
    cte_ref="final",
    created_by="@jeanpeguero",
    updated_by="@michellecooper",
    created_date="2022-02-28",
    updated_date="2022-08-08"
  ) }}
