{{ config(
    tags=["mnpi_exception"]
) }}

WITH bizible_attribution_touchpoints AS (

    SELECT *
    FROM {{ ref('sfdc_bizible_attribution_touchpoint_source') }}
    WHERE is_deleted = 'FALSE'

), crm_person AS (

    SELECT *
    FROM {{ ref('prep_crm_person') }}

), opportunity_dimensions AS (

    SELECT *
    FROM {{ ref('map_crm_opportunity') }}

), campaigns_per_opp AS (

    SELECT
      opportunity_id,
      COUNT(DISTINCT campaign_id) AS campaigns_per_opp
      FROM bizible_attribution_touchpoints
    GROUP BY 1

), opps_per_touchpoint AS (

    SELECT 
      touchpoint_id,
      COUNT(DISTINCT opportunity_id) AS opps_per_touchpoint
    FROM bizible_attribution_touchpoints
    GROUP BY 1

), final_attribution_touchpoint AS (

    SELECT
      bizible_attribution_touchpoints.touchpoint_id AS dim_crm_touchpoint_id,

      -- shared dimension keys
      bizible_attribution_touchpoints.campaign_id AS dim_campaign_id,
      bizible_attribution_touchpoints.opportunity_id AS dim_crm_opportunity_id,
      bizible_attribution_touchpoints.bizible_account AS dim_crm_account_id,
      crm_person.dim_crm_person_id,
      opportunity_dimensions.dim_crm_user_id,
      opportunity_dimensions.dim_order_type_id,
      opportunity_dimensions.dim_sales_qualified_source_id,
      opportunity_dimensions.dim_deal_path_id,
      opportunity_dimensions.dim_parent_crm_account_id,
      opportunity_dimensions.dim_parent_sales_segment_id,
      opportunity_dimensions.dim_parent_sales_territory_id,
      opportunity_dimensions.dim_parent_industry_id,
      opportunity_dimensions.dim_parent_location_country_id,
      opportunity_dimensions.dim_parent_location_region_id,
      opportunity_dimensions.dim_account_sales_segment_id,
      opportunity_dimensions.dim_account_sales_territory_id,
      opportunity_dimensions.dim_account_industry_id,
      opportunity_dimensions.dim_account_location_country_id,
      opportunity_dimensions.dim_account_location_region_id,

      -- counts
      opps_per_touchpoint.opps_per_touchpoint,
      campaigns_per_opp.campaigns_per_opp,
      bizible_attribution_touchpoints.bizible_count_first_touch,
      bizible_attribution_touchpoints.bizible_count_lead_creation_touch,
      bizible_attribution_touchpoints.bizible_attribution_percent_full_path,
      bizible_attribution_touchpoints.bizible_count_u_shaped,
      bizible_attribution_touchpoints.bizible_count_w_shaped,
      bizible_attribution_touchpoints.bizible_count_custom_model,

      -- touchpoint revenue info
      bizible_attribution_touchpoints.bizible_revenue_full_path,
      bizible_attribution_touchpoints.bizible_revenue_custom_model,
      bizible_attribution_touchpoints.bizible_revenue_first_touch,
      bizible_attribution_touchpoints.bizible_revenue_lead_conversion,
      bizible_attribution_touchpoints.bizible_revenue_u_shaped,
      bizible_attribution_touchpoints.bizible_revenue_w_shaped

    FROM bizible_attribution_touchpoints
    LEFT JOIN crm_person
      ON bizible_attribution_touchpoints.bizible_contact = crm_person.sfdc_record_id
    LEFT JOIN opportunity_dimensions
      ON bizible_attribution_touchpoints.opportunity_id = opportunity_dimensions.dim_crm_opportunity_id
    LEFT JOIN  campaigns_per_opp 
      ON bizible_attribution_touchpoints.opportunity_id =  campaigns_per_opp.opportunity_id
    LEFT JOIN opps_per_touchpoint
      ON bizible_attribution_touchpoints.touchpoint_id = opps_per_touchpoint.touchpoint_id

)

{{ dbt_audit(
    cte_ref="final_attribution_touchpoint",
    created_by="@mcooperDD",
    updated_by="@michellecooper",
    created_date="2021-01-21",
    updated_date="2022-09-12"
) }}
