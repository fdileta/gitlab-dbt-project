{{ config(
    tags=["mnpi_exception"]
) }}

{{config({
    "schema": "common_mart_marketing"
  })
}}

{{ simple_cte([
    ('dim_crm_touchpoint','dim_crm_touchpoint'),
    ('fct_crm_attribution_touchpoint','fct_crm_attribution_touchpoint'),
    ('dim_campaign','dim_campaign'),
    ('fct_campaign','fct_campaign'),
    ('dim_crm_person','dim_crm_person'),
    ('fct_crm_person', 'fct_crm_person'),
    ('dim_crm_account','dim_crm_account'),
    ('dim_crm_user','dim_crm_user'),
    ('mart_crm_opportunity','mart_crm_opportunity')
]) }}

, final AS (

    SELECT
      -- touchpoint info
      dim_crm_touchpoint.dim_crm_touchpoint_id,
      dim_crm_touchpoint.bizible_touchpoint_date,
      dim_crm_touchpoint.bizible_touchpoint_position,
      dim_crm_touchpoint.bizible_touchpoint_source,
      dim_crm_touchpoint.bizible_touchpoint_source_type,
      dim_crm_touchpoint.bizible_touchpoint_type,
      dim_crm_touchpoint.bizible_ad_campaign_name,
      dim_crm_touchpoint.bizible_ad_content,
      dim_crm_touchpoint.bizible_ad_group_name,
      dim_crm_touchpoint.bizible_form_url,
      dim_crm_touchpoint.bizible_form_url_raw,
      dim_crm_touchpoint.bizible_landing_page,
      dim_crm_touchpoint.bizible_landing_page_raw,
      dim_crm_touchpoint.bizible_marketing_channel,
      dim_crm_touchpoint.bizible_marketing_channel_path,
      dim_crm_touchpoint.bizible_medium,
      dim_crm_touchpoint.bizible_referrer_page,
      dim_crm_touchpoint.bizible_referrer_page_raw,
      dim_crm_touchpoint.bizible_salesforce_campaign,
      dim_crm_touchpoint.bizible_integrated_campaign_grouping,
      dim_crm_touchpoint.touchpoint_segment,
      dim_crm_touchpoint.gtm_motion,
      dim_crm_touchpoint.integrated_campaign_grouping,
      dim_crm_touchpoint.pipe_name,
      fct_crm_attribution_touchpoint.bizible_count_first_touch,
      fct_crm_attribution_touchpoint.bizible_count_lead_creation_touch,
      fct_crm_attribution_touchpoint.bizible_attribution_percent_full_path,
      fct_crm_attribution_touchpoint.bizible_count_custom_model,
      fct_crm_attribution_touchpoint.bizible_count_u_shaped,
      fct_crm_attribution_touchpoint.bizible_count_w_shaped,
      fct_crm_attribution_touchpoint.bizible_revenue_full_path,
      fct_crm_attribution_touchpoint.bizible_revenue_custom_model,
      fct_crm_attribution_touchpoint.bizible_revenue_first_touch,
      fct_crm_attribution_touchpoint.bizible_revenue_lead_conversion,
      fct_crm_attribution_touchpoint.bizible_revenue_u_shaped,
      fct_crm_attribution_touchpoint.bizible_revenue_w_shaped,

      -- person info
      fct_crm_attribution_touchpoint.dim_crm_person_id,
      dim_crm_person.sfdc_record_id,
      dim_crm_person.sfdc_record_type,
      dim_crm_person.email_hash,
      dim_crm_person.email_domain,
      dim_crm_person.owner_id,
      dim_crm_person.person_score,
      dim_crm_person.title                                                  AS crm_person_title,
      dim_crm_person.country                                                AS crm_person_country,
      dim_crm_person.state                                                  AS crm_person_state,
      dim_crm_person.status                                                 AS crm_person_status,
      dim_crm_person.lead_source,
      dim_crm_person.lead_source_type,
      dim_crm_person.source_buckets                                         AS crm_person_source_buckets,
      dim_crm_person.net_new_source_categories,
      dim_crm_person.crm_partner_id,
      fct_crm_person.created_date                                           AS crm_person_created_date,
      fct_crm_person.inquiry_date,
      fct_crm_person.mql_date_first,
      fct_crm_person.mql_date_latest,
      fct_crm_person.legacy_mql_date_first,
      fct_crm_person.legacy_mql_date_latest,
      fct_crm_person.accepted_date,
      fct_crm_person.qualifying_date,
      fct_crm_person.qualified_date,
      fct_crm_person.converted_date,
      fct_crm_person.is_mql,
      fct_crm_person.is_inquiry,
      fct_crm_person.mql_count,
      fct_crm_person.last_utm_content,
      fct_crm_person.last_utm_campaign,

      -- campaign info
      dim_campaign.dim_campaign_id,
      dim_campaign.campaign_name,
      dim_campaign.is_active                                                AS campaign_is_active,
      dim_campaign.status                                                   AS campagin_status,
      dim_campaign.type,
      dim_campaign.description,
      dim_campaign.budget_holder,
      dim_campaign.bizible_touchpoint_enabled_setting,
      dim_campaign.strategic_marketing_contribution,
      dim_campaign.large_bucket,
      dim_campaign.reporting_type,
      dim_campaign.allocadia_id,
      dim_campaign.is_a_channel_partner_involved,
      dim_campaign.is_an_alliance_partner_involved,
      dim_campaign.is_this_an_in_person_event,
      dim_campaign.will_there_be_mdf_funding,
      dim_campaign.alliance_partner_name,
      dim_campaign.channel_partner_name,
      dim_campaign.sales_play,
      dim_campaign.total_planned_mqls,
      fct_campaign.dim_parent_campaign_id,
      fct_campaign.campaign_owner_id,
      fct_campaign.created_by_id                                            AS campaign_created_by_id,
      fct_campaign.start_date                                               AS campaign_start_date,
      fct_campaign.end_date                                                 AS campaign_end_date,
      fct_campaign.created_date                                             AS campaign_created_date,
      fct_campaign.last_modified_date                                       AS campaign_last_modified_date,
      fct_campaign.last_activity_date                                       AS campaign_last_activity_date,
      fct_campaign.region                                                   AS campaign_region,
      fct_campaign.sub_region                                               AS campaign_sub_region,
      fct_campaign.budgeted_cost,
      fct_campaign.expected_response,
      fct_campaign.expected_revenue,
      fct_campaign.actual_cost,
      fct_campaign.amount_all_opportunities,
      fct_campaign.amount_won_opportunities,
      fct_campaign.count_contacts,
      fct_campaign.count_converted_leads,
      fct_campaign.count_leads,
      fct_campaign.count_opportunities,
      fct_campaign.count_responses,
      fct_campaign.count_won_opportunities,
      fct_campaign.count_sent,

      -- campaign owner info
      campaign_owner.user_name                             AS campaign_rep_name,
      campaign_owner.title                                 AS campaign_rep_title,
      campaign_owner.team                                  AS campaign_rep_team,
      campaign_owner.is_active                             AS campaign_rep_is_active,
      campaign_owner.user_role_name                        AS campaign_rep_role_name,
      campaign_owner.crm_user_sales_segment                AS campaign_crm_user_segment_name_live,
      campaign_owner.crm_user_geo                          AS campaign_crm_user_geo_name_live,
      campaign_owner.crm_user_region                       AS campaign_crm_user_region_name_live,
      campaign_owner.crm_user_area                         AS campaign_crm_user_area_name_live,

      -- sales rep info
      dim_crm_user.user_name                                AS rep_name,
      dim_crm_user.title                                    AS rep_title,
      dim_crm_user.team,
      dim_crm_user.is_active                                AS rep_is_active,
      dim_crm_user.user_role_name,
      dim_crm_user.crm_user_sales_segment                   AS touchpoint_crm_user_segment_name_live,
      dim_crm_user.crm_user_geo                             AS touchpoint_crm_user_geo_name_live,
      dim_crm_user.crm_user_region                          AS touchpoint_crm_user_region_name_live,
      dim_crm_user.crm_user_area                            AS touchpoint_crm_user_area_name_live,

      -- account info
      dim_crm_account.dim_crm_account_id,
      dim_crm_account.crm_account_name,
      dim_crm_account.crm_account_billing_country,
      dim_crm_account.crm_account_industry,
      dim_crm_account.crm_account_owner_team,
      dim_crm_account.crm_account_sales_territory,
      dim_crm_account.crm_account_tsp_region,
      dim_crm_account.crm_account_tsp_sub_region,
      dim_crm_account.crm_account_tsp_area,
      dim_crm_account.crm_account_gtm_strategy,
      dim_crm_account.crm_account_focus_account,
      dim_crm_account.health_score,
      dim_crm_account.health_number,
      dim_crm_account.health_score_color,
      dim_crm_account.dim_parent_crm_account_id,
      dim_crm_account.parent_crm_account_name,
      dim_crm_account.parent_crm_account_sales_segment,
      dim_crm_account.parent_crm_account_billing_country,
      dim_crm_account.parent_crm_account_industry,
      dim_crm_account.parent_crm_account_owner_team,
      dim_crm_account.parent_crm_account_sales_territory,
      dim_crm_account.parent_crm_account_tsp_region,
      dim_crm_account.parent_crm_account_tsp_sub_region,
      dim_crm_account.parent_crm_account_tsp_area,
      dim_crm_account.parent_crm_account_gtm_strategy,
      dim_crm_account.parent_crm_account_focus_account,
      dim_crm_account.crm_account_owner_user_segment,
      dim_crm_account.record_type_id,
      dim_crm_account.federal_account,
      dim_crm_account.gitlab_com_user,
      dim_crm_account.crm_account_type,
      dim_crm_account.technical_account_manager,
      dim_crm_account.merged_to_account_id,
      dim_crm_account.is_reseller,

      -- opportunity info
      fct_crm_attribution_touchpoint.dim_crm_opportunity_id,
      mart_crm_opportunity.sales_accepted_date,
      mart_crm_opportunity.sales_accepted_month,
      mart_crm_opportunity.close_date                                       AS opportunity_close_date,
      mart_crm_opportunity.close_month                                      AS opportunity_close_month,
      mart_crm_opportunity.created_date                                     AS opportunity_created_date,
      mart_crm_opportunity.created_month                                    AS opportunity_created_month,
      mart_crm_opportunity.is_won,
      mart_crm_opportunity.is_closed,
      mart_crm_opportunity.days_in_sao,
      mart_crm_opportunity.iacv,
      mart_crm_opportunity.net_arr,
      mart_crm_opportunity.amount,
      mart_crm_opportunity.is_edu_oss,
      mart_crm_opportunity.stage_name,
      mart_crm_opportunity.reason_for_loss,
      mart_crm_opportunity.is_sao,
      mart_crm_opportunity.crm_opp_owner_sales_segment_stamped               AS crm_opp_owner_sales_segment_stamped,
      mart_crm_opportunity.crm_opp_owner_geo_stamped                         AS crm_opp_owner_geo_stamped,
      mart_crm_opportunity.crm_opp_owner_region_stamped                      AS crm_opp_owner_region_stamped,
      mart_crm_opportunity.crm_opp_owner_area_stamped                        AS crm_opp_owner_area_stamped,
      mart_crm_opportunity.crm_user_sales_segment,
      mart_crm_opportunity.crm_user_geo,
      mart_crm_opportunity.crm_user_region,
      mart_crm_opportunity.crm_user_area,
      mart_crm_opportunity.deal_path_name,
      mart_crm_opportunity.order_type,
      mart_crm_opportunity.sales_qualified_source_name,
      mart_crm_opportunity.sales_type,
      mart_crm_opportunity.closed_buckets,
      mart_crm_opportunity.source_buckets                                   AS opportunity_source_buckets,
      mart_crm_opportunity.opportunity_sales_development_representative,
      mart_crm_opportunity.opportunity_business_development_representative,
      mart_crm_opportunity.sdr_or_bdr,
      mart_crm_opportunity.opportunity_development_representative,
      mart_crm_opportunity.is_web_portal_purchase,
      mart_crm_opportunity.count_crm_attribution_touchpoints                AS crm_attribution_touchpoints_per_opp,
      mart_crm_opportunity.weighted_linear_iacv,
      mart_crm_opportunity.count_campaigns                                  AS count_campaigns_per_opp,
      (mart_crm_opportunity.iacv / mart_crm_opportunity.count_campaigns)    AS iacv_per_campaign,

      -- bizible influenced
       CASE
        WHEN  dim_campaign.budget_holder = 'fmm'
              OR campaign_rep_role_name = 'Field Marketing Manager'
              OR LOWER(dim_crm_touchpoint.utm_content) LIKE '%field%'
              OR LOWER(dim_campaign.type) = 'field event'
              OR LOWER(dim_crm_person.lead_source) = 'field event'
        THEN 1
        ELSE 0
      END AS is_fmm_influenced

    FROM fct_crm_attribution_touchpoint
    LEFT JOIN dim_crm_touchpoint
      ON fct_crm_attribution_touchpoint.dim_crm_touchpoint_id = dim_crm_touchpoint.dim_crm_touchpoint_id
    LEFT JOIN dim_campaign
      ON fct_crm_attribution_touchpoint.dim_campaign_id = dim_campaign.dim_campaign_id
    LEFT JOIN fct_campaign
      ON fct_crm_attribution_touchpoint.dim_campaign_id = fct_campaign.dim_campaign_id
    LEFT JOIN dim_crm_person
      ON fct_crm_attribution_touchpoint.dim_crm_person_id = dim_crm_person.dim_crm_person_id
    LEFT JOIN fct_crm_person
      ON fct_crm_attribution_touchpoint.dim_crm_person_id = fct_crm_person.dim_crm_person_id
    LEFT JOIN dim_crm_account
      ON fct_crm_attribution_touchpoint.dim_crm_account_id = dim_crm_account.dim_crm_account_id
    LEFT JOIN dim_crm_user
      ON fct_crm_attribution_touchpoint.dim_crm_user_id = dim_crm_user.dim_crm_user_id
    LEFT JOIN dim_crm_user AS campaign_owner
      ON fct_campaign.campaign_owner_id = campaign_owner.dim_crm_user_id
    LEFT JOIN mart_crm_opportunity
      ON fct_crm_attribution_touchpoint.dim_crm_opportunity_id = mart_crm_opportunity.dim_crm_opportunity_id
)

{{ dbt_audit(
    cte_ref="final",
    created_by="@mcooperDD",
    updated_by="@rkohnke",
    created_date="2020-02-18",
    updated_date="2022-05-06"
) }}
