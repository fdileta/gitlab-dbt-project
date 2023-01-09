{{ config(
    tags=["mnpi_exception"]
) }}

{{ simple_cte([
    ('mart_crm_touchpoint','mart_crm_touchpoint'),
    ('mart_crm_attribution_touchpoint','mart_crm_attribution_touchpoint')
])}}

, unioned AS (
 SELECT 
      mart_crm_touchpoint.dim_crm_touchpoint_id,
      mart_crm_touchpoint.bizible_touchpoint_month,
      mart_crm_touchpoint.bizible_touchpoint_date,
      mart_crm_touchpoint.bizible_integrated_campaign_grouping,
      mart_crm_touchpoint.bizible_marketing_channel_path,
      mart_crm_touchpoint.bizible_marketing_channel,
      CASE 
        WHEN mart_crm_touchpoint.account_demographics_geo = 'NORAM' 
          THEN 'AMER'
        ELSE mart_crm_touchpoint.account_demographics_geo 
      END AS account_demographics_geo_normalized,
      IFF(mart_crm_touchpoint.touchpoint_crm_user_segment_name_live  IS null,'Unknown',mart_crm_touchpoint.touchpoint_crm_user_segment_name_live) AS touchpoint_crm_user_segment_name_live,
      mart_crm_touchpoint.crm_person_status,
      mart_crm_touchpoint.bizible_touchpoint_type, 
      mart_crm_touchpoint.bizible_touchpoint_position,
      null AS sales_type,
      null AS opportunity_created_date,
      null AS sales_accepted_date,
      null AS opportunity_close_date, 
      null AS stage_name,
      null AS is_won,
      null AS is_sao,
      null AS deal_path_name,
      null AS order_type,
      mart_crm_touchpoint.bizible_landing_page,
      mart_crm_touchpoint.bizible_form_url,
      mart_crm_touchpoint.dim_crm_account_id,
      null AS dim_crm_opportunity_id,
      mart_crm_touchpoint.crm_account_name AS crm_account_name, 
      mart_crm_touchpoint.crm_account_gtm_strategy,
      UPPER(mart_crm_touchpoint.crm_person_country) AS crm_person_country,
      mart_crm_touchpoint.bizible_medium AS bizible_medium,
      mart_crm_touchpoint.touchpoint_segment,
      mart_crm_touchpoint.gtm_motion,
      mart_crm_touchpoint.last_utm_campaign,
      mart_crm_touchpoint.last_utm_content,
      mart_crm_touchpoint.bizible_ad_campaign_name,
      mart_crm_touchpoint.lead_source,
      mart_crm_touchpoint.type,
      mart_crm_touchpoint.mql_date_first::date AS mql_date_first,
      mart_crm_touchpoint.true_inquiry_date,
      mart_crm_touchpoint.dim_crm_person_id AS dim_crm_person_id,
      mart_crm_touchpoint.email_hash AS email_hash,
      is_inquiry,
      is_mql,
      is_fmm_influenced,
      is_dg_influenced,
      0 AS Total_cost,
      1 AS touchpoint_sum,
      mart_crm_touchpoint.bizible_touchpoint_source,
      mart_crm_touchpoint.bizible_ad_content,
      mart_crm_touchpoint.bizible_ad_group_name,
      mart_crm_touchpoint.bizible_form_url_raw,
      mart_crm_touchpoint.bizible_landing_page_raw,
      mart_crm_touchpoint.bizible_referrer_page,
      mart_crm_touchpoint.bizible_referrer_page_raw,
      mart_crm_touchpoint.campaign_name AS bizible_salesforce_campaign,
      mart_crm_touchpoint.dim_campaign_id,
      mart_crm_touchpoint.campaign_rep_role_name,
      NULL AS pipe_name,
      SUM(mart_crm_touchpoint.bizible_count_lead_creation_touch) AS new_lead_created_sum,
      SUM(mart_crm_touchpoint.count_true_inquiry) AS count_true_inquiry,
      SUM(mart_crm_touchpoint.count_inquiry) AS inquiry_sum, 
      SUM(mart_crm_touchpoint.pre_mql_weight) AS mql_sum,
      SUM(mart_crm_touchpoint.count_accepted) AS accepted_sum,
      SUM(mart_crm_touchpoint.count_net_new_mql) AS new_mql_sum,
      SUM(mart_crm_touchpoint.count_net_new_accepted) AS new_accepted_sum,
      0 AS first_opp_created,
      0 AS u_shaped_opp_created,
      0 AS w_shaped_opp_created,
      0 AS full_shaped_opp_created,
      0 AS custom_opp_created,
      0 AS linear_opp_created,
      0 AS first_net_arr,
      0 AS u_net_arr,
      0 AS w_net_arr,
      0 AS full_net_arr,
      0 AS custom_net_arr,
      0 AS linear_net_arr,
      0 AS first_sao,
      0 AS u_shaped_sao,
      0 AS w_shaped_sao,
      0 AS full_shaped_sao,
      0 AS custom_sao,
      0 AS linear_sao,
      0 AS pipeline_first_net_arr,
      0 AS pipeline_u_net_arr,
      0 AS pipeline_w_net_arr,
      0 AS pipeline_full_net_arr,
      0 AS pipeline_custom_net_arr,
      0 AS pipeline_linear_net_arr,
      0 AS won_first,
      0 AS won_u,
      0 AS won_w,
      0 AS won_full,
      0 AS won_custom,
      0 AS won_linear,
      0 AS won_first_net_arr,
      0 AS won_u_net_arr,
      0 AS won_w_net_arr,
      0 AS won_full_net_arr,
      0 AS won_custom_net_arr,
      0 AS won_linear_net_arr
    FROM mart_crm_touchpoint
    {{ dbt_utils.group_by(56) }}
    
    UNION ALL
    
    SELECT
      mart_crm_attribution_touchpoint.dim_crm_touchpoint_id,
      mart_crm_attribution_touchpoint.bizible_touchpoint_month,
      mart_crm_attribution_touchpoint.bizible_touchpoint_date,
      mart_crm_attribution_touchpoint.bizible_integrated_campaign_grouping,
      mart_crm_attribution_touchpoint.bizible_marketing_channel_path,
      mart_crm_attribution_touchpoint.bizible_marketing_channel,
      CASE 
        WHEN mart_crm_attribution_touchpoint.account_demographics_geo = 'NORAM' 
          THEN 'AMER'
        ELSE mart_crm_attribution_touchpoint.account_demographics_geo 
      END AS account_demographics_geo_normalized,
      IFF(mart_crm_attribution_touchpoint.touchpoint_crm_user_segment_name_live IS null,'Unknown',mart_crm_attribution_touchpoint.touchpoint_crm_user_segment_name_live) AS touchpoint_crm_user_segment_name_live,
      null AS crm_person_status,
      mart_crm_attribution_touchpoint.bizible_touchpoint_type,
      mart_crm_attribution_touchpoint.bizible_touchpoint_position, 
      mart_crm_attribution_touchpoint.sales_type,
      mart_crm_attribution_touchpoint.opportunity_created_date,
      mart_crm_attribution_touchpoint.sales_accepted_date,
      mart_crm_attribution_touchpoint.opportunity_close_date,
      mart_crm_attribution_touchpoint.stage_name,
      mart_crm_attribution_touchpoint.is_won,
      mart_crm_attribution_touchpoint.is_sao,
      mart_crm_attribution_touchpoint.deal_path_name,
      mart_crm_attribution_touchpoint.order_type AS order_type,
      mart_crm_attribution_touchpoint.bizible_landing_page,
      mart_crm_attribution_touchpoint.bizible_form_url,
      mart_crm_attribution_touchpoint.dim_crm_account_id,
      mart_crm_attribution_touchpoint.dim_crm_opportunity_id AS dim_crm_opportunity_id,
      mart_crm_attribution_touchpoint.crm_account_name,
      mart_crm_attribution_touchpoint.crm_account_gtm_strategy,
      UPPER(mart_crm_attribution_touchpoint.crm_person_country) AS crm_person_country,
      mart_crm_attribution_touchpoint.bizible_medium AS bizible_medium,
      mart_crm_attribution_touchpoint.touchpoint_segment,
      mart_crm_attribution_touchpoint.gtm_motion,
      mart_crm_attribution_touchpoint.last_utm_campaign,
      mart_crm_attribution_touchpoint.last_utm_content,
      mart_crm_attribution_touchpoint.bizible_ad_campaign_name,
      mart_crm_attribution_touchpoint.lead_source,
      mart_crm_attribution_touchpoint.type,
      null AS mql_date_first,
      null AS true_inquiry_date,
      null AS dim_crm_person_id,
      email_hash AS email_hash,
      null AS is_inquiry,
      null AS is_mql,
      is_fmm_influenced,
      CASE 
        WHEN touchpoint_segment = 'Demand Gen' THEN 1 -- move back to dimension
        ELSE 0
      END AS is_dg_influenced,
      0 AS total_cost,
      0 AS touchpoint_sum,
      mart_crm_attribution_touchpoint.bizible_touchpoint_source,
      mart_crm_attribution_touchpoint.bizible_ad_content,
      mart_crm_attribution_touchpoint.bizible_ad_group_name,
      mart_crm_attribution_touchpoint.bizible_form_url_raw,
      mart_crm_attribution_touchpoint.bizible_landing_page_raw,
      mart_crm_attribution_touchpoint.bizible_referrer_page,
      mart_crm_attribution_touchpoint.bizible_referrer_page_raw,
      mart_crm_attribution_touchpoint.campaign_name AS bizible_salesforce_campaign,
      mart_crm_attribution_touchpoint.dim_campaign_id,
      mart_crm_attribution_touchpoint.campaign_rep_role_name,
      mart_crm_attribution_touchpoint.pipe_name,
      0 AS new_lead_created_sum,
      0 AS count_true_inquiry, 
      0 AS inquiry_sum,
      0 AS mql_sum,
      0 AS accepted_sum,
      0 AS new_mql_sum,
      0 AS new_accepted_sum,
      SUM(mart_crm_attribution_touchpoint.bizible_count_first_touch) AS first_opp_created,
      SUM(mart_crm_attribution_touchpoint.bizible_count_u_shaped) AS u_shaped_opp_created,
      SUM(mart_crm_attribution_touchpoint.bizible_count_w_shaped) AS w_shaped_opp_created,
      SUM(mart_crm_attribution_touchpoint.bizible_count_first_touch) AS full_shaped_opp_created,
      SUM(mart_crm_attribution_touchpoint.bizible_count_custom_model) AS custom_opp_created,
      SUM(mart_crm_attribution_touchpoint.l_weight) AS linear_opp_created,
      SUM(mart_crm_attribution_touchpoint.first_net_arr) AS first_net_arr,
      SUM(mart_crm_attribution_touchpoint.u_net_arr) AS u_net_arr,
      SUM(mart_crm_attribution_touchpoint.w_net_arr) AS w_net_arr,
      SUM(mart_crm_attribution_touchpoint.full_net_arr) AS full_net_arr,
      SUM(mart_crm_attribution_touchpoint.custom_net_arr) AS custom_net_arr,
      SUM(mart_crm_attribution_touchpoint.linear_net_arr) AS linear_net_arr,
      CASE
        WHEN mart_crm_attribution_touchpoint.is_sao = TRUE
          THEN SUM(mart_crm_attribution_touchpoint.bizible_count_first_touch) 
        ELSE 0 
      END AS first_sao,
      CASE 
        WHEN mart_crm_attribution_touchpoint.is_sao = TRUE
          THEN SUM(mart_crm_attribution_touchpoint.bizible_count_u_shaped) 
        ELSE 0 
      END AS u_shaped_sao,
      CASE 
        WHEN mart_crm_attribution_touchpoint.is_sao = TRUE
          THEN SUM(mart_crm_attribution_touchpoint.bizible_count_w_shaped) 
        ELSE 0 
      END AS w_shaped_sao,
      CASE 
        WHEN mart_crm_attribution_touchpoint.is_sao = TRUE
          THEN SUM(mart_crm_attribution_touchpoint.bizible_count_first_touch) 
        ELSE 0 
      END AS full_shaped_sao,
      CASE 
        WHEN mart_crm_attribution_touchpoint.is_sao = TRUE
          THEN SUM(mart_crm_attribution_touchpoint.bizible_count_custom_model) 
        ELSE 0 
      END AS custom_sao,
      CASE 
        WHEN mart_crm_attribution_touchpoint.is_sao = TRUE
          THEN SUM(mart_crm_attribution_touchpoint.l_weight) 
        ELSE 0 
      END AS linear_sao,
      CASE 
        WHEN mart_crm_attribution_touchpoint.is_sao = TRUE
          THEN SUM(mart_crm_attribution_touchpoint.u_net_arr) 
        ELSE 0 
      END AS pipeline_first_net_arr,
      CASE 
        WHEN mart_crm_attribution_touchpoint.is_sao = TRUE
          THEN SUM(mart_crm_attribution_touchpoint.u_net_arr) 
        ELSE 0 
        END AS pipeline_u_net_arr,
      CASE 
        WHEN mart_crm_attribution_touchpoint.is_sao = TRUE
          THEN SUM(mart_crm_attribution_touchpoint.w_net_arr) 
        ELSE 0 
      END AS pipeline_w_net_arr,
      CASE 
        WHEN mart_crm_attribution_touchpoint.is_sao = TRUE
          THEN SUM(mart_crm_attribution_touchpoint.full_net_arr) 
        ELSE 0 
      END AS pipeline_full_net_arr,
      CASE 
        WHEN mart_crm_attribution_touchpoint.is_sao = TRUE
          THEN SUM(mart_crm_attribution_touchpoint.custom_net_arr) 
        ELSE 0 
      END AS pipeline_custom_net_arr,
      CASE 
        WHEN mart_crm_attribution_touchpoint.is_sao = TRUE
          THEN SUM(mart_crm_attribution_touchpoint.linear_net_arr) 
        ELSE 0 
      END AS pipeline_linear_net_arr,
      CASE 
        WHEN mart_crm_attribution_touchpoint.is_won = TRUE
          THEN SUM(mart_crm_attribution_touchpoint.bizible_count_first_touch) 
        ELSE 0 
      END AS won_first,
      CASE 
        WHEN mart_crm_attribution_touchpoint.is_won = TRUE
          THEN SUM(mart_crm_attribution_touchpoint.bizible_count_u_shaped) 
        ELSE 0 
      END AS won_u,
      CASE 
        WHEN mart_crm_attribution_touchpoint.is_won = TRUE
          THEN SUM(mart_crm_attribution_touchpoint.bizible_count_w_shaped) 
        ELSE 0 
      END AS won_w,
      CASE 
        WHEN mart_crm_attribution_touchpoint.is_won = TRUE
          THEN SUM(mart_crm_attribution_touchpoint.bizible_count_first_touch) 
        ELSE 0 
      END AS won_full,
      CASE 
        WHEN mart_crm_attribution_touchpoint.is_won = TRUE
          THEN SUM(mart_crm_attribution_touchpoint.bizible_count_custom_model) 
        ELSE 0 
      END AS won_custom,
      CASE 
        WHEN mart_crm_attribution_touchpoint.is_won = TRUE
          THEN SUM(mart_crm_attribution_touchpoint.l_weight) 
        ELSE 0 
      END AS won_linear,
      CASE 
        WHEN mart_crm_attribution_touchpoint.is_won = TRUE
          THEN SUM(mart_crm_attribution_touchpoint.first_net_arr) 
        ELSE 0 
      END AS won_first_net_arr,
      CASE 
        WHEN mart_crm_attribution_touchpoint.is_won = TRUE
          THEN SUM(mart_crm_attribution_touchpoint.u_net_arr) 
        ELSE 0 
      END AS won_u_net_arr,
      CASE 
        WHEN mart_crm_attribution_touchpoint.is_won = TRUE
          THEN SUM(mart_crm_attribution_touchpoint.w_net_arr) 
        ELSE 0 
      END AS won_w_net_arr,
      CASE 
        WHEN mart_crm_attribution_touchpoint.is_won = TRUE
          THEN SUM(mart_crm_attribution_touchpoint.full_net_arr) 
        ELSE 0 
      END AS won_full_net_arr,
      CASE 
        WHEN mart_crm_attribution_touchpoint.is_won = TRUE
          THEN SUM(mart_crm_attribution_touchpoint.custom_net_arr) 
        ELSE 0 
      END AS won_custom_net_arr,
      CASE 
        WHEN mart_crm_attribution_touchpoint.is_won = TRUE
          THEN SUM(mart_crm_attribution_touchpoint.linear_net_arr) 
        ELSE 0 
      END AS won_linear_net_arr
    FROM mart_crm_attribution_touchpoint
    {{ dbt_utils.group_by(56) }}
)

{{ dbt_audit(
    cte_ref="unioned",
    created_by="@michellecooper",
    updated_by="@michellecooper",
    created_date="2022-10-05",
    updated_date="2022-12-22"
) }}