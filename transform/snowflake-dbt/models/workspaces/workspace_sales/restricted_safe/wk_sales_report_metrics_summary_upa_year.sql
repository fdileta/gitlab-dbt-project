{{ config(alias='report_metrics_summary_upa_year') }}

WITH report_metrics_summary_account_year AS (

    SELECT *
    --FROM  prod.workspace_sales.report_metrics_summary_account_year
    FROM {{ ref('wk_sales_report_metrics_summary_account_year') }}

 ), consolidated_upa AS (

  SELECT
      report_fiscal_year,
      upa_id,
      upa_name,
      upa_owner_name,
      upa_owner_id,
      upa_country,
      upa_state,
      upa_city,
      upa_zip_code,
      upa_user_geo,
      upa_user_region,
      upa_user_segment,
      upa_user_area,
      upa_user_role,
      upa_industry,
      SUM(CASE WHEN account_forbes_rank IS NOT NULL THEN 1 ELSE 0 END)   AS count_forbes_accounts,
      MIN(account_forbes_rank)      AS forbes_rank,
      MAX(potential_users)          AS potential_users,
      MAX(licenses)                 AS licenses,
      MAX(linkedin_developer)       AS linkedin_developer,
      MAX(zi_developers)            AS zi_developers,
      MAX(zi_revenue)               AS zi_revenue,
      MAX(employees)                AS employees,
      MAX(upa_lam_dev_count)        AS lam_dev_count,

      SUM(has_technical_account_manager_flag) AS count_technical_account_managers,

      -- LAM
    -- MAX(potential_arr_lam)            AS potential_arr_lam,
    -- MAX(potential_carr_this_account)  AS potential_carr_this_account,

      -- atr for current fy
      SUM(fy_sfdc_atr)  AS fy_sfdc_atr,
      -- next fiscal year atr base reported at fy
      SUM(nfy_sfdc_atr) AS nfy_sfdc_atr,

      -- arr by fy
      SUM(arr) AS arr,

      MAX(is_customer_flag)             AS is_customer_flag,
      MAX(is_over_5k_customer_flag)     AS is_over_5k_customer_flag,
      MAX(is_over_10k_customer_flag)    AS is_over_10k_customer_flag,
      MAX(is_over_50k_customer_flag)    AS is_over_50k_customer_flag,
      MAX(is_over_500k_customer_flag)   AS is_over_500k_customer_flag,
      SUM(is_over_5k_customer_flag)     AS count_over_5k_customers,
      SUM(is_over_10k_customer_flag)    AS count_over_10k_customers,
      SUM(is_over_50k_customer_flag)    AS count_over_50k_customers,
      SUM(is_over_500k_customer_flag)   AS count_over_500k_customers,
      SUM(is_prospect_flag)             AS count_of_prospects,
      SUM(is_customer_flag)             AS count_of_customers,

      SUM(arr_channel)                  AS arr_channel,
      SUM(arr_direct)                   AS arr_direct,

      SUM(product_starter_arr)          AS product_starter_arr,
      SUM(product_premium_arr)          AS product_premium_arr,
      SUM(product_ultimate_arr)         AS product_ultimate_arr,
      SUM(delivery_self_managed_arr)    AS delivery_self_managed_arr,
      SUM(delivery_saas_arr)            AS delivery_saas_arr,


      -- rolling last 12 months bokked net arr
      SUM(last_12m_booked_net_arr)                      AS last_12m_booked_net_arr,
      SUM(last_12m_booked_non_web_net_arr)              AS last_12m_booked_non_web_net_arr,
      SUM(last_12m_booked_web_direct_sourced_net_arr)   AS last_12m_booked_web_direct_sourced_net_arr,
      SUM(last_12m_booked_channel_sourced_net_arr)      AS last_12m_booked_channel_sourced_net_arr,
      SUM(last_12m_booked_sdr_sourced_net_arr)          AS last_12m_booked_sdr_sourced_net_arr,
      SUM(last_12m_booked_ae_sourced_net_arr)           AS last_12m_booked_ae_sourced_net_arr,
      SUM(last_12m_booked_churn_contraction_net_arr)    AS last_12m_booked_churn_contraction_net_arr,
      SUM(last_12m_booked_fo_net_arr)                   AS last_12m_booked_fo_net_arr,
      SUM(last_12m_booked_new_connected_net_arr)        AS last_12m_booked_new_connected_net_arr,
      SUM(last_12m_booked_growth_net_arr)               AS last_12m_booked_growth_net_arr,
      SUM(last_12m_booked_deal_count)                   AS last_12m_booked_deal_count,
      SUM(last_12m_booked_direct_net_arr)               AS last_12m_booked_direct_net_arr,
      SUM(last_12m_booked_channel_net_arr)              AS last_12m_booked_channel_net_arr,
      SUM(last_12m_atr)                                 AS last_12m_atr,

      -- fy booked net arr
      SUM(fy_booked_net_arr)                   AS fy_booked_net_arr,
      SUM(fy_booked_web_direct_sourced_net_arr) AS fy_booked_web_direct_sourced_net_arr,
      SUM(fy_booked_channel_sourced_net_arr)   AS fy_booked_channel_sourced_net_arr,
      SUM(fy_booked_sdr_sourced_net_arr)       AS fy_booked_sdr_sourced_net_arr,
      SUM(fy_booked_ae_sourced_net_arr)        AS fy_booked_ae_sourced_net_arr,
      SUM(fy_booked_churn_contraction_net_arr) AS fy_booked_churn_contraction_net_arr,
      SUM(fy_booked_fo_net_arr)                AS fy_booked_fo_net_arr,
      SUM(fy_booked_new_connected_net_arr)     AS fy_booked_new_connected_net_arr,
      SUM(fy_booked_growth_net_arr)            AS fy_booked_growth_net_arr,
      SUM(fy_booked_deal_count)                AS fy_booked_deal_count,
      SUM(fy_booked_direct_net_arr)            AS fy_booked_direct_net_arr,
      SUM(fy_booked_channel_net_arr)           AS fy_booked_channel_net_arr,
      SUM(fy_booked_direct_deal_count)         AS fy_booked_direct_deal_count,
      SUM(fy_booked_channel_deal_count)        AS fy_booked_channel_deal_count,

      -- open pipe forward looking
      SUM(open_pipe)                    AS open_pipe,
      SUM(count_open_deals_pipe)        AS count_open_deals_pipe,
      SUM(customer_has_open_pipe_flag)  AS customer_has_open_pipe_flag,
      SUM(prospect_has_open_pipe_flag)  AS prospect_has_open_pipe_flag,

      -- pipe generation
      SUM(pg_ytd_net_arr) AS pg_ytd_net_arr,
      SUM(pg_ytd_web_direct_sourced_net_arr)    AS pg_ytd_web_direct_sourced_net_arr,
      SUM(pg_ytd_channel_sourced_net_arr)       AS pg_ytd_channel_sourced_net_arr,
      SUM(pg_ytd_sdr_sourced_net_arr)           AS pg_ytd_sdr_sourced_net_arr,
      SUM(pg_ytd_ae_sourced_net_arr)            AS pg_ytd_ae_sourced_net_arr,

      SUM(pg_last_12m_net_arr) AS pg_last_12m_net_arr,
      SUM(pg_last_12m_web_direct_sourced_net_arr)   AS pg_last_12m_web_direct_sourced_net_arr,
      SUM(pg_last_12m_channel_sourced_net_arr)      AS pg_last_12m_channel_sourced_net_arr,
      SUM(pg_last_12m_sdr_sourced_net_arr)          AS pg_last_12m_sdr_sourced_net_arr,
      SUM(pg_last_12m_ae_sourced_net_arr)           AS pg_last_12m_ae_sourced_net_arr,
      
      SUM(last_12m_sao_deal_count)                    AS last_12m_sao_deal_count,
      SUM(last_12m_sao_net_arr)                       AS last_12m_sao_net_arr,
      SUM(last_12m_sao_booked_net_arr)                AS last_12m_sao_booked_net_arr, 
      SUM(fy_sao_deal_count)                          AS fy_sao_deal_count,
      SUM(fy_sao_net_arr)                             AS fy_sao_net_arr,
      SUM(fy_sao_booked_net_arr)                      AS fy_sao_booked_net_arr
      
    FROM report_metrics_summary_account_year
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15


 )

 SELECT *
 FROM consolidated_upa