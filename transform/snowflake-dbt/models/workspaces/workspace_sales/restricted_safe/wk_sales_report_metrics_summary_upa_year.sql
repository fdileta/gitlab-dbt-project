{{ config(alias='report_metrics_summary_upa_year') }}

WITH report_metrics_summary_account_year AS (

    SELECT *
    --FROM  prod.workspace_sales.report_metrics_summary_account_year
    FROM {{ ref('wk_sales_report_metrics_summary_account_year') }}

), upa_virtual_cte AS (

SELECT 
    report_fiscal_year,
    upa_id,
    upa_name,
    upa_user_geo,
    account_id              AS virtual_upa_id,
    account_name            AS virtual_upa_name,
    account_user_segment    AS virtual_upa_segment,
    account_user_geo        AS virtual_upa_geo,
    account_user_region     AS virtual_upa_region,
    account_user_area       AS virtual_upa_area,
    account_country         AS virtual_upa_country,
    account_state           AS virtual_upa_state,
    account_zip_code        AS virtual_upa_zip_code,
    account_industry        AS virtual_upa_industry,
    account_owner_name            AS virtual_upa_owner_name,
    account_owner_title_category  AS virtual_upa_owner_title_category,
    account_owner_id              AS virtual_upa_owner_id,
    account_id,
    account_name,
    arr AS account_arr,
    1 AS level
FROM report_metrics_summary_account_year
WHERE upa_user_geo != account_user_geo
    AND arr > 5000
   -- AND upa_user_geo = 'EMEA'
UNION ALL 
SELECT 
    upa.report_fiscal_year,
    upa.upa_id,
    upa.upa_name,
    upa.upa_user_geo,
    upa.virtual_upa_id,
    upa.virtual_upa_name,
    upa.virtual_upa_segment,
    upa.virtual_upa_geo,
    upa.virtual_upa_region,
    upa.virtual_upa_area,
    upa.virtual_upa_country,
    upa.virtual_upa_state,
    upa.virtual_upa_zip_code,
    upa.virtual_upa_industry,
    upa.virtual_upa_owner_name,
    upa.virtual_upa_owner_title_category,
    upa.virtual_upa_owner_id,
    child.account_id,
    child.account_name,
    child.arr AS account_arr,
    level + 1 AS level
FROM report_metrics_summary_account_year child
INNER JOIN upa_virtual_cte upa
    ON child.parent_id = upa.account_id
    AND child.report_fiscal_year = upa.report_fiscal_year

), max_virtual_upa_depth AS (

    SELECT 
        report_fiscal_year,
        upa_id,
        upa_name,
        virtual_upa_segment,
        virtual_upa_geo,
        virtual_upa_id, 
        virtual_upa_name, 
        MAX(level) AS max_depth
    FROM upa_virtual_cte
    GROUP BY 1,2,3,4,5,6,7
    
), selected_virtual_upa_head AS (

SELECT 
    report_fiscal_year,
    upa_id,
    upa_name,
    virtual_upa_segment,
    virtual_upa_geo,
    virtual_upa_id,
    virtual_upa_name,
    max_depth,
    ROW_NUMBER() OVER (PARTITION BY upa_id, report_fiscal_year ORDER BY max_depth DESC) AS level
FROM max_virtual_upa_depth
QUALIFY level = 1

), final_virtual_upa AS (

    
    SELECT total.*
    FROM upa_virtual_cte total
    INNER JOIN selected_virtual_upa_head selected
        ON total.virtual_upa_id = selected.virtual_upa_id
        AND total.report_fiscal_year = selected.report_fiscal_year


------------------------
    
), consolidated_upa AS (

  SELECT
    acc.report_fiscal_year,
    CASE 
        WHEN new_upa.upa_id IS NOT NULL 
            THEN 'Virtual'
        ELSE 'Real'
    END                                     AS upa_type,
    CASE 
        WHEN new_upa.upa_id IS NOT NULL 
            THEN new_upa.virtual_upa_id 
        ELSE acc.upa_id
    END                                     AS upa_id,
    CASE 
        WHEN new_upa.upa_id IS NOT NULL 
            THEN new_upa.virtual_upa_name
        ELSE acc.upa_name
    END                                     AS upa_name,
    CASE 
        WHEN new_upa.upa_id IS NOT NULL 
            THEN new_upa.virtual_upa_owner_name
        ELSE acc.upa_owner_name
    END                                     AS upa_owner_name,
    CASE 
        WHEN new_upa.upa_id IS NOT NULL 
            THEN new_upa.virtual_upa_owner_id 
        ELSE acc.upa_owner_id
    END                                     AS upa_owner_id,
    CASE 
        WHEN new_upa.upa_id IS NOT NULL 
            THEN new_upa.virtual_upa_owner_title_category
        ELSE acc.upa_owner_title_category
    END                                     AS upa_owner_title_category,
    
    CASE 
        WHEN new_upa.upa_id IS NOT NULL 
            THEN new_upa.virtual_upa_industry 
        ELSE acc.upa_industry
    END                                     AS upa_industry,
    
    -- Account Demographics
    CASE 
        WHEN new_upa.upa_id IS NOT NULL 
            THEN new_upa.virtual_upa_segment
        ELSE acc.upa_ad_segment
    END                                     AS upa_ad_segment,
    CASE 
        WHEN new_upa.upa_id IS NOT NULL 
            THEN new_upa.virtual_upa_geo
        ELSE acc.upa_ad_geo
    END                                     AS upa_ad_geo,
    CASE 
        WHEN new_upa.upa_id IS NOT NULL 
            THEN new_upa.virtual_upa_region 
        ELSE acc.upa_ad_region
    END                                     AS upa_ad_region,
    CASE 
        WHEN new_upa.upa_id IS NOT NULL 
            THEN new_upa.virtual_upa_area
        ELSE acc.upa_ad_area
    END                                     AS upa_ad_area,
    CASE 
        WHEN new_upa.upa_id IS NOT NULL 
            THEN new_upa.virtual_upa_country 
        ELSE acc.upa_ad_country
    END                                     AS upa_ad_country,

    CASE 
        WHEN new_upa.upa_id IS NOT NULL 
            THEN new_upa.virtual_upa_zip_code 
        ELSE acc.upa_ad_zip_code
    END                                     AS upa_ad_zip_code,

    -- Account User Owner fields
    CASE 
        WHEN new_upa.upa_id IS NOT NULL 
            THEN new_upa.virtual_upa_segment
        ELSE acc.upa_user_segment
    END                                     AS upa_user_segment,
    CASE 
        WHEN new_upa.upa_id IS NOT NULL 
            THEN new_upa.virtual_upa_geo
        ELSE acc.upa_user_geo
    END                                     AS upa_user_geo,
    CASE 
        WHEN new_upa.upa_id IS NOT NULL 
            THEN new_upa.virtual_upa_region 
        ELSE acc.upa_user_region
    END                                     AS upa_user_region,
    CASE 
        WHEN new_upa.upa_id IS NOT NULL 
            THEN new_upa.virtual_upa_area
        ELSE acc.upa_user_area
    END                                     AS upa_user_area,
    CASE 
        WHEN new_upa.upa_id IS NOT NULL 
            THEN new_upa.virtual_upa_country 
        ELSE acc.upa_ad_country
    END                                     AS upa_user_country,
    CASE 
        WHEN new_upa.upa_id IS NOT NULL 
            THEN new_upa.virtual_upa_zip_code 
        ELSE acc.upa_ad_zip_code
    END                                     AS upa_user_zip_code,
    
    
    acc.lam_dev_count_bin_rank,
    acc.lam_dev_count_bin_name,
    -- Public Sector
    CASE
        WHEN MAX(acc.is_public_sector_flag) = 1
            THEN 'Public'
        ELSE 'Private'
    END                             AS sector_type,
    MAX(acc.is_public_sector_flag)      AS is_public_sector_flag,
    
    
    SUM(CASE WHEN acc.account_forbes_rank IS NOT NULL THEN 1 ELSE 0 END)   AS count_forbes_accounts,
    MIN(account_forbes_rank)      AS forbes_rank,
    MAX(acc.potential_users)          AS potential_users,
    MAX(acc.licenses)                 AS licenses,
    MAX(acc.linkedin_developer)       AS linkedin_developer,
    MAX(acc.zi_developers)            AS zi_developers,
    MAX(acc.zi_revenue)               AS zi_revenue,
    MAX(acc.employees)                AS employees,
    MAX(acc.upa_lam_dev_count)        AS upa_lam_dev_count,

    SUM(acc.has_technical_account_manager_flag) AS count_technical_account_managers,

    -- atr for current fy
    SUM(acc.fy_atr)  AS fy_atr,
    -- next fiscal year atr base reported at fy
    SUM(acc.nfy_atr) AS nfy_atr,

    -- arr by fy
    SUM(acc.arr) AS arr,

    CASE 
        WHEN  MAX(acc.is_customer_flag) = 1
        THEN 0
    ELSE 1
    END                                   AS is_prospect_flag,
    MAX(acc.is_customer_flag)             AS is_customer_flag,
    MAX(acc.is_over_5k_customer_flag)     AS is_over_5k_customer_flag,
    MAX(acc.is_over_10k_customer_flag)    AS is_over_10k_customer_flag,
    MAX(acc.is_over_50k_customer_flag)    AS is_over_50k_customer_flag,
    MAX(acc.is_over_500k_customer_flag)   AS is_over_500k_customer_flag,
    SUM(acc.is_over_5k_customer_flag)     AS count_over_5k_customers,
    SUM(acc.is_over_10k_customer_flag)    AS count_over_10k_customers,
    SUM(acc.is_over_50k_customer_flag)    AS count_over_50k_customers,
    SUM(acc.is_over_500k_customer_flag)   AS count_over_500k_customers,
    SUM(acc.is_prospect_flag)             AS count_of_prospects,
    SUM(acc.is_customer_flag)             AS count_of_customers,

    SUM(acc.arr_channel)                  AS arr_channel,
    SUM(acc.arr_direct)                   AS arr_direct,

    SUM(acc.product_starter_arr)          AS product_starter_arr,
    SUM(acc.product_premium_arr)          AS product_premium_arr,
    SUM(acc.product_ultimate_arr)         AS product_ultimate_arr,
    SUM(acc.delivery_self_managed_arr)    AS delivery_self_managed_arr,
    SUM(acc.delivery_saas_arr)            AS delivery_saas_arr,


    -- rolling last 12 months bokked net arr
    SUM(last_12m_booked_net_arr)                      AS last_12m_booked_net_arr,
    SUM(acc.last_12m_booked_non_web_net_arr)              AS last_12m_booked_non_web_net_arr,
    SUM(acc.last_12m_booked_web_direct_sourced_net_arr)   AS last_12m_booked_web_direct_sourced_net_arr,
    SUM(acc.last_12m_booked_channel_sourced_net_arr)      AS last_12m_booked_channel_sourced_net_arr,
    SUM(acc.last_12m_booked_sdr_sourced_net_arr)          AS last_12m_booked_sdr_sourced_net_arr,
    SUM(acc.last_12m_booked_ae_sourced_net_arr)           AS last_12m_booked_ae_sourced_net_arr,
    SUM(acc.last_12m_booked_churn_contraction_net_arr)    AS last_12m_booked_churn_contraction_net_arr,
    SUM(acc.last_12m_booked_fo_net_arr)                   AS last_12m_booked_fo_net_arr,
    SUM(acc.last_12m_booked_new_connected_net_arr)        AS last_12m_booked_new_connected_net_arr,
    SUM(acc.last_12m_booked_growth_net_arr)               AS last_12m_booked_growth_net_arr,
    SUM(acc.last_12m_booked_deal_count)                   AS last_12m_booked_deal_count,
    SUM(acc.last_12m_booked_direct_net_arr)               AS last_12m_booked_direct_net_arr,
    SUM(acc.last_12m_booked_channel_net_arr)              AS last_12m_booked_channel_net_arr,
    SUM(acc.last_12m_atr)                                 AS last_12m_atr,

    -- fy booked net arr
    SUM(acc.fy_booked_net_arr)                   AS fy_booked_net_arr,
    SUM(acc.fy_booked_web_direct_sourced_net_arr) AS fy_booked_web_direct_sourced_net_arr,
    SUM(acc.fy_booked_channel_sourced_net_arr)   AS fy_booked_channel_sourced_net_arr,
    SUM(acc.fy_booked_sdr_sourced_net_arr)       AS fy_booked_sdr_sourced_net_arr,
    SUM(acc.fy_booked_ae_sourced_net_arr)        AS fy_booked_ae_sourced_net_arr,
    SUM(acc.fy_booked_churn_contraction_net_arr) AS fy_booked_churn_contraction_net_arr,
    SUM(acc.fy_booked_fo_net_arr)                AS fy_booked_fo_net_arr,
    SUM(acc.fy_booked_new_connected_net_arr)     AS fy_booked_new_connected_net_arr,
    SUM(acc.fy_booked_growth_net_arr)            AS fy_booked_growth_net_arr,
    SUM(acc.fy_booked_deal_count)                AS fy_booked_deal_count,
    SUM(acc.fy_booked_direct_net_arr)            AS fy_booked_direct_net_arr,
    SUM(acc.fy_booked_channel_net_arr)           AS fy_booked_channel_net_arr,
    SUM(acc.fy_booked_direct_deal_count)         AS fy_booked_direct_deal_count,
    SUM(acc.fy_booked_channel_deal_count)        AS fy_booked_channel_deal_count,

    -- open pipe forward looking
    SUM(acc.open_pipe)                    AS open_pipe,
    SUM(acc.count_open_deals_pipe)        AS count_open_deals_pipe,
    SUM(acc.customer_has_open_pipe_flag)  AS customer_has_open_pipe_flag,
    SUM(acc.prospect_has_open_pipe_flag)  AS prospect_has_open_pipe_flag,

    -- pipe generation
    SUM(acc.pg_ytd_net_arr) AS pg_ytd_net_arr,
    SUM(acc.pg_ytd_web_direct_sourced_net_arr)    AS pg_ytd_web_direct_sourced_net_arr,
    SUM(acc.pg_ytd_channel_sourced_net_arr)       AS pg_ytd_channel_sourced_net_arr,
    SUM(acc.pg_ytd_sdr_sourced_net_arr)           AS pg_ytd_sdr_sourced_net_arr,
    SUM(acc.pg_ytd_ae_sourced_net_arr)            AS pg_ytd_ae_sourced_net_arr,

    SUM(acc.pg_last_12m_net_arr) AS pg_last_12m_net_arr,
    SUM(acc.pg_last_12m_web_direct_sourced_net_arr)   AS pg_last_12m_web_direct_sourced_net_arr,
    SUM(acc.pg_last_12m_channel_sourced_net_arr)      AS pg_last_12m_channel_sourced_net_arr,
    SUM(acc.pg_last_12m_sdr_sourced_net_arr)          AS pg_last_12m_sdr_sourced_net_arr,
    SUM(acc.pg_last_12m_ae_sourced_net_arr)           AS pg_last_12m_ae_sourced_net_arr,
    
    SUM(acc.last_12m_sao_deal_count)                    AS last_12m_sao_deal_count,
    SUM(acc.last_12m_sao_net_arr)                       AS last_12m_sao_net_arr,
    SUM(acc.last_12m_sao_booked_net_arr)                AS last_12m_sao_booked_net_arr, 
    SUM(acc.fy_sao_deal_count)                          AS fy_sao_deal_count,
    SUM(acc.fy_sao_net_arr)                             AS fy_sao_net_arr,
    SUM(acc.fy_sao_booked_net_arr)                      AS fy_sao_booked_net_arr
    
  FROM report_metrics_summary_account_year acc
    LEFT JOIN final_virtual_upa new_upa
        ON new_upa.account_id = acc.account_id
        AND new_upa.report_fiscal_year = acc.report_fiscal_year
  GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22

)

SELECT *
FROM consolidated_upa