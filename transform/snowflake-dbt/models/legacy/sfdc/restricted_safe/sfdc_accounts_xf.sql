WITH sfdc_account AS (

    SELECT * 
    FROM {{ ref('sfdc_account') }}

), sfdc_users AS (

    SELECT *
    FROM {{ ref('sfdc_users') }}

), sfdc_record_type AS (

    SELECT *
    FROM {{ ref('sfdc_record_type') }}     

), sfdc_account_deal_size_segmentation AS (

    SELECT *
    FROM {{ ref('sfdc_account_deal_size_segmentation') }}

), parent_account AS (

    SELECT *
    FROM {{ ref('sfdc_account') }}

), joined AS (

    SELECT
      sfdc_account.account_id,
      sfdc_account.account_name,
      sfdc_account.account_id_18,
      sfdc_account.master_record_id,
      sfdc_account.owner_id,
      sfdc_account.parent_id,
      sfdc_account.primary_contact_id,
      sfdc_account.record_type_id,
      sfdc_account.ultimate_parent_id,
      sfdc_account.partner_vat_tax_id,
      sfdc_account.federal_account,
      sfdc_account.gitlab_com_user,
      sfdc_account.account_manager,
      sfdc_account.account_owner,
      sfdc_account.account_owner_team,
      sfdc_account.business_development_rep,
      sfdc_account.dedicated_service_engineer,
      sfdc_account.sales_development_rep,
      sfdc_account.technical_account_manager_id,
      sfdc_account.ultimate_parent_account_id,
      sfdc_account.account_type,
      sfdc_account.df_industry,
      sfdc_account.industry,
      sfdc_account.sub_industry,
      sfdc_account.parent_account_industry_hierarchy,
      sfdc_account.account_tier,
      sfdc_account.customer_since_date,
      sfdc_account.carr_this_account,
      sfdc_account.carr_account_family,
      sfdc_account.next_renewal_date,
      sfdc_account.license_utilization,
      sfdc_account.support_level,
      sfdc_account.named_account,
      sfdc_account.billing_country,
      sfdc_account.billing_country_code,
      sfdc_account.billing_postal_code,
      sfdc_account.is_sdr_target_account,
      sfdc_account.lam,
      sfdc_account.lam_dev_count,
      sfdc_account.potential_arr_lam,
      sfdc_account.is_jihu_account,
      sfdc_account.partners_signed_contract_date,
      sfdc_account.partner_account_iban_number,
      sfdc_account.partner_type,
      sfdc_account.partner_status,
      sfdc_account.fy22_new_logo_target_list,
      sfdc_account.is_first_order_available,
      sfdc_account.zi_technologies,
      sfdc_account.technical_account_manager_date,
      sfdc_account.gitlab_customer_success_project,
      sfdc_account.forbes_2000_rank,
      sfdc_account.potential_users,
      sfdc_account.number_of_licenses_this_account,
      sfdc_account.decision_maker_count_linkedin,
      sfdc_account.number_of_employees,
      sfdc_account.account_phone,
      sfdc_account.zoominfo_account_phone,
      sfdc_account.tsp_approved_next_owner,
      sfdc_account.tsp_next_owner_role,
      sfdc_account.tsp_account_employees,
      sfdc_account.tsp_max_family_employees,
      sfdc_account.tsp_region,
      sfdc_account.tsp_sub_region,
      sfdc_account.tsp_area,
      sfdc_account.tsp_territory,
      sfdc_account.tsp_address_country,
      sfdc_account.tsp_address_state,
      sfdc_account.tsp_address_city,
      sfdc_account.tsp_address_street,
      sfdc_account.tsp_address_postal_code,
      sfdc_account.account_demographics_sales_segment,
      sfdc_account.account_demographics_geo,
      sfdc_account.account_demographics_region,
      sfdc_account.account_demographics_area,
      sfdc_account.account_demographics_territory,
      sfdc_account.account_demographics_employee_count,
      sfdc_account.account_demographics_max_family_employee,
      sfdc_account.account_demographics_upa_country,
      sfdc_account.account_demographics_upa_state,
      sfdc_account.account_demographics_upa_city,
      sfdc_account.account_demographics_upa_street,
      sfdc_account.account_demographics_upa_postal_code,
      sfdc_account.health_score,
      sfdc_account.health_number,
      sfdc_account.health_score_color,
      sfdc_account.count_active_subscription_charges,
      sfdc_account.count_active_subscriptions,
      sfdc_account.count_billing_accounts,
      sfdc_account.count_licensed_users,
      sfdc_account.count_of_new_business_won_opportunities,
      sfdc_account.count_open_renewal_opportunities,
      sfdc_account.count_opportunities,
      sfdc_account.count_products_purchased,
      sfdc_account.count_won_opportunities,
      sfdc_account.count_concurrent_ee_subscriptions,
      sfdc_account.count_ce_instances,
      sfdc_account.count_active_ce_users,
      sfdc_account.count_open_opportunities,
      sfdc_account.count_using_ce,
      sfdc_account.abm_tier,
      sfdc_account.gtm_strategy,
      sfdc_account.gtm_acceleration_date,
      sfdc_account.gtm_account_based_date,
      sfdc_account.gtm_account_centric_date,
      sfdc_account.abm_tier_1_date,
      sfdc_account.abm_tier_2_date,
      sfdc_account.abm_tier_3_date,
      sfdc_account.demandbase_account_list,
      sfdc_account.demandbase_intent,
      sfdc_account.demandbase_page_views,
      sfdc_account.demandbase_score,
      sfdc_account.demandbase_sessions,
      sfdc_account.demandbase_trending_offsite_intent,
      sfdc_account.demandbase_trending_onsite_engagement,
      sfdc_account.ultimate_parent_sales_segment,
      sfdc_account.division_sales_segment,
      sfdc_account.tsp_max_hierarchy_sales_segment,
      sfdc_account.tsp_test_sales_segment,
      sfdc_account.sales_segment,
      sfdc_account.account_segment,
      sfdc_account.is_locally_managed_account,
      sfdc_account.is_strategic_account,
      sfdc_account.next_fy_account_owner_temp,
      sfdc_account.next_fy_planning_notes_temp,
      sfdc_account.partner_track,
      sfdc_account.partners_partner_type,
      sfdc_account.gitlab_partner_program,
      sfdc_account.zoom_info_company_name,
      sfdc_account.zoom_info_company_revenue,
      sfdc_account.zoom_info_company_employee_count,
      sfdc_account.zoom_info_company_industry,
      sfdc_account.zoom_info_company_city,
      sfdc_account.zoom_info_company_state_province,
      sfdc_account.zoom_info_company_country,
      sfdc_account.is_excluded_from_zoom_info_enrich,
      sfdc_account.zoom_info_website,
      sfdc_account.zoom_info_company_other_domains,
      sfdc_account.zoom_info_dozisf_zi_id,
      sfdc_account.zoom_info_parent_company_zi_id,
      sfdc_account.zoom_info_parent_company_name,
      sfdc_account.zoom_info_ultimate_parent_company_zi_id,
      sfdc_account.zoom_info_ultimate_parent_company_name,
      sfdc_account.zoom_info_number_of_developers,
      sfdc_account.is_key_account,
      sfdc_account.created_by_id,
      sfdc_account.created_date,
      sfdc_account.is_deleted,
      sfdc_account.last_modified_by_id,
      sfdc_account.last_modified_date,
      sfdc_account.last_activity_date,
      sfdc_account.systemmodstamp,
      tam_user.name                                                                   AS technical_account_manager,
      parent_account.account_name                                                     AS ultimate_parent_account_name, 

      -- ************************************
      -- sales segmentation deprecated fields - 2020-09-03
      -- left temporary for the sake of MVC and avoid breaking SiSense existing charts
      -- issue: https://gitlab.com/gitlab-data/analytics/-/issues/5709
      sfdc_account.ultimate_parent_sales_segment                                        AS ultimate_parent_account_segment,
      -- ************************************
      
      sfdc_record_type.record_type_name,
      sfdc_record_type.business_process_id,
      sfdc_record_type.record_type_label,
      sfdc_record_type.record_type_description,
      sfdc_record_type.record_type_modifying_object_type,
      sfdc_account_deal_size_segmentation.deal_size,
      CASE 
        WHEN sfdc_account.ultimate_parent_sales_segment IN ('Large', 'Strategic')
          OR sfdc_account.division_sales_segment IN ('Large', 'Strategic') 
          THEN TRUE
        ELSE FALSE 
      END                                                                               AS is_large_and_up,


      -- NF 20210829 Zoom info technology flags
      CASE
        WHEN CONTAINS (sfdc_account.zi_technologies,'ARE_USED: Jenkins') 
          THEN 1 
        ELSE 0
      END                                 AS zi_jenkins_presence_flag,
      CASE
        WHEN CONTAINS (sfdc_account.zi_technologies, 'ARE_USED: SVN') 
          THEN 1 
        ELSE 0
      END                                 AS zi_svn_presence_flag,
      CASE
        WHEN CONTAINS (sfdc_account.zi_technologies, 'ARE_USED: Tortoise SVN') 
          THEN 1 
        ELSE 0
      END                                 AS zi_tortoise_svn_presence_flag,
      CASE
        WHEN CONTAINS (sfdc_account.zi_technologies, 'ARE_USED: Google Cloud Platform') 
          THEN 1 
        ELSE 0
      END                                 AS zi_gcp_presence_flag,
      CASE
        WHEN CONTAINS (sfdc_account.zi_technologies, 'ARE_USED: Atlassian') 
          THEN 1 
        ELSE 0
      END                                 AS zi_atlassian_presence_flag,
      CASE
        WHEN CONTAINS (sfdc_account.zi_technologies, 'ARE_USED: GitHub') 
          THEN 1 
        ELSE 0
      END                                 AS zi_github_presence_flag,
      CASE
        WHEN CONTAINS (sfdc_account.zi_technologies, 'ARE_USED: GitHub Enterprise') 
          THEN 1 
        ELSE 0
      END                                 AS zi_github_enterprise_presence_flag,
      CASE
        WHEN CONTAINS (sfdc_account.zi_technologies, 'ARE_USED: AWS') 
          THEN 1 
        ELSE 0
      END                                 AS zi_aws_presence_flag,
      CASE
        WHEN CONTAINS (sfdc_account.zi_technologies, 'ARE_USED: Kubernetes') 
          THEN 1 
        ELSE 0
      END                                 AS zi_kubernetes_presence_flag,
      CASE
        WHEN CONTAINS (sfdc_account.zi_technologies, 'ARE_USED: Apache Subversion') 
          THEN 1 
        ELSE 0
      END                                 AS zi_apache_subversion_presence_flag,
      CASE
        WHEN CONTAINS (sfdc_account.zi_technologies, 'ARE_USED: Apache Subversion (SVN)') 
          THEN 1 
        ELSE 0
      END                                 AS zi_apache_subversion_svn_presence_flag,
      CASE
        WHEN CONTAINS (sfdc_account.zi_technologies, 'ARE_USED: Hashicorp') 
          THEN 1 
        ELSE 0
      END                                 AS zi_hashicorp_presence_flag,
      CASE
        WHEN CONTAINS (sfdc_account.zi_technologies, 'ARE_USED: Amazon AWS CloudTrail') 
          THEN 1 
        ELSE 0
      END                                 AS zi_aws_cloud_trail_presence_flag,
      CASE
        WHEN CONTAINS (sfdc_account.zi_technologies, 'ARE_USED: CircleCI') 
          THEN 1 
        ELSE 0
      END                                 AS zi_circle_ci_presence_flag,
      CASE
        WHEN CONTAINS (sfdc_account.zi_technologies, 'ARE_USED: BitBucket') 
          THEN 1 
        ELSE 0
      END                                 AS zi_bit_bucket_presence_flag,

    -- NF 2022-01-28 Added extra account owner demographics fields
    --account_owner.user_segment            AS account_owner_user_segment, -- coming directly from source table
    -- JK: 2022-11-16 changing the source back to the user object to align with EDM 
    account_owner.user_segment            AS account_owner_user_segment,
    
    account_owner.user_geo                AS account_owner_user_geo, 
    account_owner.user_region             AS account_owner_user_region,
    account_owner.user_area               AS account_owner_user_area,

    parent_account.account_demographics_sales_segment    AS upa_demographics_segment,
    parent_account.account_demographics_geo              AS upa_demographics_geo,
    parent_account.account_demographics_region           AS upa_demographics_region,
    parent_account.account_demographics_area             AS upa_demographics_area,
    parent_account.account_demographics_territory        AS upa_demographics_territory

    FROM sfdc_account
    LEFT JOIN parent_account
      ON sfdc_account.ultimate_parent_account_id = parent_account.account_id
    LEFT JOIN sfdc_users tam_user
      ON sfdc_account.technical_account_manager_id = tam_user.user_id
    LEFT JOIN sfdc_users account_owner
      ON sfdc_account.owner_id = account_owner.user_id
    LEFT JOIN sfdc_record_type
      ON sfdc_account.record_type_id = sfdc_record_type.record_type_id
    LEFT JOIN sfdc_account_deal_size_segmentation
      ON sfdc_account.account_id = sfdc_account_deal_size_segmentation.account_id

)

SELECT *
FROM joined
