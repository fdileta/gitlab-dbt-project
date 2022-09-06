{{ simple_cte([
    ('dim_crm_account', 'dim_crm_account'),
    ('fct_crm_account', 'fct_crm_account'),
    ('dim_crm_user', 'dim_crm_user')
    ])

}},

 final AS ( 

    SELECT
       --primary key
      dim_crm_account.dim_crm_account_id,

      --surrogate keys
      fct_crm_account.dim_parent_crm_account_id,
      fct_crm_account.dim_crm_user_id,
      fct_crm_account.merged_to_account_id,
      fct_crm_account.record_type_id,
      fct_crm_account.crm_account_owner_id,
      fct_crm_account.technical_account_manager_id,
      fct_crm_account.master_record_id,
      fct_crm_account.dim_crm_person_primary_contact_id,

      --account people
      dim_crm_account.crm_account_owner,
      dim_crm_account.account_owner,
      dim_crm_account.technical_account_manager,

      --crm account owner attributes
      crm_account_owner.crm_user_geo AS crm_account_owner_geo,
      crm_account_owner.crm_user_region AS crm_account_owner_region,
      crm_account_owner.crm_user_area AS crm_account_owner_area,

      ----ultimate parent crm account info
      dim_crm_account.parent_crm_account_name,
      dim_crm_account.parent_crm_account_billing_country_code,
      dim_crm_account.parent_crm_account_industry_hierarchy,
      dim_crm_account.parent_crm_account_gtm_strategy,
      dim_crm_account.parent_crm_account_focus_account,
      dim_crm_account.parent_crm_account_tsp_max_family_employees,
      dim_crm_account.parent_crm_account_employee_count_band,
      dim_crm_account.parent_crm_account_created_date,
      dim_crm_account.parent_crm_account_zi_technologies,
      dim_crm_account.parent_crm_account_zoom_info_website,
      dim_crm_account.parent_crm_account_zoom_info_company_other_domains,
      dim_crm_account.parent_crm_account_zoom_info_dozisf_zi_id,
      dim_crm_account.parent_crm_account_zoom_info_parent_company_zi_id,
      dim_crm_account.parent_crm_account_zoom_info_parent_company_name,
      dim_crm_account.parent_crm_account_zoom_info_ultimate_parent_company_zi_id,
      dim_crm_account.parent_crm_account_zoom_info_ultimate_parent_company_name,
      dim_crm_account.parent_crm_account_demographics_sales_segment,
      dim_crm_account.parent_crm_account_demographics_geo,
      dim_crm_account.parent_crm_account_demographics_region,
      dim_crm_account.parent_crm_account_demographics_area,
      dim_crm_account.parent_crm_account_demographics_territory,
      dim_crm_account.parent_crm_account_demographics_max_family_employee,
      dim_crm_account.parent_crm_account_demographics_upa_country,
      dim_crm_account.parent_crm_account_demographics_upa_state,
      dim_crm_account.parent_crm_account_demographics_upa_city,
      dim_crm_account.parent_crm_account_demographics_upa_street,
      dim_crm_account.parent_crm_account_demographics_upa_postal_code,

      --descriptive attributes
      dim_crm_account.crm_account_name,
      dim_crm_account.crm_account_demographics_employee_count,
      dim_crm_account.crm_account_gtm_strategy,
      dim_crm_account.crm_account_focus_account,
      dim_crm_account.crm_account_owner_user_segment,
      dim_crm_account.crm_account_billing_country,
      dim_crm_account.crm_account_type,
      dim_crm_account.crm_account_industry,
      dim_crm_account.crm_account_sub_industry,
      dim_crm_account.crm_account_sales_territory,
      dim_crm_account.crm_account_employee_count_band,
      dim_crm_account.partner_vat_tax_id,
      dim_crm_account.account_manager,
      dim_crm_account.business_development_rep,
      dim_crm_account.dedicated_service_engineer,
      dim_crm_account.account_tier,
      dim_crm_account.license_utilization,
      dim_crm_account.support_level,
      dim_crm_account.named_account,
      dim_crm_account.billing_postal_code,
      dim_crm_account.partner_type,
      dim_crm_account.partner_status,
      dim_crm_account.gitlab_customer_success_project,
      dim_crm_account.demandbase_account_list,
      dim_crm_account.demandbase_intent,
      dim_crm_account.demandbase_page_views,
      dim_crm_account.demandbase_score,
      dim_crm_account.demandbase_sessions,
      dim_crm_account.demandbase_trending_offsite_intent,
      dim_crm_account.demandbase_trending_onsite_engagement,
      dim_crm_account.is_locally_managed_account,
      dim_crm_account.is_strategic_account,
      dim_crm_account.partner_track,
      dim_crm_account.partners_partner_type,
      dim_crm_account.gitlab_partner_program,
      dim_crm_account.zoom_info_company_name,
      dim_crm_account.zoom_info_company_revenue,
      dim_crm_account.zoom_info_company_employee_count,
      dim_crm_account.zoom_info_company_industry,
      dim_crm_account.zoom_info_company_city,
      dim_crm_account.zoom_info_company_state_province,
      dim_crm_account.zoom_info_company_country,
      dim_crm_account.abm_tier,
      dim_crm_account.health_number,
      dim_crm_account.health_score_color,
      dim_crm_account.partner_account_iban_number,
      dim_crm_account.federal_account,
      dim_crm_account.gitlab_com_user,
      dim_crm_account.crm_account_zi_technologies,
      dim_crm_account.crm_account_zoom_info_website,
      dim_crm_account.crm_account_zoom_info_company_other_domains,
      dim_crm_account.crm_account_zoom_info_dozisf_zi_id,
      dim_crm_account.crm_account_zoom_info_parent_company_zi_id,
      dim_crm_account.crm_account_zoom_info_parent_company_name,
      dim_crm_account.crm_account_zoom_info_ultimate_parent_company_zi_id,
      dim_crm_account.crm_account_zoom_info_ultimate_parent_company_name,
      dim_crm_account.forbes_2000_rank,

      --degenerative dimensions
      dim_crm_account.is_sdr_target_account,
      dim_crm_account.is_key_account,
      dim_crm_account.is_reseller,
      dim_crm_account.is_jihu_account,
      dim_crm_account.is_first_order_available,
      dim_crm_account.is_excluded_from_zoom_info_enrich,
      dim_crm_account.is_zi_jenkins_present,
      dim_crm_account.is_zi_svn_present,
      dim_crm_account.is_zi_tortoise_svn_present,
      dim_crm_account.is_zi_gcp_present,
      dim_crm_account.is_zi_atlassian_present,
      dim_crm_account.is_zi_github_present,
      dim_crm_account.is_zi_github_enterprise_present,
      dim_crm_account.is_zi_aws_present,
      dim_crm_account.is_zi_kubernetes_present,
      dim_crm_account.is_zi_apache_subversion_present,
      dim_crm_account.is_zi_apache_subversion_svn_present,
      dim_crm_account.is_zi_hashicorp_present,
      dim_crm_account.is_zi_aws_cloud_trail_present,
      dim_crm_account.is_zi_circle_ci_present,
      dim_crm_account.is_zi_bit_bucket_present,

      --dates
      dim_crm_account.crm_account_created_date,
      dim_crm_account.abm_tier_1_date,
      dim_crm_account.abm_tier_2_date,
      dim_crm_account.abm_tier_3_date,
      dim_crm_account.gtm_acceleration_date,
      dim_crm_account.gtm_account_based_date,
      dim_crm_account.gtm_account_centric_date,
      dim_crm_account.partners_signed_contract_date,
      dim_crm_account.technical_account_manager_date,
      dim_crm_account.next_renewal_date,
      dim_crm_account.customer_since_date,

      --measures
      fct_crm_account.count_active_subscription_charges,
      fct_crm_account.count_active_subscriptions,
      fct_crm_account.count_billing_accounts,
      fct_crm_account.count_licensed_users,
      fct_crm_account.count_of_new_business_won_opportunities,
      fct_crm_account.count_open_renewal_opportunities,
      fct_crm_account.count_opportunities,
      fct_crm_account.count_products_purchased,
      fct_crm_account.count_won_opportunities,
      fct_crm_account.count_concurrent_ee_subscriptions,
      fct_crm_account.count_ce_instances,
      fct_crm_account.count_active_ce_users,
      fct_crm_account.count_open_opportunities,
      fct_crm_account.count_using_ce,
      fct_crm_account.potential_arr_lam,
      fct_crm_account.parent_crm_account_lam,
      fct_crm_account.parent_crm_account_lam_dev_count,
      fct_crm_account.carr_this_account,
      fct_crm_account.carr_account_family,
      fct_crm_account.potential_users,
      fct_crm_account.number_of_licenses_this_account,
      fct_crm_account.crm_account_zoom_info_number_of_developers,
      fct_crm_account.decision_maker_count_linkedin,

      --metadata
      fct_crm_account.created_by_id,
      dim_crm_account.created_by_name,
      fct_crm_account.last_modified_by_id,
      dim_crm_account.last_modified_by_name,
      fct_crm_account.last_modified_date_id,
      dim_crm_account.last_modified_date,
      fct_crm_account.last_activity_date_id,
      dim_crm_account.last_activity_date,
      dim_crm_account.is_deleted

      FROM dim_crm_account
      INNER JOIN fct_crm_account
        ON dim_crm_account.dim_crm_account_id = fct_crm_account.dim_crm_account_id
      LEFT JOIN dim_crm_user crm_account_owner
        ON fct_crm_account.crm_account_owner_id = crm_account_owner.dim_crm_user_id

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@michellecooper",
    updated_by="@j_kim",
    created_date="2022-08-10",
    updated_date="2022-08-31"
) }}
