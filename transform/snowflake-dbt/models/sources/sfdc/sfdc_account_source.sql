{{ config(
    tags=["mnpi"]
) }}

/*

  ATTENTION: When a field is added to this live model, add it to the SFDC_ACCOUNT_SNAPSHOTS_SOURCE model to keep the live and snapshot models in alignment.

*/

WITH source AS (

  SELECT *
  FROM {{ source('salesforce', 'account') }}

),

renamed AS (

  SELECT
    id AS account_id,
    name AS account_name,

    -- keys
    account_id_18__c AS account_id_18,
    masterrecordid AS master_record_id,
    ownerid AS owner_id,
    parentid AS parent_id,
    primary_contact_id__c AS primary_contact_id,
    recordtypeid AS record_type_id,
    ultimate_parent_account_id__c AS ultimate_parent_id,
    partner_vat_tax_id__c AS partner_vat_tax_id,


    -- key people GL side
    gitlab_com_user__c AS gitlab_com_user,
    account_manager__c AS account_manager,
    account_owner_calc__c AS account_owner,
    account_owner_team__c AS account_owner_team,
    business_development_rep__c AS business_development_rep,
    dedicated_service_engineer__c AS dedicated_service_engineer,
    sdr_assigned__c AS sales_development_rep,
    -- solutions_architect__c                     AS solutions_architect,
    technical_account_manager_lu__c AS technical_account_manager_id,

    -- info
    "{{ this.database }}".{{ target.schema }}.ID15TO18(SUBSTRING(REGEXP_REPLACE(
      ultimate_parent_account__c, '_HL_ENCODED_/|<a\\s+href="/', ''
      ), 0, 15)) AS ultimate_parent_account_id,
    type AS account_type,
    dfox_industry__c AS df_industry,
    industry AS industry,
    sub_industry__c AS sub_industry,
    parent_lam_industry_acct_heirarchy__c AS parent_account_industry_hierarchy,
    account_tier__c AS account_tier,
    customer_since__c::DATE AS customer_since_date,
    carr_this_account__c AS carr_this_account,
    carr_acct_family__c AS carr_account_family,
    next_renewal_date__c AS next_renewal_date,
    license_utilization__c AS license_utilization,
    support_level__c AS support_level,
    named_account__c AS named_account,
    billingcountry AS billing_country,
    billingcountrycode AS billing_country_code,
    billingpostalcode AS billing_postal_code,
    sdr_target_account__c::BOOLEAN AS is_sdr_target_account,
    lam_tier__c AS lam,
    lam_dev_count__c AS lam_dev_count,
    potential_arr_lam__c AS potential_arr_lam,
    jihu_account__c::BOOLEAN AS is_jihu_account,
    partners_signed_contract_date__c AS partners_signed_contract_date,
    partner_account_iban_number__c AS partner_account_iban_number,
    partners_partner_type__c AS partner_type,
    partners_partner_status__c AS partner_status,
    fy22_new_logo_target_list__c::BOOLEAN AS fy22_new_logo_target_list,
    first_order_available__c::BOOLEAN AS is_first_order_available,
    REPLACE(
      zi_technologies__c,
      'The technologies that are used and not used at this account, according to ZoomInfo, after completing a scan are:', -- noqa:L016
      ''
    ) AS zi_technologies,
    technical_account_manager_date__c::DATE AS technical_account_manager_date,
    gitlab_customer_success_project__c::VARCHAR AS gitlab_customer_success_project,
    forbes_2000_rank__c AS forbes_2000_rank,
    potential_users__c AS potential_users,
    number_of_licenses_this_account__c AS number_of_licenses_this_account,
    decision_maker_count_linkedin__c AS decision_maker_count_linkedin,

    -- territory success planning fields
    atam_approved_next_owner__c AS tsp_approved_next_owner,
    atam_next_owner_role__c AS tsp_next_owner_role,
    atam_account_employees__c AS tsp_account_employees,
    jb_max_family_employees__c AS tsp_max_family_employees,
    TRIM(SPLIT_PART(atam_region__c, '-', 1)) AS tsp_region,
    TRIM(SPLIT_PART(atam_sub_region__c, '-', 1)) AS tsp_sub_region,
    TRIM(SPLIT_PART(atam_area__c, '-', 1)) AS tsp_area,
    atam_territory__c AS tsp_territory,
    atam_address_country__c AS tsp_address_country,
    atam_address_state__c AS tsp_address_state,
    atam_address_city__c AS tsp_address_city,
    atam_address_street__c AS tsp_address_street,
    atam_address_postal_code__c AS tsp_address_postal_code,

    -- account demographics fields
    account_demographics_sales_segment__c AS account_demographics_sales_segment,
    account_demographics_geo__c AS account_demographics_geo,
    account_demographics_region__c AS account_demographics_region,
    account_demographics_area__c AS account_demographics_area,
    account_demographics_territory__c AS account_demographics_territory,
    account_demographics_employee_count__c AS account_demographics_employee_count,
    account_demographic_max_family_employees__c AS account_demographics_max_family_employee,
    account_demographics_upa_country__c AS account_demographics_upa_country,
    account_demographics_upa_state__c AS account_demographics_upa_state,
    account_demographics_upa_city__c AS account_demographics_upa_city,
    account_demographics_upa_street__c AS account_demographics_upa_street,
    account_demographics_upa_postal_code__c AS account_demographics_upa_postal_code,

    -- present state info
    health__c AS health_score,
    gs_health_score__c AS health_number,
    gs_health_score_color__c AS health_score_color,

    -- opportunity metrics
    count_of_active_subscription_charges__c AS count_active_subscription_charges,
    count_of_active_subscriptions__c AS count_active_subscriptions,
    count_of_billing_accounts__c AS count_billing_accounts,
    license_user_count__c AS count_licensed_users,
    count_of_new_business_won_opps__c AS count_of_new_business_won_opportunities,
    count_of_open_renewal_opportunities__c AS count_open_renewal_opportunities,
    count_of_opportunities__c AS count_opportunities,
    count_of_products_purchased__c AS count_products_purchased,
    count_of_won_opportunities__c AS count_won_opportunities,
    concurrent_ee_subscriptions__c AS count_concurrent_ee_subscriptions,
    ce_instances__c AS count_ce_instances,
    active_ce_users__c AS count_active_ce_users,
    number_of_open_opportunities__c AS count_open_opportunities,
    using_ce__c AS count_using_ce,

    --account based marketing fields
    abm_tier__c AS abm_tier,
    gtm_strategy__c AS gtm_strategy,
    gtm_acceleration_date__c AS gtm_acceleration_date,
    gtm_account_based_date__c AS gtm_account_based_date,
    gtm_account_centric_date__c AS gtm_account_centric_date,
    abm_tier_1_date__c AS abm_tier_1_date,
    abm_tier_2_date__c AS abm_tier_2_date,
    abm_tier_3_date__c AS abm_tier_3_date,

    --demandbase fields
    account_list__c AS demandbase_account_list,
    intent__c AS demandbase_intent,
    page_views__c AS demandbase_page_views,
    score__c AS demandbase_score,
    sessions__c AS demandbase_sessions,
    trending_offsite_intent__c AS demandbase_trending_offsite_intent,
    trending_onsite_engagement__c AS demandbase_trending_onsite_engagement,

    -- sales segment fields
    ultimate_parent_sales_segment_employees__c AS ultimate_parent_sales_segment,
    sales_segmentation_new__c AS division_sales_segment,
    jb_test_sales_segment__c AS tsp_max_hierarchy_sales_segment,
    account_owner_user_segment__c AS account_owner_user_segment,
    -- ************************************
    -- sales segmentation deprecated fields - 2020-09-03
    -- left temporary for the sake of MVC and avoid breaking SiSense existing charts
    jb_test_sales_segment__c AS tsp_test_sales_segment,
    ultimate_parent_sales_segment_employees__c AS sales_segment,
    sales_segmentation_new__c AS account_segment,

      -- ************************************
      -- NF: 2020-12-17
      -- these two fields are used to identify accounts owned 
      -- by reps within hierarchies that they do not fully own
      -- or even within different regions

    locally_managed__c AS is_locally_managed_account,
    strategic__c AS is_strategic_account,

    -- ************************************
    -- New SFDC Account Fields for FY22 Planning
    next_fy_account_owner_temp__c AS next_fy_account_owner_temp,
    next_fy_planning_notes_temp__c AS next_fy_planning_notes_temp,
    --*************************************
    -- Partner Account fields
    partner_track__c AS partner_track,
    partners_partner_type__c AS partners_partner_type,
    gitlab_partner_programs__c AS gitlab_partner_program,

    --*************************************
    -- Zoom Info Fields
    zi_account_name__c AS zoom_info_company_name,
    zi_revenue__c AS zoom_info_company_revenue,
    zi_employees__c AS zoom_info_company_employee_count,
    zi_industry__c AS zoom_info_company_industry,
    zi_city__c AS zoom_info_company_city,
    zi_state_province__c AS zoom_info_company_state_province,
    zi_country__c AS zoom_info_company_country,
    exclude_from_zoominfo_enrich__c AS is_excluded_from_zoom_info_enrich,
    zi_website__c AS zoom_info_website,
    zi_company_other_domains__c AS zoom_info_company_other_domains,
    dozisf__zoominfo_id__c AS zoom_info_dozisf_zi_id,
    zi_parent_company_zoominfo_id__c AS zoom_info_parent_company_zi_id,
    zi_parent_company_name__c AS zoom_info_parent_company_name,
    zi_ultimate_parent_company_zoominfo_id__c AS zoom_info_ultimate_parent_company_zi_id,
    zi_ultimate_parent_company_name__c AS zoom_info_ultimate_parent_company_name,
    zi_number_of_developers__c AS zoom_info_number_of_developers,

    -- NF: Added on 20220427 to support EMEA reporting
    key_account__c                     AS is_key_account,

    -- metadata
    createdbyid AS created_by_id,
    createddate AS created_date,
    isdeleted AS is_deleted,
    lastmodifiedbyid AS last_modified_by_id,
    lastmodifieddate AS last_modified_date,
    lastactivitydate AS last_activity_date,
    CONVERT_TIMEZONE(
      'America/Los_Angeles', CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP())
    ) AS _last_dbt_run,
    systemmodstamp

  FROM source
)

SELECT *
FROM renamed
