{%- macro sfdc_account_fields(model_type) %}

WITH map_merged_crm_account AS (

    SELECT *
    FROM {{ ref('map_merged_crm_account') }}

), prep_crm_person AS (

    SELECT *
    FROM {{ ref('prep_crm_person') }}

{%- if model_type == 'live' %}

{%- elif model_type == 'snapshot' %}
), snapshot_dates AS (

    SELECT *
    FROM {{ ref('dim_date') }}
    WHERE date_actual >= '2020-03-01' and date_actual <= CURRENT_DATE
    {% if is_incremental() %}

   -- this filter will only be applied on an incremental run
   AND date_id > (SELECT max(snapshot_id) FROM {{ this }})

{% endif %}

), lam_corrections AS (

    SELECT
      snapshot_dates.date_id                                                                                   AS snapshot_id,
      dim_parent_crm_account_id                                                                                AS dim_parent_crm_account_id,
      dev_count                                                                                                AS dev_count,
      estimated_capped_lam                                                                                     AS estimated_capped_lam,
      dim_parent_crm_account_sales_segment                                                                     AS dim_parent_crm_account_sales_segment
      
    FROM {{ ref('driveload_lam_corrections_source') }}
    INNER JOIN snapshot_dates
        ON snapshot_dates.date_actual >= valid_from
          AND snapshot_dates.date_actual < COALESCE(valid_to, '9999-12-31'::TIMESTAMP)

{%- endif %}

), sfdc_account AS (

    SELECT
    {%- if model_type == 'live' %}
        *
    {%- elif model_type == 'snapshot' %}
        {{ dbt_utils.surrogate_key(['sfdc_account_snapshots_source.account_id','snapshot_dates.date_id'])}}    AS crm_account_snapshot_id,
        snapshot_dates.date_id                                                                                 AS snapshot_id,
        snapshot_dates.date_actual                                                                             AS snapshot_date,
        sfdc_account_snapshots_source.*
     {%- endif %}
    FROM
    {%- if model_type == 'live' %}
        {{ ref('sfdc_account_source') }}
    {%- elif model_type == 'snapshot' %}
        {{ ref('sfdc_account_snapshots_source') }}
         INNER JOIN snapshot_dates
           ON snapshot_dates.date_actual >= sfdc_account_snapshots_source.dbt_valid_from
           AND snapshot_dates.date_actual < COALESCE(sfdc_account_snapshots_source.dbt_valid_to, '9999-12-31'::TIMESTAMP)
    {%- endif %}
    WHERE account_id IS NOT NULL

), sfdc_users AS (

    SELECT
      {%- if model_type == 'live' %}
        *
      {%- elif model_type == 'snapshot' %}
      {{ dbt_utils.surrogate_key(['sfdc_user_snapshots_source.user_id','snapshot_dates.date_id'])}}            AS crm_user_snapshot_id,
      snapshot_dates.date_id                                                                                   AS snapshot_id,
      sfdc_user_snapshots_source.*
      {%- endif %}
    FROM
      {%- if model_type == 'live' %}
      {{ ref('sfdc_users_source') }}
      {%- elif model_type == 'snapshot' %}
      {{ ref('sfdc_user_snapshots_source') }}
       INNER JOIN snapshot_dates
         ON snapshot_dates.date_actual >= sfdc_user_snapshots_source.dbt_valid_from
         AND snapshot_dates.date_actual < COALESCE(sfdc_user_snapshots_source.dbt_valid_to, '9999-12-31'::TIMESTAMP)
    {%- endif %}

), sfdc_record_type AS (

    SELECT *
    FROM {{ ref('sfdc_record_type') }}

), pte_scores AS (

    SELECT 
      crm_account_id                                                                                           AS account_id,
      score                                                                                                    AS score,
      decile                                                                                                   AS decile,
      score_group                                                                                              AS score_group,
      MIN(score_date)                                                                                          AS valid_from,
      COALESCE(LEAD(valid_from) OVER (PARTITION BY crm_account_id ORDER BY valid_from), CURRENT_DATE())        AS valid_to,
      CASE 
        WHEN ROW_NUMBER() OVER (PARTITION BY crm_account_id ORDER BY valid_from DESC) = 1 
          THEN TRUE
        ELSE FALSE
      END                                                                                                      AS is_current
    FROM {{ ref('pte_scores_source') }}    
    {{ dbt_utils.group_by(n=4)}}
    ORDER BY valid_from, valid_to


), ptc_scores AS (

    SELECT 
      crm_account_id                                                                                           AS account_id,
      score                                                                                                    AS score,
      decile                                                                                                   AS decile,
      score_group                                                                                              AS score_group,
      MIN(score_date)                                                                                          AS valid_from,
      COALESCE(LEAD(valid_from) OVER (PARTITION BY crm_account_id ORDER BY valid_from), CURRENT_DATE())        AS valid_to,
      CASE 
        WHEN ROW_NUMBER() OVER (PARTITION BY crm_account_id ORDER BY valid_from DESC) = 1 
          THEN TRUE
        ELSE FALSE
      END                                                                                                      AS is_current
    FROM {{ ref('ptc_scores_source') }}    
    {{ dbt_utils.group_by(n=4)}}
    ORDER BY valid_from, valid_to

), final AS (

    SELECT
      --crm account information
      {%- if model_type == 'live' %}

      {%- elif model_type == 'snapshot' %}
      sfdc_account.crm_account_snapshot_id,
      sfdc_account.snapshot_id,
      sfdc_account.snapshot_date,
      {%- endif %}
      --primary key
      sfdc_account.account_id                                                                                  AS dim_crm_account_id,

      --surrogate keys
      sfdc_account.ultimate_parent_account_id                                                                  AS dim_parent_crm_account_id,
      sfdc_account.owner_id                                                                                    AS dim_crm_user_id,
      map_merged_crm_account.dim_crm_account_id                                                                AS merged_to_account_id,
      sfdc_account.record_type_id                                                                              AS record_type_id,
      account_owner.user_id                                                                                    AS crm_account_owner_id,
      proposed_account_owner.user_id                                                                           AS proposed_crm_account_owner_id,
      technical_account_manager.user_id                                                                        AS technical_account_manager_id,
      sfdc_account.master_record_id,
      prep_crm_person.dim_crm_person_id                                                                        AS dim_crm_person_primary_contact_id,

      --account people
      account_owner.name                                                                                       AS account_owner,
      proposed_account_owner.name                                                                              AS proposed_crm_account_owner,
      technical_account_manager.name                                                                           AS technical_account_manager,

      --descriptive attributes
      sfdc_account.account_name                                                                                AS crm_account_name,
      sfdc_account.account_demographics_sales_segment                                                          AS parent_crm_account_demographics_sales_segment,
      sfdc_account.account_demographics_geo                                                                    AS parent_crm_account_demographics_geo,
      sfdc_account.account_demographics_region                                                                 AS parent_crm_account_demographics_region,
      sfdc_account.account_demographics_area                                                                   AS parent_crm_account_demographics_area,
      sfdc_account.account_demographics_territory                                                              AS parent_crm_account_demographics_territory,
      sfdc_account.account_demographics_max_family_employee                                                    AS parent_crm_account_demographics_max_family_employee,
      sfdc_account.account_demographics_upa_country                                                            AS parent_crm_account_demographics_upa_country,
      sfdc_account.account_demographics_upa_state                                                              AS parent_crm_account_demographics_upa_state,
      sfdc_account.account_demographics_upa_city                                                               AS parent_crm_account_demographics_upa_city,
      sfdc_account.account_demographics_upa_street                                                             AS parent_crm_account_demographics_upa_street,
      sfdc_account.account_demographics_upa_postal_code                                                        AS parent_crm_account_demographics_upa_postal_code,
      sfdc_account.account_demographics_employee_count                                                         AS crm_account_demographics_employee_count,
      sfdc_account.parent_account_industry_hierarchy                                                           AS parent_crm_account_industry,
      sfdc_account.gtm_strategy                                                                                AS crm_account_gtm_strategy,
      CASE
        WHEN LOWER(sfdc_account.gtm_strategy) IN ('account centric', 'account based - net new', 'account based - expand') THEN 'Focus Account'
        ELSE 'Non - Focus Account'
      END                                                                                                      AS crm_account_focus_account,
      CASE
         WHEN sfdc_account.account_demographics_max_family_employee > 2000 THEN 'Employees > 2K'
         WHEN sfdc_account.account_demographics_max_family_employee <= 2000 AND sfdc_account.account_demographics_max_family_employee > 1500 THEN 'Employees > 1.5K'
         WHEN sfdc_account.account_demographics_max_family_employee <= 1500 AND sfdc_account.account_demographics_max_family_employee > 1000  THEN 'Employees > 1K'
         ELSE 'Employees < 1K'
      END                                                                                                      AS crm_account_employee_count_band,
      sfdc_account.account_owner_user_segment                                                                  AS crm_account_owner_user_segment,
      sfdc_account.billing_country                                                                             AS crm_account_billing_country,
      sfdc_account.billing_country_code                                                                        AS crm_account_billing_country_code,
      sfdc_account.account_type                                                                                AS crm_account_type,
      sfdc_account.industry                                                                                    AS crm_account_industry,
      sfdc_account.sub_industry                                                                                AS crm_account_sub_industry,
      sfdc_account.account_owner                                                                               AS crm_account_owner,
      sfdc_account.partner_vat_tax_id                                                                          AS partner_vat_tax_id,
      sfdc_account.account_manager                                                                             AS account_manager,
      sfdc_account.business_development_rep                                                                    AS business_development_rep,
      sfdc_account.dedicated_service_engineer                                                                  AS dedicated_service_engineer,
      sfdc_account.account_tier                                                                                AS account_tier,
      sfdc_account.license_utilization                                                                         AS license_utilization,
      sfdc_account.support_level                                                                               AS support_level,
      sfdc_account.named_account                                                                               AS named_account,
      sfdc_account.billing_postal_code                                                                         AS billing_postal_code,
      sfdc_account.partner_type                                                                                AS partner_type,
      sfdc_account.partner_status                                                                              AS partner_status,
      sfdc_account.gitlab_customer_success_project                                                             AS gitlab_customer_success_project,
      sfdc_account.demandbase_account_list                                                                     AS demandbase_account_list,
      sfdc_account.demandbase_intent                                                                           AS demandbase_intent,
      sfdc_account.demandbase_page_views                                                                       AS demandbase_page_views,
      sfdc_account.demandbase_score                                                                            AS demandbase_score,
      sfdc_account.demandbase_sessions                                                                         AS demandbase_sessions,
      sfdc_account.demandbase_trending_offsite_intent                                                          AS demandbase_trending_offsite_intent,
      sfdc_account.demandbase_trending_onsite_engagement                                                       AS demandbase_trending_onsite_engagement,
      sfdc_account.is_locally_managed_account                                                                  AS is_locally_managed_account,
      sfdc_account.is_strategic_account                                                                        AS is_strategic_account,
      sfdc_account.partner_track                                                                               AS partner_track,
      sfdc_account.partners_partner_type                                                                       AS partners_partner_type,
      sfdc_account.gitlab_partner_program                                                                      AS gitlab_partner_program,
      sfdc_account.zoom_info_company_name                                                                      AS zoom_info_company_name,
      sfdc_account.zoom_info_company_revenue                                                                   AS zoom_info_company_revenue,
      sfdc_account.zoom_info_company_employee_count                                                            AS zoom_info_company_employee_count,
      sfdc_account.zoom_info_company_industry                                                                  AS zoom_info_company_industry,
      sfdc_account.zoom_info_company_city                                                                      AS zoom_info_company_city,
      sfdc_account.zoom_info_company_state_province                                                            AS zoom_info_company_state_province,
      sfdc_account.zoom_info_company_country                                                                   AS zoom_info_company_country,
      sfdc_account.account_phone                                                                               AS account_phone,
      sfdc_account.zoominfo_account_phone                                                                      AS zoominfo_account_phone,
      sfdc_account.abm_tier                                                                                    AS abm_tier,
      sfdc_account.health_number                                                                               AS health_number,
      sfdc_account.health_score_color                                                                          AS health_score_color,
      sfdc_account.partner_account_iban_number                                                                 AS partner_account_iban_number,
      sfdc_account.gitlab_com_user                                                                             AS gitlab_com_user,
      sfdc_account.zi_technologies                                                                             AS crm_account_zi_technologies,
      sfdc_account.zoom_info_website                                                                           AS crm_account_zoom_info_website,
      sfdc_account.zoom_info_company_other_domains                                                             AS crm_account_zoom_info_company_other_domains,
      sfdc_account.zoom_info_dozisf_zi_id                                                                      AS crm_account_zoom_info_dozisf_zi_id,
      sfdc_account.zoom_info_parent_company_zi_id                                                              AS crm_account_zoom_info_parent_company_zi_id,
      sfdc_account.zoom_info_parent_company_name                                                               AS crm_account_zoom_info_parent_company_name,
      sfdc_account.zoom_info_ultimate_parent_company_zi_id                                                     AS crm_account_zoom_info_ultimate_parent_company_zi_id,
      sfdc_account.zoom_info_ultimate_parent_company_name                                                      AS crm_account_zoom_info_ultimate_parent_company_name,
      sfdc_account.zoom_info_number_of_developers                                                              AS crm_account_zoom_info_number_of_developers,
      sfdc_account.zoom_info_total_funding                                                                     AS crm_account_zoom_info_total_funding,
      sfdc_account.forbes_2000_rank                                                                            AS forbes_2000_rank,
      sfdc_account.sales_development_rep                                                                       AS sales_development_rep,
	  sfdc_account.admin_manual_source_number_of_employees                                                     AS admin_manual_source_number_of_employees, 
      sfdc_account.admin_manual_source_account_address                                                         AS admin_manual_source_account_address,      

      --degenerative dimensions
      sfdc_account.is_sdr_target_account                                                                       AS is_sdr_target_account,
      IFF(sfdc_record_type.record_type_label = 'Partner'
          AND sfdc_account.partner_type IN ('Alliance', 'Channel')
          AND sfdc_account.partner_status = 'Authorized',
          TRUE, FALSE)                                                                                         AS is_reseller,
      sfdc_account.is_jihu_account                                                                             AS is_jihu_account,
      sfdc_account.is_first_order_available,
      sfdc_account.is_key_account                                                                              AS is_key_account,
      CASE
        WHEN CONTAINS (sfdc_account.zi_technologies,'ARE_USED: Jenkins')
          THEN 1
        ELSE 0
      END                                                                                                      AS is_zi_jenkins_present,
      CASE
        WHEN CONTAINS (sfdc_account.zi_technologies, 'ARE_USED: SVN')
          THEN 1
        ELSE 0
      END                                                                                                      AS is_zi_svn_present,
      CASE
        WHEN CONTAINS (sfdc_account.zi_technologies, 'ARE_USED: Tortoise SVN')
          THEN 1
        ELSE 0
      END                                                                                                      AS is_zi_tortoise_svn_present,
      CASE
        WHEN CONTAINS (sfdc_account.zi_technologies, 'ARE_USED: Google Cloud Platform')
          THEN 1
        ELSE 0
      END                                                                                                      AS is_zi_gcp_present,
      CASE
        WHEN CONTAINS (sfdc_account.zi_technologies, 'ARE_USED: Atlassian')
          THEN 1
        ELSE 0
      END                                                                                                      AS is_zi_atlassian_present,
      CASE
        WHEN CONTAINS (sfdc_account.zi_technologies, 'ARE_USED: GitHub')
          THEN 1
        ELSE 0
      END                                                                                                      AS is_zi_github_present,
      CASE
        WHEN CONTAINS (sfdc_account.zi_technologies, 'ARE_USED: GitHub Enterprise')
          THEN 1
        ELSE 0
      END                                                                                                      AS is_zi_github_enterprise_present,
      CASE
        WHEN CONTAINS (sfdc_account.zi_technologies, 'ARE_USED: AWS')
          THEN 1
        ELSE 0
      END                                                                                                      AS is_zi_aws_present,
      CASE
        WHEN CONTAINS (sfdc_account.zi_technologies, 'ARE_USED: Kubernetes')
          THEN 1
        ELSE 0
      END                                                                                                      AS is_zi_kubernetes_present,
      CASE
        WHEN CONTAINS (sfdc_account.zi_technologies, 'ARE_USED: Apache Subversion')
          THEN 1
        ELSE 0
      END                                                                                                      AS is_zi_apache_subversion_present,
      CASE
        WHEN CONTAINS (sfdc_account.zi_technologies, 'ARE_USED: Apache Subversion (SVN)')
          THEN 1
        ELSE 0
      END                                                                                                      AS is_zi_apache_subversion_svn_present,
      CASE
        WHEN CONTAINS (sfdc_account.zi_technologies, 'ARE_USED: Hashicorp')
          THEN 1
        ELSE 0
      END                                                                                                      AS is_zi_hashicorp_present,
      CASE
        WHEN CONTAINS (sfdc_account.zi_technologies, 'ARE_USED: Amazon AWS CloudTrail')
          THEN 1
        ELSE 0
      END                                                                                                      AS is_zi_aws_cloud_trail_present,
      CASE
        WHEN CONTAINS (sfdc_account.zi_technologies, 'ARE_USED: CircleCI')
          THEN 1
        ELSE 0
      END                                                                                                      AS is_zi_circle_ci_present,
      CASE
        WHEN CONTAINS (sfdc_account.zi_technologies, 'ARE_USED: BitBucket')
          THEN 1
        ELSE 0
      END                                                                                                      AS is_zi_bit_bucket_present,
      sfdc_account.is_excluded_from_zoom_info_enrich,

      --dates
      {{ get_date_id('sfdc_account.created_date') }}                                                           AS crm_account_created_date_id,
      sfdc_account.created_date                                                                                AS crm_account_created_date,
      {{ get_date_id('sfdc_account.abm_tier_1_date') }}                                                        AS abm_tier_1_date_id,
      sfdc_account.abm_tier_1_date,
      {{ get_date_id('sfdc_account.abm_tier_2_date') }}                                                        AS abm_tier_2_date_id,
      sfdc_account.abm_tier_2_date,
      {{ get_date_id('sfdc_account.abm_tier_3_date') }}                                                        AS abm_tier_3_date_id,
      sfdc_account.abm_tier_3_date,
      {{ get_date_id('sfdc_account.gtm_acceleration_date') }}                                                  AS gtm_acceleration_date_id,
      sfdc_account.gtm_acceleration_date,
      {{ get_date_id('sfdc_account.gtm_account_based_date') }}                                                 AS gtm_account_based_date_id,
      sfdc_account.gtm_account_based_date,
      {{ get_date_id('sfdc_account.gtm_account_centric_date') }}                                               AS gtm_account_centric_date_id,
      sfdc_account.gtm_account_centric_date,
      {{ get_date_id('sfdc_account.partners_signed_contract_date') }}                                          AS partners_signed_contract_date_id,
      CAST(sfdc_account.partners_signed_contract_date AS date)                                                 AS partners_signed_contract_date,
      {{ get_date_id('sfdc_account.technical_account_manager_date') }}                                         AS technical_account_manager_date_id,
      sfdc_account.technical_account_manager_date,
      {{ get_date_id('sfdc_account.customer_since_date') }}                                                    AS customer_since_date_id,
      sfdc_account.customer_since_date,
      {{ get_date_id('sfdc_account.next_renewal_date') }}                                                      AS next_renewal_date_id,
      sfdc_account.next_renewal_date                                                                           AS next_renewal_date,

      --measures
      sfdc_account.count_active_subscription_charges                                                           AS count_active_subscription_charges,
      sfdc_account.count_active_subscriptions                                                                  AS count_active_subscriptions,
      sfdc_account.count_billing_accounts                                                                      AS count_billing_accounts,
      sfdc_account.count_licensed_users                                                                        AS count_licensed_users,
      sfdc_account.count_of_new_business_won_opportunities                                                     AS count_of_new_business_won_opportunities,
      sfdc_account.count_open_renewal_opportunities                                                            AS count_open_renewal_opportunities,
      sfdc_account.count_opportunities                                                                         AS count_opportunities,
      sfdc_account.count_products_purchased                                                                    AS count_products_purchased,
      sfdc_account.count_won_opportunities                                                                     AS count_won_opportunities,
      sfdc_account.count_concurrent_ee_subscriptions                                                           AS count_concurrent_ee_subscriptions,
      sfdc_account.count_ce_instances                                                                          AS count_ce_instances,
      sfdc_account.count_active_ce_users                                                                       AS count_active_ce_users,
      sfdc_account.count_open_opportunities                                                                    AS count_open_opportunities,
      sfdc_account.count_using_ce                                                                              AS count_using_ce,
      sfdc_account.carr_this_account                                                                           AS carr_this_account,
      sfdc_account.carr_account_family                                                                         AS carr_account_family,
      sfdc_account.potential_users                                                                             AS potential_users,
      sfdc_account.number_of_licenses_this_account                                                             AS number_of_licenses_this_account,
      sfdc_account.decision_maker_count_linkedin                                                               AS decision_maker_count_linkedin,
      sfdc_account.number_of_employees                                                                         AS number_of_employees,
      {%- if model_type == 'live' %}
      sfdc_account.lam                                                                                         AS parent_crm_account_lam,
      sfdc_account.lam_dev_count                                                                               AS parent_crm_account_lam_dev_count,
      {%- elif model_type == 'snapshot' %}
      IFNULL(lam_corrections.estimated_capped_lam, sfdc_account.lam)                                           AS parent_crm_account_lam,
      IFNULL(lam_corrections.dev_count, sfdc_account.lam_dev_count)                                            AS parent_crm_account_lam_dev_count,
      {%- endif %}

      -- PtC and PtE 
      pte_scores.score                                                                                         AS pte_score,
      pte_scores.decile                                                                                        AS pte_decile,
      pte_scores.score_group                                                                                   AS pte_score_group,
      ptc_scores.score                                                                                         AS ptc_score,
      ptc_scores.decile                                                                                        AS ptc_decile,
      ptc_scores.score_group                                                                                   AS ptc_score_group,


      --metadata
      sfdc_account.created_by_id,
      created_by.name                                                                                          AS created_by_name,
      sfdc_account.last_modified_by_id,
      last_modified_by.name                                                                                    AS last_modified_by_name,
      {{ get_date_id('sfdc_account.last_modified_date') }}                                                     AS last_modified_date_id,
      sfdc_account.last_modified_date,
      {{ get_date_id('sfdc_account.last_activity_date') }}                                                     AS last_activity_date_id,
      sfdc_account.last_activity_date,
      sfdc_account.is_deleted

    FROM sfdc_account
    LEFT JOIN map_merged_crm_account
      ON sfdc_account.account_id = map_merged_crm_account.sfdc_account_id
    LEFT JOIN sfdc_record_type
      ON sfdc_account.record_type_id = sfdc_record_type.record_type_id
    LEFT JOIN prep_crm_person
      ON sfdc_account.primary_contact_id = prep_crm_person.sfdc_record_id
    {%- if model_type == 'live' %}
    LEFT JOIN pte_scores 
      ON sfdc_account.account_id = pte_scores.account_id 
        AND pte_scores.is_current = TRUE
    LEFT JOIN ptc_scores
      ON sfdc_account.account_id = ptc_scores.account_id 
        AND ptc_scores.is_current = TRUE
    LEFT OUTER JOIN sfdc_users AS technical_account_manager
      ON sfdc_account.technical_account_manager_id = technical_account_manager.user_id
    LEFT JOIN sfdc_users AS account_owner
      ON sfdc_account.owner_id = account_owner.user_id
    LEFT JOIN sfdc_users AS proposed_account_owner
      ON proposed_account_owner.user_id = sfdc_account.proposed_account_owner
    LEFT JOIN sfdc_users created_by
      ON sfdc_account.created_by_id = created_by.user_id
    LEFT JOIN sfdc_users AS last_modified_by
      ON sfdc_account.last_modified_by_id = last_modified_by.user_id
    {%- elif model_type == 'snapshot' %}
    LEFT OUTER JOIN sfdc_users AS technical_account_manager
      ON sfdc_account.technical_account_manager_id = technical_account_manager.user_id
        AND sfdc_account.snapshot_id = technical_account_manager.snapshot_id
    LEFT JOIN sfdc_users AS account_owner
      ON account_owner.user_id = sfdc_account.owner_id
        AND account_owner.snapshot_id = sfdc_account.snapshot_id
    LEFT JOIN sfdc_users AS proposed_account_owner
      ON proposed_account_owner.user_id = sfdc_account.proposed_account_owner
        AND proposed_account_owner.snapshot_id = sfdc_account.snapshot_id
    LEFT JOIN lam_corrections
      ON sfdc_account.ultimate_parent_account_id = lam_corrections.dim_parent_crm_account_id
        AND sfdc_account.snapshot_id = lam_corrections.snapshot_id
        AND parent_crm_account_demographics_sales_segment = lam_corrections.dim_parent_crm_account_sales_segment
    LEFT JOIN sfdc_users AS created_by
      ON sfdc_account.created_by_id = created_by.user_id
        AND sfdc_account.snapshot_id = created_by.snapshot_id
    LEFT JOIN sfdc_users AS last_modified_by
      ON sfdc_account.last_modified_by_id = last_modified_by.user_id
        AND sfdc_account.snapshot_id = last_modified_by.snapshot_id
    LEFT JOIN pte_scores 
      ON sfdc_account.account_id = pte_scores.account_id
        AND sfdc_account.snapshot_date >= pte_scores.valid_from::DATE
        AND  sfdc_account.snapshot_date < pte_scores.valid_to::DATE
    LEFT JOIN ptc_scores 
      ON sfdc_account.account_id = ptc_scores.account_id
        AND sfdc_account.snapshot_date >= ptc_scores.valid_from::DATE
        AND  sfdc_account.snapshot_date < ptc_scores.valid_to::DATE
    
    {%- endif %}

)

{%- endmacro %}
