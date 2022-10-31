WITH biz_person AS (

    SELECT *
    FROM {{ref('sfdc_bizible_person_source')}}
    WHERE is_deleted = 'FALSE'

), biz_touchpoints AS (

    SELECT *
    FROM {{ref('sfdc_bizible_touchpoint_source')}}
    WHERE bizible_touchpoint_position LIKE '%FT%'
     AND is_deleted = 'FALSE'

), biz_person_with_touchpoints AS (

    SELECT

      biz_touchpoints.*,
      biz_person.bizible_contact_id,
      biz_person.bizible_lead_id

    FROM biz_touchpoints
    JOIN biz_person
      ON biz_touchpoints.bizible_person_id = biz_person.person_id

), sfdc_contacts AS (

    SELECT
    {{ hash_sensitive_columns('sfdc_contact_source') }}
    FROM {{ref('sfdc_contact_source')}}
    WHERE is_deleted = 'FALSE'

), sfdc_leads AS (

    SELECT
    {{ hash_sensitive_columns('sfdc_lead_source') }}
    FROM {{ref('sfdc_lead_source')}}
    WHERE is_deleted = 'FALSE'

),  was_converted_lead AS (

    SELECT DISTINCT
      contact_id,
      1 AS was_converted_lead
    FROM {{ ref('sfdc_contact_source') }}
    INNER JOIN {{ ref('sfdc_lead_source') }}
      ON sfdc_contact_source.contact_id = sfdc_lead_source.converted_contact_id

),  crm_person_final AS (

    SELECT
      --id
      {{ dbt_utils.surrogate_key(['sfdc_contacts.contact_id']) }} AS dim_crm_person_id,
      sfdc_contacts.contact_id                      AS sfdc_record_id,
      bizible_person_id                             AS bizible_person_id,
      'contact'                                     AS sfdc_record_type,
      contact_email_hash                            AS email_hash,
      email_domain,
      email_domain_type,

      --keys
      master_record_id,
      owner_id,
      record_type_id,
      account_id                                    AS dim_crm_account_id,
      reports_to_id,
      owner_id                                      AS dim_crm_user_id,

      --info
      person_score,
      behavior_score,
      contact_title                                 AS title,
      it_job_title_hierarchy,
      has_opted_out_email,
      email_bounced_date,
      email_bounced_reason,
      contact_status                                AS status,
      lead_source,
      lead_source_type,
      was_converted_lead.was_converted_lead         AS was_converted_lead,
      source_buckets,
      net_new_source_categories,
      bizible_touchpoint_position,
      bizible_marketing_channel_path,
      bizible_touchpoint_date,
      marketo_last_interesting_moment,
      marketo_last_interesting_moment_date,
      outreach_step_number,
      NULL                                          AS matched_account_owner_role,
      NULL                                          AS matched_account_account_owner_name,
      NULL                                          AS matched_account_sdr_assigned,
      NULL                                          AS matched_account_type,
      NULL                                          AS matched_account_gtm_strategy,
      is_first_order_initial_mql,
      is_first_order_mql,
      is_first_order_person,
      last_utm_content,
      last_utm_campaign,
      sequence_step_type,
      name_of_active_sequence,
      sequence_task_due_date,
      sequence_status,
      is_actively_being_sequenced,
      prospect_share_status,
      partner_prospect_status,
      partner_prospect_id,
      partner_prospect_owner_name,
      mailing_country                               AS country,
      mailing_state                                 AS state,
      last_activity_date,
      NULL                                          AS employee_bucket,
      account_demographics_sales_segment,
      account_demographics_sales_segment_grouped,
      account_demographics_geo,
      account_demographics_region,
      account_demographics_area,
      account_demographics_segment_region_grouped,
      account_demographics_territory,
      account_demographics_employee_count,
      account_demographics_max_family_employee,
      account_demographics_upa_country,
      account_demographics_upa_state,
      account_demographics_upa_city,
      account_demographics_upa_street,
      account_demographics_upa_postal_code,
      NULL                                          AS crm_partner_id,
      NULL                                          AS ga_client_id,
      NULL                                          AS cognism_company_office_city,
      NULL                                          AS cognism_company_office_state,
      NULL                                          AS cognism_company_office_country,
      NULL                                          AS cognism_city,
      NULL                                          AS cognism_state,
      NULL                                          AS cognism_country,
      cognism_employee_count,
      NULL                                          AS leandata_matched_account_billing_state,
      NULL                                          AS leandata_matched_account_billing_postal_code,
      NULL                                          AS leandata_matched_account_billing_country,
      NULL                                          AS leandata_matched_account_employee_count,
      NULL                                          AS leandata_matched_account_sales_segment,
      zoominfo_contact_city,
      zoominfo_contact_state,
      zoominfo_contact_country,
      zoominfo_company_city,
      zoominfo_company_state,
      zoominfo_company_country,
      zoominfo_phone_number, 
      zoominfo_mobile_phone_number,
      zoominfo_do_not_call_direct_phone,
      zoominfo_do_not_call_mobile_phone,
	  last_transfer_date_time,
	  time_from_last_transfer_to_sequence,
	  time_from_mql_to_last_transfer,
      NULL                                           AS zoominfo_company_employee_count


    FROM sfdc_contacts
    LEFT JOIN biz_person_with_touchpoints
      ON sfdc_contacts.contact_id = biz_person_with_touchpoints.bizible_contact_id
    LEFT JOIN was_converted_lead
      ON was_converted_lead.contact_id = sfdc_contacts.contact_id

    UNION

    SELECT
      --id
      {{ dbt_utils.surrogate_key(['lead_id']) }} AS dim_crm_person_id,
      lead_id                                    AS sfdc_record_id,
      bizible_person_id                          AS bizible_person_id,
      'lead'                                     AS sfdc_record_type,
      lead_email_hash                            AS email_hash,
      email_domain,
      email_domain_type,

      --keys
      master_record_id,
      owner_id,
      record_type_id,
      lean_data_matched_account                  AS dim_crm_account_id,
      NULL                                       AS reports_to_id,
      owner_id                                   AS dim_crm_user_id,

      --info
      person_score,
      behavior_score,
      title,
      it_job_title_hierarchy,
      has_opted_out_email,
      email_bounced_date,
      email_bounced_reason,
      lead_status                                AS status,
      lead_source,
      lead_source_type,
      0                                          AS was_converted_lead,
      source_buckets,
      net_new_source_categories,
      bizible_touchpoint_position,
      bizible_marketing_channel_path,
      bizible_touchpoint_date,
      marketo_last_interesting_moment,
      marketo_last_interesting_moment_date,
      outreach_step_number,
      matched_account_owner_role,
      matched_account_account_owner_name,
      matched_account_sdr_assigned,
      matched_account_type,
      matched_account_gtm_strategy,
      is_first_order_initial_mql,
      is_first_order_mql,
      is_first_order_person,
      last_utm_content,
      last_utm_campaign,
      sequence_step_type,
      name_of_active_sequence,
      sequence_task_due_date,
      sequence_status,
      is_actively_being_sequenced,
      prospect_share_status,
      partner_prospect_status,
      partner_prospect_id,
      partner_prospect_owner_name,
      country,
      state,
      last_activity_date,
      employee_bucket,
      account_demographics_sales_segment,
      account_demographics_sales_segment_grouped,
      account_demographics_geo,
      account_demographics_region,
      account_demographics_area,
      account_demographics_segment_region_grouped,
      account_demographics_territory,
      account_demographics_employee_count,
      account_demographics_max_family_employee,
      account_demographics_upa_country,
      account_demographics_upa_state,
      account_demographics_upa_city,
      account_demographics_upa_street,
      account_demographics_upa_postal_code,
      crm_partner_id,
      ga_client_id,
      cognism_company_office_city,
      cognism_company_office_state,
      cognism_company_office_country,
      cognism_city,
      cognism_state,
      cognism_country,
      cognism_employee_count,
      leandata_matched_account_billing_state,
      leandata_matched_account_billing_postal_code,
      leandata_matched_account_billing_country,
      leandata_matched_account_employee_count,
      leandata_matched_account_sales_segment,
      zoominfo_contact_city,
      zoominfo_contact_state,
      zoominfo_contact_country,
      zoominfo_company_city,
      zoominfo_company_state,
      zoominfo_company_country,
      zoominfo_phone_number, 
      zoominfo_mobile_phone_number,
      zoominfo_do_not_call_direct_phone,
      zoominfo_do_not_call_mobile_phone,
	  last_transfer_date_time,
	  time_from_last_transfer_to_sequence,
	  time_from_mql_to_last_transfer,
      zoominfo_company_employee_count

    FROM sfdc_leads
    LEFT JOIN biz_person_with_touchpoints
      ON sfdc_leads.lead_id = biz_person_with_touchpoints.bizible_lead_id
    WHERE is_converted = 'FALSE'

), duplicates AS (

    SELECT
      dim_crm_person_id
    FROM crm_person_final
    GROUP BY 1
    HAVING COUNT(*) > 1

), final AS (

    SELECT *
    FROM crm_person_final
    WHERE dim_crm_person_id NOT IN (
                                    SELECT *
                                    FROM duplicates
                                      )
      AND sfdc_record_id != '00Q4M00000kDDKuUAO' --DQ issue: https://gitlab.com/gitlab-data/analytics/-/issues/11559

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@mcooperDD",
    updated_by="@rkohnke",
    created_date="2020-12-08",
    updated_date="2022-10-26"
) }}
