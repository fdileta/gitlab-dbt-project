{{config({
    "schema": "common"
  })
}}

WITH first_contact  AS (

    SELECT

      opportunity_id,                                                             -- opportunity_id
      contact_id                                                                  AS sfdc_contact_id,
      md5(cast(coalesce(cast(contact_id as varchar), '') as varchar))             AS crm_person_id,
      ROW_NUMBER() OVER (PARTITION BY opportunity_id ORDER BY created_date ASC)   AS row_num

    FROM {{ ref('sfdc_opportunity_contact_role_source')}}

), crm_account_dimensions AS (

    SELECT *
    FROM {{ ref('map_crm_account')}}

), order_type AS (

    SELECT *
    FROM {{ ref('dim_order_type')}}

), opportunity_source AS (

    SELECT *
    FROM {{ ref('dim_opportunity_source')}}

), purchase_channel AS (

    SELECT *
    FROM {{ ref('dim_purchase_channel')}}

), sales_segment AS (

    SELECT *
    FROM {{ ref('dim_sales_segment')}}

), sfdc_opportunity AS (

    SELECT *
    FROM {{ ref('sfdc_opportunity')}}

), opportunity_fields AS(

    SELECT

      opportunity_id                         AS crm_opportunity_id,
      merged_opportunity_id                  AS merged_crm_opportunity_id,
      account_id                             AS crm_account_id,
      owner_id                               AS crm_sales_rep_id,
      incremental_acv                        AS iacv,
      net_arr,
      created_date,
      {{ get_date_id('created_date') }}      AS created_date_id,
      sales_accepted_date,
      {{ get_date_id('created_date') }}      AS sales_accepted_date_id,
      close_date,
      {{ get_date_id('created_date') }}      AS close_date_id,
      stage_0_pending_acceptance_date,
      {{ get_date_id('created_date') }}      AS stage_0_pending_acceptance_date_id,
      stage_1_discovery_date,
      {{ get_date_id('created_date') }}      AS stage_1_discovery_date_id,
      stage_2_scoping_date,
      {{ get_date_id('created_date') }}      AS stage_2_scoping_date_id,
      stage_3_technical_evaluation_date,
      {{ get_date_id('created_date') }}      AS stage_3_technical_evaluation_date_id,
      stage_4_proposal_date,
      {{ get_date_id('created_date') }}      AS stage_4_proposal_date_id,
      stage_5_negotiating_date,
      {{ get_date_id('created_date') }}      AS stage_5_negotiating_date_id,
      stage_6_closed_won_date,
      {{ get_date_id('created_date') }}      AS stage_6_closed_won_date_id,
      stage_6_closed_lost_date,
      {{ get_date_id('created_date') }}      AS stage_6_closed_lost_date_id,
      days_in_0_pending_acceptance,
      days_in_1_discovery,
      days_in_2_scoping,
      days_in_3_technical_evaluation,
      days_in_4_proposal,
      days_in_5_negotiating,
      is_closed,
      is_won,
      is_refund,
      is_downgrade,
      is_swing_deal,
      is_edu_oss,
      is_web_portal_purchase,
      deal_path,
      order_type_stamped                      AS order_type,
      sales_segment,
      sales_qualified_source,
      days_in_sao

    FROM sfdc_opportunity

), is_sao AS (

    SELECT

      opportunity_id,
      CASE
        WHEN sfdc_opportunity.sales_accepted_date IS NOT NULL
          AND is_edu_oss = 0
          AND stage_name != '10-Duplicate'
            THEN TRUE
      	ELSE FALSE
      END                                                                         AS is_sao

    FROM sfdc_opportunity

), is_sdr_sao AS (

    SELECT

      opportunity_id,
      CASE
        WHEN opportunity_id in (select opportunity_id from is_sao where is_sao = true)
          AND sales_qualified_source IN (
                                        'SDR Generated'
                                        , 'BDR Generated'
                                        )
            THEN TRUE
        ELSE FALSE
      END                                                                         AS is_sdr_sao

    FROM sfdc_opportunity

), final_opportunities AS (

    SELECT

      -- opportunity and person ids
      opportunity_fields.crm_opportunity_id,
      opportunity_fields.merged_crm_opportunity_id,
      opportunity_fields.crm_account_id,
      first_contact.crm_person_id,
      first_contact.sfdc_contact_id,

      -- dates
      opportunity_fields.created_date,
      opportunity_fields.created_date_id,
      opportunity_fields.sales_accepted_date,
      opportunity_fields.sales_accepted_date_id,
      opportunity_fields.close_date,
      opportunity_fields.close_date_id,
      opportunity_fields.stage_0_pending_acceptance_date,
      opportunity_fields.stage_0_pending_acceptance_date_id,
      opportunity_fields.stage_1_discovery_date,
      opportunity_fields.stage_1_discovery_date_id,
      opportunity_fields.stage_2_scoping_date,
      opportunity_fields.stage_2_scoping_date_id,
      opportunity_fields.stage_3_technical_evaluation_date,
      opportunity_fields.stage_3_technical_evaluation_date_id,
      opportunity_fields.stage_4_proposal_date,
      opportunity_fields.stage_4_proposal_date_id,
      opportunity_fields.stage_5_negotiating_date,
      opportunity_fields.stage_5_negotiating_date_id,
      opportunity_fields.stage_6_closed_won_date,
      opportunity_fields.stage_6_closed_won_date_id,
      opportunity_fields.stage_6_closed_lost_date,
      opportunity_fields.stage_6_closed_lost_date_id,
      opportunity_fields.days_in_0_pending_acceptance,
      opportunity_fields.days_in_1_discovery,
      opportunity_fields.days_in_2_scoping,
      opportunity_fields.days_in_3_technical_evaluation,
      opportunity_fields.days_in_4_proposal,
      opportunity_fields.days_in_5_negotiating,
      opportunity_fields.days_in_sao,

      -- common dimension keys
      COALESCE(opportunity_fields.crm_sales_rep_id, MD5(-1))                                                        AS dim_crm_sales_rep_id,
      COALESCE(order_type.dim_order_type_id, MD5(-1))                                                               AS dim_order_type_id,
      COALESCE(opportunity_source.dim_opportunity_source_id, MD5(-1))                                               AS dim_opportunity_source_id,
      COALESCE(purchase_channel.dim_purchase_channel_id, MD5(-1))                                                   AS dim_purchase_channel_id,
      COALESCE(crm_account_dimensions.dim_sales_segment_name_id,sales_segment.dim_sales_segment_name_id, MD5(-1))   AS dim_sales_segment_name_id,
      COALESCE(crm_account_dimensions.dim_geo_region_name_id, MD5(-1))                                              AS dim_geo_region_name_id,
      COALESCE(crm_account_dimensions.dim_geo_sub_region_name_id, MD5(-1))                                          AS dim_geo_sub_region_name_id,
      COALESCE(crm_account_dimensions.dim_geo_area_name_id, MD5(-1))                                                AS dim_geo_area_name_id,
      COALESCE(crm_account_dimensions.dim_sales_territory_name_id, MD5(-1))                                         AS dim_sales_territory_name_id,
      COALESCE(crm_account_dimensions.dim_industry_name_id, MD5(-1))                                                AS dim_industry_name_id,

      -- flags
      opportunity_fields.is_closed,
      opportunity_fields.is_won,
      opportunity_fields.is_refund,
      opportunity_fields.is_downgrade,
      opportunity_fields.is_swing_deal,
      opportunity_fields.is_edu_oss,
      opportunity_fields.is_web_portal_purchase,
      is_sao.is_sao,
      is_sdr_sao.is_sdr_sao,

      -- additive fields
      opportunity_fields.iacv,
      opportunity_fields.net_arr

    FROM opportunity_fields
    LEFT JOIN crm_account_dimensions
      ON opportunity_fields.crm_account_id = crm_account_dimensions.crm_account_id
    LEFT JOIN first_contact
      ON opportunity_fields.crm_opportunity_id = first_contact.opportunity_id AND first_contact.row_num = 1
    LEFT JOIN opportunity_source
      ON opportunity_fields.sales_qualified_source = opportunity_source.opportunity_source_name
    LEFT JOIN order_type
      ON opportunity_fields.order_type = order_type.order_type_name
    LEFT JOIN purchase_channel
      ON opportunity_fields.deal_path = purchase_channel.purchase_channel_name
    LEFT JOIN sales_segment
      ON opportunity_fields.sales_segment = sales_segment.dim_sales_segment_name
    LEFT JOIN is_sao
      ON opportunity_fields.crm_opportunity_id = is_sao.opportunity_id
    LEFT JOIN is_sdr_sao
      ON opportunity_fields.crm_opportunity_id = is_sdr_sao.opportunity_id

)

{{ dbt_audit(
    cte_ref="final_opportunities",
    created_by="@mcooperDD",
    updated_by="@mcooperDD",
    created_date="2020-11-30",
    updated_date="2020-11-30"
) }}
