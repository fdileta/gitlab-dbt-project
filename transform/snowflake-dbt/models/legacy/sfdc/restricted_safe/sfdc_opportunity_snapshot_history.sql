{{ config(
    tags=["mnpi"]
) }}

WITH sfdc_opportunity_snapshots AS (

    SELECT *
    FROM {{ref('sfdc_opportunity_snapshots_base')}}

), net_arr_net_iacv_conversion_factors AS (

    SELECT *
    FROM {{ref('sheetload_net_arr_net_iacv_conversion_factors_source')}}

), final AS (

    SELECT
      -- keys
      date_actual,
      valid_from,
      valid_to,
      is_currently_valid,
      opportunity_snapshot_id,
      accountid                      AS account_id,
      id                             AS opportunity_id,
      name                           AS opportunity_name,
      ownerid                        AS owner_id,

      -- logistical information
      business_type__c               AS business_type,
      closedate::DATE                AS close_date,
      createddate::DATE              AS created_date,
      deployment_preference__c       AS deployment_preference,
      sql_source__c                  AS generated_source,
      leadsource                     AS lead_source,
      merged_opportunity__c          AS merged_opportunity_id,
      opportunity_owner__c           AS opportunity_owner,
      account_owner__c               AS opportunity_owner_manager,
      sales_market__c                AS opportunity_owner_department,
      SDR_LU__c                      AS opportunity_sales_development_representative,
      BDR_LU__c                      AS opportunity_business_development_representative,
      BDR_SDR__c                     AS opportunity_development_representative,
      account_owner_team_o__c        AS account_owner_team_stamped,
      COALESCE({{ sales_segment_cleaning('ultimate_parent_sales_segment_emp_o__c') }}, {{ sales_segment_cleaning('ultimate_parent_sales_segment_o__c') }} )
                                     AS parent_segment,
      sales_accepted_date__c         AS sales_accepted_date,
      engagement_type__c             AS sales_path,
      sales_qualified_date__c        AS sales_qualified_date,
      COALESCE({{ sales_segment_cleaning('sales_segmentation_employees_o__c') }}, 'Unknown' )
                                     AS sales_segment,
      type                           AS sales_type,
      {{  sfdc_source_buckets('leadsource') }}
      stagename                      AS stage_name,
      revenue_type__c                AS order_type,

      --Stamped User Segment fields
      {{ sales_hierarchy_sales_segment_cleaning('user_segment_o__c') }}
                                     AS user_segment_stamped,
      stamped_user_geo__c            AS user_geo_stamped,
      stamped_user_region__c         AS user_region_stamped,
      stamped_user_area__c           AS user_area_stamped,
      
      -- opportunity information
      acv_2__c                       AS acv,
      IFF(acv_2__c >= 0, 1, 0)       AS closed_deals, -- so that you can exclude closed deals that had negative impact
      competitors__c                 AS competitors,
      critical_deal_flag__c          AS critical_deal_flag,
      {{sfdc_deal_size('incremental_acv_2__c', 'deal_size')}},
      forecastcategoryname           AS forecast_category_name,
      incremental_acv_2__c           AS forecasted_iacv,
      iacv_created_date__c           AS iacv_created_date,
      incremental_acv__c             AS incremental_acv,
      invoice_number__c              AS invoice_number,
      is_refund_opportunity__c       AS is_refund,
      is_downgrade_opportunity__c    AS is_downgrade,
      swing_deal__c                  AS is_swing_deal,
      is_edu_oss_opportunity__c      AS is_edu_oss,
      isclosed                       AS is_closed,
      net_iacv__c                    AS net_incremental_acv,
      nrv__c                         AS nrv,
      campaignid                     AS primary_campaign_source_id,
      probability                    AS probability,
      professional_services_value__c AS professional_services_value,
      push_counter__c                AS pushed_count,
      reason_for_lost__c             AS reason_for_loss,
      reason_for_lost_details__c     AS reason_for_loss_details,
      refund_iacv__c                 AS refund_iacv,
      downgrade_iacv__c              AS downgrade_iacv,
      renewal_acv__c                 AS renewal_acv,
      renewal_amount__c              AS renewal_amount,
      sql_source__c                  AS sales_qualified_source,
      IFF(sales_qualified_source = 'Channel Generated', 'Partner Sourced', 'Co-sell')
                                     AS sqs_bucket_engagement,
      solutions_to_be_replaced__c    AS solutions_to_be_replaced,
      amount                         AS total_contract_value,
      upside_iacv__c                 AS upside_iacv,
      upside_swing_deal_iacv__c      AS upside_swing_deal_iacv,
      web_portal_purchase__c         AS is_web_portal_purchase,
      opportunity_term__c            AS opportunity_term,
      opportunity_category__c        AS opportunity_category,
      arr_net__c                     AS net_arr,
      CASE
        WHEN closedate::DATE >= '2018-02-01' THEN COALESCE((net_iacv__c * ratio_net_iacv_to_net_arr), net_iacv__c)
        ELSE NULL
      END                            AS net_arr_converted,
      CASE
        WHEN closedate::DATE <= '2021-01-31' THEN net_arr_converted
        ELSE net_arr
      END                            AS net_arr_final,
      arr_basis__c                   AS arr_basis,
      arr__c                         AS arr,
      amount                         AS amount,
      recurring_amount__c            AS recurring_amount,
      true_up_amount__c              AS true_up_amount,
      proserv_amount__c              AS proserv_amount,
      other_non_recurring_amount__c  AS other_non_recurring_amount,
      start_date__c::DATE            AS subscription_start_date,
      end_date__c::DATE              AS subscription_end_date,

      -- channel reporting
      -- original issue: https://gitlab.com/gitlab-data/analytics/-/issues/6072
      deal_path__c                                AS deal_path,
      dr_partner_deal_type__c                     AS dr_partner_deal_type,
      dr_partner_engagement__c                    AS dr_partner_engagement,
      order_type_test__c                          AS order_type_stamped,
      {{ channel_type('sqs_bucket_engagement', 'order_type_stamped') }}
                                                  AS channel_type,
      impartnerprm__partneraccount__c             AS partner_account,
      vartopiadrs__dr_status1__c                  AS dr_status,
      distributor__c                              AS distributor,
      influence_partner__c                        AS influence_partner,
      fulfillment_partner__c                      AS fulfillment_partner,
      platform_partner__c                         AS platform_partner,
      partner_track__c                            AS partner_track,
      public_sector_opp__c::BOOLEAN               AS is_public_sector_opp,
      registration_from_portal__c::BOOLEAN        AS is_registration_from_portal,
      calculated_discount__c                      AS calculated_discount,
      partner_discount__c                         AS partner_discount,
      partner_discount_calc__c                    AS partner_discount_calc,
      comp_channel_neutral__c                     AS comp_channel_neutral,

      -- command plan fields
      fm_champion__c                 AS cp_champion,
      fm_close_plan__c               AS cp_close_plan,
      fm_competition__c              AS cp_competition,
      fm_decision_criteria__c        AS cp_decision_criteria,
      fm_decision_process__c         AS cp_decision_process,
      fm_economic_buyer__c           AS cp_economic_buyer,
      fm_identify_pain__c            AS cp_identify_pain,
      fm_metrics__c                  AS cp_metrics,
      fm_risks__c                    AS cp_risks,
      fm_use_cases__c                AS cp_use_cases,
      fm_value_driver__c             AS cp_value_driver,
      fm_why_do_anything_at_all__c   AS cp_why_do_anything_at_all,
      fm_why_gitlab__c               AS cp_why_gitlab,
      fm_why_now__c                  AS cp_why_now,
      fm_score__c                    AS cp_score,

      -- ************************************

       -- dates in stage fields
      x0_pending_acceptance_date__c               AS stage_0_pending_acceptance_date,
      x1_discovery_date__c                        AS stage_1_discovery_date,
      x2_scoping_date__c                          AS stage_2_scoping_date,
      x3_technical_evaluation_date__c             AS stage_3_technical_evaluation_date,
      x4_proposal_date__c                         AS stage_4_proposal_date,
      x5_negotiating_date__c                      AS stage_5_negotiating_date,
      x6_awaiting_signature_date__c               AS stage_6_awaiting_signature_date,
      x6_closed_won_date__c                       AS stage_6_closed_won_date,
      x7_closed_lost_date__c                      AS stage_6_closed_lost_date,


      -- flag to identify eligible booking deals, excluding jihu - issue: https://gitlab.com/gitlab-com/sales-team/field-operations/systems/-/issues/1805
      fp_a_master_bookings_flag__c                AS fpa_master_bookings_flag,


      -- metadata
      convert_timezone('America/Los_Angeles',convert_timezone('UTC',
               CURRENT_TIMESTAMP())) AS _last_dbt_run,
      isdeleted                      AS is_deleted,
      lastactivitydate               AS last_activity_date,
      recordtypeid                   AS record_type_id
    FROM sfdc_opportunity_snapshots
    LEFT JOIN net_arr_net_iacv_conversion_factors
      ON sfdc_opportunity_snapshots.id = net_arr_net_iacv_conversion_factors.opportunity_id

)

SELECT *
FROM final
