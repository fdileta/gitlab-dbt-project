WITH source AS (

    SELECT *
    FROM {{ ref('fy24_mock_opportunities') }}

 ), renamed AS (

      SELECT
        -- keys
        account_id,
        opportunity_id,
        opportunity_name,
        owner_id,

        -- logistical information
        is_closed,
        is_won,
        close_date::TIMESTAMP_TZ(9),
        created_date::TIMESTAMP_TZ(9),
        days_in_stage,
        deployment_preference,
        generated_source,
        lead_source,
        merged_opportunity_id,
        duplicate_opportunity_id,
        account_owner,
        opportunity_owner,
        opportunity_owner_manager,
        opportunity_owner_department,
        opportunity_sales_development_representative,
        opportunity_business_development_representative,
        opportunity_business_development_representative_lookup,
        opportunity_development_representative,


        account_owner_team_stamped,

        sales_accepted_date::TIMESTAMP_TZ(9),
        sales_path,
        sales_qualified_date::TIMESTAMP_TZ(9),
        iqm_submitted_by_role,

        sales_type,
        net_new_source_categories,
        source_buckets,
        stage_name,
        order_type,
        deal_path,

        -- opportunity information
        acv,
        amount,
        closed_deals, -- so that you can exclude closed deals that had negative impact
        competitors,
        critical_deal_flag,
        deal_size,
        forecast_category_name,
        forecasted_iacv,
        iacv_created_date,
        incremental_acv,
        pre_covid_iacv,
        invoice_number,
        is_refund,
        is_downgrade,
        is_swing_deal,
        is_edu_oss,
        is_ps_opp,
        net_incremental_acv,
        primary_campaign_source_id,
        probability,
        professional_services_value,
        pushed_count,
        reason_for_loss,
        reason_for_loss_details,
        refund_iacv,
        downgrade_iacv,
        renewal_acv,
        renewal_amount,
        sales_qualified_source,
        sales_qualified_source_grouped,
        sqs_bucket_engagement,
        sdr_pipeline_contribution,
        solutions_to_be_replaced,
        technical_evaluation_date,
        total_contract_value,
        recurring_amount,
        true_up_amount,
        proserv_amount,
        other_non_recurring_amount,
        upside_iacv,
        upside_swing_deal_iacv,
        is_web_portal_purchase,
        opportunity_term,
        partner_initiated_opportunity,
        user_segment,
        subscription_start_date,
        subscription_end_date,
        true_up_value,
        order_type_live,
        order_type_stamped,
        order_type_grouped,
        growth_type,
        net_arr,
        arr_basis,
        arr,
        days_in_sao,
        new_logo_count,
        user_segment_stamped,
        CASE
          WHEN user_segment_stamped IN ('Large', 'PubSec') THEN 'Large'
          ELSE user_segment_stamped
        END                                             AS user_segment_stamped_grouped,
        user_geo_stamped,
        user_region_stamped,
        user_area_stamped,
        {{ sales_segment_region_grouped('user_segment_stamped', 'user_geo_stamped', 'user_region_stamped') }}
                                                        AS user_segment_region_stamped_grouped,
        user_segment_geo_region_area_stamped,
        crm_opp_owner_user_role_type_stamped,
        crm_opp_owner_stamped_name,
        crm_account_owner_stamped_name,
        sao_crm_opp_owner_stamped_name,
        sao_crm_account_owner_stamped_name,
        sao_crm_opp_owner_sales_segment_stamped,
        sao_crm_opp_owner_sales_segment_geo_region_area_stamped,
        sao_crm_opp_owner_sales_segment_stamped_grouped,
        sao_crm_opp_owner_geo_stamped,
        sao_crm_opp_owner_region_stamped,
        sao_crm_opp_owner_area_stamped,
        {{ sales_segment_region_grouped('sao_crm_opp_owner_sales_segment_stamped', 'sao_crm_opp_owner_geo_stamped', 'sao_crm_opp_owner_region_stamped') }}
                                                        AS sao_crm_opp_owner_segment_region_stamped_grouped,
        opportunity_category,
        opportunity_health,
        risk_type,
        risk_reasons,
        tam_notes,
        primary_solution_architect,
        product_details,
        product_category,
        products_purchased,
        opportunity_deal_size,
        payment_schedule,
        comp_y2_iacv,
        comp_new_logo_override,
        is_pipeline_created_eligible,

      -- ************************************
      -- sales segmentation deprecated fields - 2020-09-03
      -- left temporary for the sake of MVC and avoid breaking SiSense existing charts
        sales_segment,
        parent_segment,
      -- ************************************

        -- dates in stage fields
        days_in_0_pending_acceptance,
        days_in_1_discovery,
        days_in_2_scoping,
        days_in_3_technical_evaluation,
        days_in_4_proposal,
        days_in_5_negotiating,

        stage_0_pending_acceptance_date,
        stage_1_discovery_date,
        stage_2_scoping_date,
        stage_3_technical_evaluation_date,
        stage_4_proposal_date,
        stage_5_negotiating_date,
        stage_6_awaiting_signature_date,
        stage_6_closed_won_date,
        stage_6_closed_lost_date,

        -- sales segment fields
        division_sales_segment_stamped,
        -- channel reporting
        -- original issue: https://gitlab.com/gitlab-data/analytics/-/issues/6072
        dr_partner_deal_type,
        dr_partner_engagement,
        dr_deal_id,
        dr_primary_registration,
        {{ channel_type('sqs_bucket_engagement', 'order_type_stamped') }}
                                                        AS channel_type,
        partner_account,
        dr_status,
        distributor,
        influence_partner,
        fulfillment_partner,
        platform_partner,
        partner_track,
        resale_partner_track,
        is_public_sector_opp,
        is_registration_from_portal,
        calculated_discount,
        partner_discount,
        partner_discount_calc,
        comp_channel_neutral,

        -- command plan fields
        cp_champion,
        cp_close_plan,
        cp_competition,
        cp_decision_criteria,
        cp_decision_process,
        cp_economic_buyer,
        cp_help,
        cp_identify_pain,
        cp_metrics,
        cp_partner,
        cp_paper_process,
        cp_review_notes,
        cp_risks,
        cp_use_cases,
        cp_value_driver,
        cp_why_do_anything_at_all,
        cp_why_gitlab,
        cp_why_now,
        cp_score,

        -- original issue: https://gitlab.com/gitlab-data/analytics/-/issues/6577
        sa_tech_evaluation_close_status,
        sa_tech_evaluation_end_date,
        sa_tech_evaluation_start_date,

        -- flag to identify eligible booking deals, excluding jihu - issue: https://gitlab.com/gitlab-com/sales-team/field-operations/systems/-/issues/1805
        fpa_master_bookings_flag,

        downgrade_reason,
        ssp_id,
        ga_client_id,

        -- metadata
        convert_timezone('America/Los_Angeles',convert_timezone('UTC',
                 CURRENT_TIMESTAMP()))                  AS _last_dbt_run,
        days_since_last_activity,
        is_deleted,
        last_activity_date,
        record_type_id

      FROM source

 )

 SELECT *
 FROM renamed