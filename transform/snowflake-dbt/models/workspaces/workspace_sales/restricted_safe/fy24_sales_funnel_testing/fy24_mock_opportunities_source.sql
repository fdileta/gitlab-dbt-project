WITH source AS (

    SELECT *
    FROM {{ ref('fy24_mock_opportunities') }}

 ), renamed AS (

      SELECT
        -- keys
        account_id::VARCHAR AS account_id,
        opportunity_id::VARCHAR AS opportunity_id,
        opportunity_name::VARCHAR AS opportunity_name,
        owner_id::VARCHAR AS owner_id,

        -- logistical information
        is_closed::VARCHAR AS is_closed,
        is_won::VARCHAR AS is_won,
        close_date::TIMESTAMP_TZ(9) AS close_date,
        created_date::TIMESTAMP_TZ(9) AS created_date,
        days_in_stage::VARCHAR AS days_in_stage,
        deployment_preference::VARCHAR AS deployment_preference,
        generated_source::VARCHAR AS generated_source,
        lead_source::VARCHAR AS lead_source,
        merged_opportunity_id::VARCHAR AS merged_opportunity_id,
        duplicate_opportunity_id::VARCHAR AS duplicate_opportunity_id,
        account_owner::VARCHAR AS account_owner,
        opportunity_owner::VARCHAR AS opportunity_owner,
        opportunity_owner_manager::VARCHAR AS opportunity_owner_manager,
        opportunity_owner_department::VARCHAR AS opportunity_owner_department,
        opportunity_sales_development_representative::VARCHAR AS opportunity_sales_development_representative,
        opportunity_business_development_representative::VARCHAR AS opportunity_business_development_representative,
        opportunity_business_development_representative_lookup::VARCHAR AS opportunity_business_development_representative_lookup,
        opportunity_development_representative::VARCHAR AS opportunity_development_representative,


        account_owner_team_stamped::VARCHAR AS account_owner_team_stamped,

        sales_accepted_date::TIMESTAMP_TZ(9) AS sales_accepted_date,
        sales_path::VARCHAR AS sales_path,
        sales_qualified_date::TIMESTAMP_TZ(9) AS sales_qualified_date,
        iqm_submitted_by_role::VARCHAR AS iqm_submitted_by_role,

        sales_type::VARCHAR AS sales_type,
        net_new_source_categories::VARCHAR AS net_new_source_categories,
        source_buckets::VARCHAR AS source_buckets,
        stage_name::VARCHAR AS stage_name,
        order_type::VARCHAR AS order_type,
        deal_path::VARCHAR AS deal_path,

        -- opportunity information
        acv::VARCHAR AS acv,
        amount::VARCHAR AS amount,
        closed_deals::VARCHAR AS closed_deals, -- so that you can exclude closed deals that had negative impact
        competitors::VARCHAR AS competitors,
        critical_deal_flag::VARCHAR AS critical_deal_flag,
        deal_size::VARCHAR AS deal_size,
        forecast_category_name::VARCHAR AS forecast_category_name,
        forecasted_iacv::VARCHAR AS forecasted_iacv,
        iacv_created_date::VARCHAR AS iacv_created_date,
        incremental_acv::VARCHAR AS incremental_acv,
        pre_covid_iacv::VARCHAR AS pre_covid_iacv,
        invoice_number::VARCHAR AS invoice_number,
        is_refund::VARCHAR AS is_refund,
        is_downgrade::VARCHAR AS is_downgrade,
        is_swing_deal::VARCHAR AS is_swing_deal,
        is_edu_oss::VARCHAR AS is_edu_oss,
        is_ps_opp::VARCHAR AS is_ps_opp,
        net_incremental_acv::VARCHAR AS net_incremental_acv,
        primary_campaign_source_id::VARCHAR AS primary_campaign_source_id,
        probability::VARCHAR AS probability,
        professional_services_value::VARCHAR AS professional_services_value,
        pushed_count::VARCHAR AS pushed_count,
        reason_for_loss::VARCHAR AS reason_for_loss,
        reason_for_loss_details::VARCHAR AS reason_for_loss_details,
        refund_iacv::VARCHAR AS refund_iacv,
        downgrade_iacv::VARCHAR AS downgrade_iacv,
        renewal_acv::VARCHAR AS renewal_acv,
        renewal_amount::VARCHAR AS renewal_amount,
        sales_qualified_source::VARCHAR AS sales_qualified_source,
        sales_qualified_source_grouped::VARCHAR AS sales_qualified_source_grouped,
        sqs_bucket_engagement::VARCHAR AS sqs_bucket_engagement,
        sdr_pipeline_contribution::VARCHAR AS sdr_pipeline_contribution,
        solutions_to_be_replaced::VARCHAR AS solutions_to_be_replaced,
        technical_evaluation_date::VARCHAR AS technical_evaluation_date,
        total_contract_value::VARCHAR AS total_contract_value,
        recurring_amount::VARCHAR AS recurring_amount,
        true_up_amount::VARCHAR AS true_up_amount,
        proserv_amount::VARCHAR AS proserv_amount,
        other_non_recurring_amount::VARCHAR AS other_non_recurring_amount,
        upside_iacv::VARCHAR AS upside_iacv,
        upside_swing_deal_iacv::VARCHAR AS upside_swing_deal_iacv,
        is_web_portal_purchase::VARCHAR AS is_web_portal_purchase,
        opportunity_term::VARCHAR AS opportunity_term,
        partner_initiated_opportunity::VARCHAR AS partner_initiated_opportunity,
        user_segment::VARCHAR AS user_segment,
        subscription_start_date::VARCHAR AS subscription_start_date,
        subscription_end_date::VARCHAR AS subscription_end_date,
        true_up_value::VARCHAR AS true_up_value,
        order_type_live::VARCHAR AS order_type_live,
        order_type_stamped::VARCHAR AS order_type_stamped,
        order_type_grouped::VARCHAR AS order_type_grouped,
        growth_type::VARCHAR AS growth_type,
        net_arr::VARCHAR AS net_arr,
        arr_basis::VARCHAR AS arr_basis,
        arr::VARCHAR AS arr,
        days_in_sao::VARCHAR AS days_in_sao,
        new_logo_count::VARCHAR AS new_logo_count,
        user_segment_stamped::VARCHAR AS user_segment_stamped,
        CASE
          WHEN user_segment_stamped IN ('Large', 'PubSec') THEN 'Large'
          ELSE user_segment_stamped
        END                                             AS user_segment_stamped_grouped,
        user_geo_stamped::VARCHAR AS user_geo_stamped,
        user_region_stamped::VARCHAR AS user_region_stamped,
        user_area_stamped::VARCHAR AS user_area_stamped,
        {{ sales_segment_region_grouped('user_segment_stamped', 'user_geo_stamped', 'user_region_stamped') }}
                                                        AS user_segment_region_stamped_grouped,
        user_segment_geo_region_area_stamped::VARCHAR AS user_segment_geo_region_area_stamped,
        crm_opp_owner_user_role_type_stamped::VARCHAR AS crm_opp_owner_user_role_type_stamped,
        crm_opp_owner_stamped_name::VARCHAR AS crm_opp_owner_stamped_name,
        crm_account_owner_stamped_name::VARCHAR AS crm_account_owner_stamped_name,
        sao_crm_opp_owner_stamped_name::VARCHAR AS sao_crm_opp_owner_stamped_name,
        sao_crm_account_owner_stamped_name::VARCHAR AS sao_crm_account_owner_stamped_name,
        sao_crm_opp_owner_sales_segment_stamped::VARCHAR AS sao_crm_opp_owner_sales_segment_stamped,
        sao_crm_opp_owner_sales_segment_geo_region_area_stamped::VARCHAR AS sao_crm_opp_owner_sales_segment_geo_region_area_stamped,
        sao_crm_opp_owner_sales_segment_stamped_grouped::VARCHAR AS sao_crm_opp_owner_sales_segment_stamped_grouped,
        sao_crm_opp_owner_geo_stamped::VARCHAR AS sao_crm_opp_owner_geo_stamped,
        sao_crm_opp_owner_region_stamped::VARCHAR AS sao_crm_opp_owner_region_stamped,
        sao_crm_opp_owner_area_stamped::VARCHAR AS sao_crm_opp_owner_area_stamped,
        {{ sales_segment_region_grouped('sao_crm_opp_owner_sales_segment_stamped', 'sao_crm_opp_owner_geo_stamped', 'sao_crm_opp_owner_region_stamped') }}
                                                        AS sao_crm_opp_owner_segment_region_stamped_grouped,
        opportunity_category::VARCHAR AS opportunity_category,
        opportunity_health::VARCHAR AS opportunity_health,
        risk_type::VARCHAR AS risk_type,
        risk_reasons::VARCHAR AS risk_reasons,
        tam_notes::VARCHAR AS tam_notes,
        primary_solution_architect::VARCHAR AS primary_solution_architect,
        product_details::VARCHAR AS product_details,
        product_category::VARCHAR AS product_category,
        products_purchased::VARCHAR AS products_purchased,
        opportunity_deal_size::VARCHAR AS opportunity_deal_size,
        payment_schedule::VARCHAR AS payment_schedule,
        comp_y2_iacv::VARCHAR AS comp_y2_iacv,
        comp_new_logo_override::VARCHAR AS comp_new_logo_override,
        is_pipeline_created_eligible::VARCHAR AS is_pipeline_created_eligible,

      -- ************************************
      -- sales segmentation deprecated fields - 2020-09-03
      -- left temporary for the sake of MVC and avoid breaking SiSense existing charts
        sales_segment::VARCHAR AS sales_segment,
        parent_segment::VARCHAR AS parent_segment,
      -- ************************************

        -- dates in stage fields
        days_in_0_pending_acceptance::VARCHAR AS days_in_0_pending_acceptance,
        days_in_1_discovery::VARCHAR AS days_in_1_discovery,
        days_in_2_scoping::VARCHAR AS days_in_2_scoping,
        days_in_3_technical_evaluation::VARCHAR AS days_in_3_technical_evaluation,
        days_in_4_proposal::VARCHAR AS days_in_4_proposal,
        days_in_5_negotiating::VARCHAR AS days_in_5_negotiating,

        stage_0_pending_acceptance_date::VARCHAR AS stage_0_pending_acceptance_date,
        stage_1_discovery_date::VARCHAR AS stage_1_discovery_date,
        stage_2_scoping_date::VARCHAR AS stage_2_scoping_date,
        stage_3_technical_evaluation_date::VARCHAR AS stage_3_technical_evaluation_date,
        stage_4_proposal_date::VARCHAR AS stage_4_proposal_date,
        stage_5_negotiating_date::VARCHAR AS stage_5_negotiating_date,
        stage_6_awaiting_signature_date::VARCHAR AS stage_6_awaiting_signature_date,
        stage_6_closed_won_date::VARCHAR AS stage_6_closed_won_date,
        stage_6_closed_lost_date::VARCHAR AS stage_6_closed_lost_date,

        -- sales segment fields
        division_sales_segment_stamped::VARCHAR AS division_sales_segment_stamped,
        -- channel reporting
        -- original issue: https://gitlab.com/gitlab-data/analytics/-/issues/6072
        dr_partner_deal_type::VARCHAR AS dr_partner_deal_type,
        dr_partner_engagement::VARCHAR AS dr_partner_engagement,
        dr_deal_id::VARCHAR AS dr_deal_id,
        dr_primary_registration::VARCHAR AS dr_primary_registration,
        channel_type::VARCHAR AS channel_type,
        partner_account::VARCHAR AS partner_account,
        dr_status::VARCHAR AS dr_status,
        distributor::VARCHAR AS distributor,
        influence_partner::VARCHAR AS influence_partner,
        fulfillment_partner::VARCHAR AS fulfillment_partner,
        platform_partner::VARCHAR AS platform_partner,
        partner_track::VARCHAR AS partner_track,
        resale_partner_track::VARCHAR AS resale_partner_track,
        is_public_sector_opp::VARCHAR AS is_public_sector_opp,
        is_registration_from_portal::VARCHAR AS is_registration_from_portal,
        calculated_discount::VARCHAR AS calculated_discount,
        partner_discount::VARCHAR AS partner_discount,
        partner_discount_calc::VARCHAR AS partner_discount_calc,
        comp_channel_neutral::VARCHAR AS comp_channel_neutral,

        -- command plan fields
        cp_champion::VARCHAR AS cp_champion,
        cp_close_plan::VARCHAR AS cp_close_plan,
        cp_competition::VARCHAR AS cp_competition,
        cp_decision_criteria::VARCHAR AS cp_decision_criteria,
        cp_decision_process::VARCHAR AS cp_decision_process,
        cp_economic_buyer::VARCHAR AS cp_economic_buyer,
        cp_help::VARCHAR AS cp_help,
        cp_identify_pain::VARCHAR AS cp_identify_pain,
        cp_metrics::VARCHAR AS cp_metrics,
        cp_partner::VARCHAR AS cp_partner,
        cp_paper_process::VARCHAR AS cp_paper_process,
        cp_review_notes::VARCHAR AS cp_review_notes,
        cp_risks::VARCHAR AS cp_risks,
        cp_use_cases::VARCHAR AS cp_use_cases,
        cp_value_driver::VARCHAR AS cp_value_driver,
        cp_why_do_anything_at_all::VARCHAR AS cp_why_do_anything_at_all,
        cp_why_gitlab::VARCHAR AS cp_why_gitlab,
        cp_why_now::VARCHAR AS cp_why_now,
        cp_score::VARCHAR AS cp_score,

        -- original issue: https://gitlab.com/gitlab-data/analytics/-/issues/6577
        sa_tech_evaluation_close_status::VARCHAR AS sa_tech_evaluation_close_status,
        sa_tech_evaluation_end_date::VARCHAR AS sa_tech_evaluation_end_date,
        sa_tech_evaluation_start_date::VARCHAR AS sa_tech_evaluation_start_date,

        -- flag to identify eligible booking deals, excluding jihu - issue: https://gitlab.com/gitlab-com/sales-team/field-operations/systems/-/issues/1805
        fpa_master_bookings_flag::VARCHAR AS fpa_master_bookings_flag,

        downgrade_reason::VARCHAR AS downgrade_reason,
        ssp_id::VARCHAR AS ssp_id,
        ga_client_id::VARCHAR AS ga_client_id,

        -- metadata
        _last_dbt_run::VARCHAR AS _last_dbt_run,
        days_since_last_activity::VARCHAR AS days_since_last_activity,
        is_deleted::VARCHAR AS is_deleted,
        last_activity_date::VARCHAR AS last_activity_date,
        record_type_id::VARCHAR AS record_type_id

      FROM source

 )

 SELECT *
 FROM renamed