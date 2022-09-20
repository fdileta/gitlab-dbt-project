{{ config(alias='sfdc_opportunity_snapshot_history_xf_refactored_edm') }}

-- NF 20220907 Moved EDM Refactored model as base model, move original model to a deprecated model

WITH date_details AS (

    SELECT *
    FROM {{ ref('wk_sales_date_details') }}
    -- FROM prod.workspace_sales.date_details

), sfdc_accounts_xf AS (

    SELECT *
    FROM {{ref('wk_sales_sfdc_accounts_xf')}}
    -- FROM PROD.restricted_safe_workspace_sales.sfdc_accounts_xf

), sfdc_opportunity_snapshot_history_legacy AS (

    SELECT *
    FROM {{ref('sfdc_opportunity_snapshot_history')}}
    -- FROM prod.restricted_safe_legacy.sfdc_opportunity_snapshot_history

), edm_snapshot_opty AS (

    SELECT *
    FROM {{ref('mart_crm_opportunity_daily_snapshot')}}

), sfdc_opportunity_xf AS (

    SELECT
      opportunity_id,
      owner_id,
      account_id,
      order_type_stamped,
      deal_category,
      opportunity_category,
      deal_group,
      opportunity_owner_manager,
      is_edu_oss,

      -- Opportunity Owner Stamped fields
      opportunity_owner_user_segment,
      opportunity_owner_user_region,
      opportunity_owner_user_area,
      opportunity_owner_user_geo,

      -------------------
      --  NF 2022-01-28 TO BE DEPRECATED once pipeline velocity reports in Sisense are updated
      sales_team_rd_asm_level,
      sales_team_cro_level,

      -- this fields use the opportunity owner version for current FY and account fields for previous years
      report_opportunity_user_segment,
      report_opportunity_user_geo,
      report_opportunity_user_region,
      report_opportunity_user_area,
      report_user_segment_geo_region_area,
      report_user_segment_geo_region_area_sqs_ot,

      -- NF 2022-02-17 new aggregated keys
      key_sqs,
      key_ot,

      key_segment,
      key_segment_sqs,
      key_segment_ot,

      key_segment_geo,
      key_segment_geo_sqs,
      key_segment_geo_ot,

      key_segment_geo_region,
      key_segment_geo_region_sqs,
      key_segment_geo_region_ot,

      key_segment_geo_region_area,
      key_segment_geo_region_area_sqs,
      key_segment_geo_region_area_ot,

      key_segment_geo_area,

      -------------------------------------
      -- NF: These fields are not exposed yet in opty history, just for check
      -- I am adding this logic

      stage_1_date,
      stage_1_date_month,
      stage_1_fiscal_year,
      stage_1_fiscal_quarter_name,
      stage_1_fiscal_quarter_date,
      --------------------------------------

      is_won,
      is_duplicate_flag,
      raw_net_arr,
      sales_qualified_source,
      net_incremental_acv

    --FROM prod.restricted_safe_workspace_sales.sfdc_opportunity_xf
    FROM {{ref('wk_sales_sfdc_opportunity_xf')}}

), sfdc_users_xf AS (

    SELECT *
    --FROM prod.workspace_sales.sfdc_users_xf
    FROM {{ref('wk_sales_sfdc_users_xf')}}

  
-- CTE to be UPDATED using EDM fields as source
), sfdc_opportunity_snapshot_history AS (

     SELECT

      -- NF 20220907 Pending fields to be added to the edm mart
      sfdc_opportunity_snapshot_history.incremental_acv,
      sfdc_opportunity_snapshot_history.net_incremental_acv,
      sfdc_opportunity_snapshot_history.order_type_stamped          AS snapshot_order_type_stamped,
      sfdc_opportunity_snapshot_history.stage_6_awaiting_signature_date,

      edm_snapshot_opty.crm_opportunity_snapshot_id AS opportunity_snapshot_id,
      edm_snapshot_opty.dim_crm_opportunity_id AS opportunity_id,
      edm_snapshot_opty.opportunity_name,
      edm_snapshot_opty.owner_id,
      edm_snapshot_opty.opportunity_owner_department,

      --------------------------------------------
      --------------------------------------------
      edm_snapshot_opty.close_date,
      edm_snapshot_opty.created_date,
      edm_snapshot_opty.sales_qualified_date,
      edm_snapshot_opty.sales_accepted_date,
      --------------------------------------------
      --------------------------------------------
      edm_snapshot_opty.opportunity_sales_development_representative,
      edm_snapshot_opty.opportunity_business_development_representative,
      edm_snapshot_opty.opportunity_development_representative,
      --------------------------------------------
      --------------------------------------------
      --  NF: For reporting we tend to use live values for things like order type
      --      exposing some of those fields here in case they are needed

      edm_snapshot_opty.sales_qualified_source_name                 AS snapshot_sales_qualified_source,
      edm_snapshot_opty.is_edu_oss                                  AS snapshot_is_edu_oss,
      edm_snapshot_opty.opportunity_category                        AS snapshot_opportunity_category,

      -- Accounts might get deleted or merged, I am selecting the latest account id from the opty object
      -- to avoid showing non-valid account ids
      edm_snapshot_opty.dim_crm_account_id                          AS raw_account_id,
      -- edm_snapshot_opty.net_arr                                     AS raw_net_arr,
      edm_snapshot_opty.raw_net_arr,
      edm_snapshot_opty.deployment_preference,
      edm_snapshot_opty.merged_opportunity_id,
      edm_snapshot_opty.sales_path,
      edm_snapshot_opty.sales_type,
      edm_snapshot_opty.stage_name,
      edm_snapshot_opty.competitors,
      edm_snapshot_opty.forecast_category_name,
      edm_snapshot_opty.invoice_number,
      edm_snapshot_opty.primary_campaign_source_id,
      edm_snapshot_opty.professional_services_value,
      edm_snapshot_opty.total_contract_value,
      edm_snapshot_opty.is_web_portal_purchase,
      edm_snapshot_opty.opportunity_term,
      edm_snapshot_opty.arr_basis,
      edm_snapshot_opty.arr,
      edm_snapshot_opty.amount,
      edm_snapshot_opty.recurring_amount,
      edm_snapshot_opty.true_up_amount,
      edm_snapshot_opty.proserv_amount,
      edm_snapshot_opty.renewal_amount,
      edm_snapshot_opty.other_non_recurring_amount,
      edm_snapshot_opty.subscription_start_date                         AS quote_start_date,
      edm_snapshot_opty.subscription_end_date                           AS quote_end_date,

      edm_snapshot_opty.cp_champion,
      edm_snapshot_opty.cp_close_plan,
      edm_snapshot_opty.cp_competition,
      edm_snapshot_opty.cp_decision_criteria,
      edm_snapshot_opty.cp_decision_process,
      edm_snapshot_opty.cp_economic_buyer,
      edm_snapshot_opty.cp_identify_pain,
      edm_snapshot_opty.cp_metrics,
      edm_snapshot_opty.cp_risks,
      edm_snapshot_opty.cp_use_cases,
      edm_snapshot_opty.cp_value_driver,
      edm_snapshot_opty.cp_why_do_anything_at_all,
      edm_snapshot_opty.cp_why_gitlab,
      edm_snapshot_opty.cp_why_now,
      edm_snapshot_opty.cp_score,

      edm_snapshot_opty.dbt_updated_at AS _last_dbt_run,
      edm_snapshot_opty.is_deleted,
      edm_snapshot_opty.last_activity_date,

      -- Channel Org. fields
      -- this fields should be changed to this historical version
      edm_snapshot_opty.deal_path_name                                  AS deal_path,
      edm_snapshot_opty.dr_partner_deal_type,
      edm_snapshot_opty.dr_partner_engagement,
      edm_snapshot_opty.partner_account,
      edm_snapshot_opty.dr_status,
      edm_snapshot_opty.distributor,
      edm_snapshot_opty.influence_partner,
      edm_snapshot_opty.fulfillment_partner,
      edm_snapshot_opty.platform_partner,
      edm_snapshot_opty.partner_track,
      edm_snapshot_opty.is_public_sector_opp,
      edm_snapshot_opty.is_registration_from_portal,
      edm_snapshot_opty.calculated_discount,
      edm_snapshot_opty.partner_discount,
      edm_snapshot_opty.partner_discount_calc,
      edm_snapshot_opty.comp_channel_neutral,
      edm_snapshot_opty.fpa_master_bookings_flag,

      -- stage dates
      -- dates in stage fields
      edm_snapshot_opty.stage_0_pending_acceptance_date,
      edm_snapshot_opty.stage_1_discovery_date,
      edm_snapshot_opty.stage_2_scoping_date,
      edm_snapshot_opty.stage_3_technical_evaluation_date,
      edm_snapshot_opty.stage_4_proposal_date,
      edm_snapshot_opty.stage_5_negotiating_date,
 
      edm_snapshot_opty.stage_6_closed_won_date,
      edm_snapshot_opty.stage_6_closed_lost_date,

      CASE
        WHEN edm_snapshot_opty.deal_path_name = 'Direct'
          THEN 'Direct'
        WHEN edm_snapshot_opty.deal_path_name = 'Web Direct'
          THEN 'Web Direct'
        WHEN edm_snapshot_opty.deal_path_name = 'Channel'
            AND edm_snapshot_opty.sales_qualified_source_name = 'Channel Generated'
          THEN 'Partner Sourced'
        WHEN edm_snapshot_opty.deal_path_name = 'Channel'
            AND edm_snapshot_opty.sales_qualified_source_name != 'Channel Generated'
          THEN 'Partner Co-Sell'
      END                                                         AS deal_path_engagement,

      ------------------------------------------------------------------------------------------------------
      ------------------------------------------------------------------------------------------------------
      -- Base helpers for reporting
      CASE
        WHEN edm_snapshot_opty.stage_name IN ('00-Pre Opportunity', '0-Pending Acceptance', '0-Qualifying'
                              ,'Developing', '1-Discovery', '2-Developing', '2-Scoping')
          THEN 'Pipeline'
        WHEN edm_snapshot_opty.stage_name IN ('3-Technical Evaluation', '4-Proposal', '5-Negotiating'
                              , '6-Awaiting Signature', '7-Closing')
          THEN '3+ Pipeline'
        WHEN edm_snapshot_opty.stage_name IN ('8-Closed Lost', 'Closed Lost')
          THEN 'Lost'
        WHEN edm_snapshot_opty.stage_name IN ('Closed Won')
          THEN 'Closed Won'
        ELSE 'Other'
      END                                                         AS stage_name_3plus,

      CASE
        WHEN edm_snapshot_opty.stage_name IN ('00-Pre Opportunity', '0-Pending Acceptance', '0-Qualifying'
                            , 'Developing', '1-Discovery', '2-Developing', '2-Scoping', '3-Technical Evaluation')
          THEN 'Pipeline'
        WHEN edm_snapshot_opty.stage_name IN ('4-Proposal', '5-Negotiating', '6-Awaiting Signature', '7-Closing')
          THEN '4+ Pipeline'
        WHEN edm_snapshot_opty.stage_name IN ('8-Closed Lost', 'Closed Lost')
          THEN 'Lost'
        WHEN edm_snapshot_opty.stage_name IN ('Closed Won')
          THEN 'Closed Won'
        ELSE 'Other'
      END                                                         AS stage_name_4plus,


      CASE
        WHEN edm_snapshot_opty.stage_name
          IN ('1-Discovery', '2-Developing', '2-Scoping','3-Technical Evaluation', '4-Proposal', 'Closed Won','5-Negotiating', '6-Awaiting Signature', '7-Closing')
            THEN 1
        ELSE 0
      END                                                         AS is_stage_1_plus,

      CASE
        WHEN edm_snapshot_opty.stage_name
          IN ('3-Technical Evaluation', '4-Proposal', 'Closed Won','5-Negotiating', '6-Awaiting Signature', '7-Closing')
            THEN 1
        ELSE 0
      END                                                         AS is_stage_3_plus,

      CASE
        WHEN edm_snapshot_opty.stage_name
          IN ('4-Proposal', 'Closed Won','5-Negotiating', '6-Awaiting Signature', '7-Closing')
            THEN 1
        ELSE 0
      END                                                         AS is_stage_4_plus,

      CASE
        WHEN edm_snapshot_opty.stage_name = 'Closed Won'
          THEN 1 ELSE 0
      END                                                         AS is_won,

      CASE
        WHEN edm_snapshot_opty.stage_name IN ('8-Closed Lost', 'Closed Lost')
          THEN 1 ELSE 0
      END                                                         AS is_lost,

      CASE
        WHEN edm_snapshot_opty.stage_name IN ('8-Closed Lost', 'Closed Lost', '9-Unqualified', 'Closed Won', '10-Duplicate')
            THEN 0
        ELSE 1
      END                                                         AS is_open,

      CASE
        WHEN edm_snapshot_opty.stage_name IN ('8-Closed Lost', 'Closed Lost', '9-Unqualified', 'Closed Won', '10-Duplicate')
          THEN 1
        ELSE 0
      END                                                         AS is_closed,


      CASE
        WHEN LOWER(edm_snapshot_opty.sales_type) like '%renewal%'
          THEN 1
        ELSE 0
      END                                                         AS is_renewal,

      CASE
        WHEN edm_snapshot_opty.opportunity_category IN ('Credit')
          THEN 1
        ELSE 0
      END                                                         AS is_credit_flag,

      CASE
        WHEN edm_snapshot_opty.opportunity_category IN ('Decommission')
          THEN 1
        ELSE 0
      END                                                          AS is_refund,


      CASE
        WHEN edm_snapshot_opty.opportunity_category IN ('Contract Reset')
          THEN 1
        ELSE 0
      END                                                          AS is_contract_reset_flag,

      -- NF: 20210827 Fields for competitor analysis
      CASE
        WHEN CONTAINS (edm_snapshot_opty.competitors, 'Other')
          THEN 1
        ELSE 0
      END                                 AS competitors_other_flag,
      CASE
        WHEN CONTAINS (edm_snapshot_opty.competitors, 'GitLab Core')
          THEN 1
        ELSE 0
      END                                 AS competitors_gitlab_core_flag,
      CASE
        WHEN CONTAINS (edm_snapshot_opty.competitors, 'None')
          THEN 1
        ELSE 0
      END                                 AS competitors_none_flag,
      CASE
        WHEN CONTAINS (edm_snapshot_opty.competitors, 'GitHub Enterprise')
          THEN 1
        ELSE 0
      END                                 AS competitors_github_enterprise_flag,
      CASE
        WHEN CONTAINS (edm_snapshot_opty.competitors, 'BitBucket Server')
          THEN 1
        ELSE 0
      END                                 AS competitors_bitbucket_server_flag,
      CASE
        WHEN CONTAINS (edm_snapshot_opty.competitors, 'Unknown')
          THEN 1
        ELSE 0
      END                                 AS competitors_unknown_flag,
      CASE
        WHEN CONTAINS (edm_snapshot_opty.competitors, 'GitHub.com')
          THEN 1
        ELSE 0
      END                                 AS competitors_github_flag,
      CASE
        WHEN CONTAINS (edm_snapshot_opty.competitors, 'GitLab.com')
          THEN 1
        ELSE 0
      END                                 AS competitors_gitlab_flag,
      CASE
        WHEN CONTAINS (edm_snapshot_opty.competitors, 'Jenkins')
          THEN 1
        ELSE 0
      END                                 AS competitors_jenkins_flag,
      CASE
        WHEN CONTAINS (edm_snapshot_opty.competitors, 'Azure DevOps')
          THEN 1
        ELSE 0
      END                                 AS competitors_azure_devops_flag,
      CASE
        WHEN CONTAINS (edm_snapshot_opty.competitors, 'SVN')
          THEN 1
        ELSE 0
      END                                 AS competitors_svn_flag,
      CASE
        WHEN CONTAINS (edm_snapshot_opty.competitors, 'BitBucket.Org')
          THEN 1
        ELSE 0
      END                                 AS competitors_bitbucket_flag,
      CASE
        WHEN CONTAINS (edm_snapshot_opty.competitors, 'Atlassian')
          THEN 1
        ELSE 0
      END                                 AS competitors_atlassian_flag,
      CASE
        WHEN CONTAINS (edm_snapshot_opty.competitors, 'Perforce')
          THEN 1
        ELSE 0
      END                                 AS competitors_perforce_flag,
      CASE
        WHEN CONTAINS (edm_snapshot_opty.competitors, 'Visual Studio Team Services')
          THEN 1
        ELSE 0
      END                                 AS competitors_visual_studio_flag,
      CASE
        WHEN CONTAINS (edm_snapshot_opty.competitors, 'Azure')
          THEN 1
        ELSE 0
      END                                 AS competitors_azure_flag,
      CASE
        WHEN CONTAINS (edm_snapshot_opty.competitors, 'Amazon Code Commit')
          THEN 1
        ELSE 0
      END                                 AS competitors_amazon_code_commit_flag,
      CASE
        WHEN CONTAINS (edm_snapshot_opty.competitors, 'CircleCI')
          THEN 1
        ELSE 0
      END                                 AS competitors_circleci_flag,
      CASE
        WHEN CONTAINS (edm_snapshot_opty.competitors, 'Bamboo')
          THEN 1
        ELSE 0
      END                                 AS competitors_bamboo_flag,
      CASE
        WHEN CONTAINS (edm_snapshot_opty.competitors, 'AWS')
          THEN 1
        ELSE 0
      END                                 AS competitors_aws_flag,

    CASE
        WHEN edm_snapshot_opty.stage_name = 'Closed Won'
          THEN '1.Won'
        WHEN edm_snapshot_opty.stage_name IN ('8-Closed Lost', 'Closed Lost')
          THEN '2.Lost'
        WHEN edm_snapshot_opty.stage_name NOT IN ('8-Closed Lost', 'Closed Lost', '9-Unqualified', 'Closed Won', '10-Duplicate')
          THEN '0. Open'
        ELSE 'N/A'
      END                                                         AS stage_category,

      ------------------------------
      -- fields for counting new logos, these fields count refund as negative
      CASE
        WHEN edm_snapshot_opty.opportunity_category IN ('Decommission')
          THEN -1
        WHEN edm_snapshot_opty.opportunity_category IN ('Credit')
          THEN 0
        ELSE 1
      END                                                          AS calculated_deal_count,


      -- calculated age field
      -- if open, use the diff between created date and snapshot date
      -- if closed, a) the close date is later than snapshot date, use snapshot date
      -- if closed, b) the close is in the past, use close date
      CASE
        WHEN is_open = 1
          THEN DATEDIFF(days, created_date_detail.date_actual, snapshot_date.date_actual)
        WHEN is_open = 0 AND snapshot_date.date_actual < close_date_detail.date_actual
          THEN DATEDIFF(days, created_date_detail.date_actual, snapshot_date.date_actual)
        ELSE DATEDIFF(days, created_date_detail.date_actual, close_date_detail.date_actual)
      END                                                       AS calculated_age_in_days,

      ------------------------------------------------------------------------------------------------------
      ------------------------------------------------------------------------------------------------------
      --date helpers

      edm_snapshot_opty.snapshot_date,
      --sfdc_opportunity_snapshot_history.date_actual::DATE         AS snapshot_date,
      snapshot_date.first_day_of_month                            AS snapshot_date_month,
      snapshot_date.fiscal_year                                   AS snapshot_fiscal_year,
      snapshot_date.fiscal_quarter_name_fy                        AS snapshot_fiscal_quarter_name,
      snapshot_date.first_day_of_fiscal_quarter                   AS snapshot_fiscal_quarter_date,
      snapshot_date.day_of_fiscal_quarter_normalised              AS snapshot_day_of_fiscal_quarter_normalised,
      snapshot_date.day_of_fiscal_year_normalised                 AS snapshot_day_of_fiscal_year_normalised,

      close_date_detail.first_day_of_month                        AS close_date_month,
      close_date_detail.fiscal_year                               AS close_fiscal_year,
      close_date_detail.fiscal_quarter_name_fy                    AS close_fiscal_quarter_name,
      close_date_detail.first_day_of_fiscal_quarter               AS close_fiscal_quarter_date,

      -- This refers to the closing quarter perspective instead of the snapshot quarter
      90 - DATEDIFF(day, snapshot_date.date_actual, close_date_detail.last_day_of_fiscal_quarter)           AS close_day_of_fiscal_quarter_normalised,

      created_date_detail.first_day_of_month                      AS created_date_month,
      created_date_detail.fiscal_year                             AS created_fiscal_year,
      created_date_detail.fiscal_quarter_name_fy                  AS created_fiscal_quarter_name,
      created_date_detail.first_day_of_fiscal_quarter             AS created_fiscal_quarter_date,

      created_date_detail.date_actual                             AS net_arr_created_date,
      created_date_detail.first_day_of_month                      AS net_arr_created_date_month,
      created_date_detail.fiscal_year                             AS net_arr_created_fiscal_year,
      created_date_detail.fiscal_quarter_name_fy                  AS net_arr_created_fiscal_quarter_name,
      created_date_detail.first_day_of_fiscal_quarter             AS net_arr_created_fiscal_quarter_date,

      net_arr_created_date.date_actual                            AS pipeline_created_date,
      net_arr_created_date.first_day_of_month                     AS pipeline_created_date_month,
      net_arr_created_date.fiscal_year                            AS pipeline_created_fiscal_year,
      net_arr_created_date.fiscal_quarter_name_fy                 AS pipeline_created_fiscal_quarter_name,
      net_arr_created_date.first_day_of_fiscal_quarter            AS pipeline_created_fiscal_quarter_date,

      sales_accepted_date.first_day_of_month                      AS sales_accepted_month,
      sales_accepted_date.fiscal_year                             AS sales_accepted_fiscal_year,
      sales_accepted_date.fiscal_quarter_name_fy                  AS sales_accepted_fiscal_quarter_name,
      sales_accepted_date.first_day_of_fiscal_quarter             AS sales_accepted_fiscal_quarter_date

    FROM sfdc_opportunity_snapshot_history_legacy AS sfdc_opportunity_snapshot_history
    INNER JOIN edm_snapshot_opty
      ON edm_snapshot_opty.dim_crm_opportunity_id = sfdc_opportunity_snapshot_history.opportunity_id
        AND edm_snapshot_opty.snapshot_date = sfdc_opportunity_snapshot_history.date_actual::DATE
    INNER JOIN date_details AS close_date_detail
      ON close_date_detail.date_actual = sfdc_opportunity_snapshot_history.close_date::DATE
    INNER JOIN date_details AS snapshot_date
      ON sfdc_opportunity_snapshot_history.date_actual::DATE = snapshot_date.date_actual
    LEFT JOIN date_details AS created_date_detail
      ON created_date_detail.date_actual = sfdc_opportunity_snapshot_history.created_date::DATE
    LEFT JOIN date_details AS net_arr_created_date
      ON net_arr_created_date.date_actual = sfdc_opportunity_snapshot_history.iacv_created_date::DATE
    LEFT JOIN date_details AS sales_accepted_date
      ON sales_accepted_date.date_actual = sfdc_opportunity_snapshot_history.sales_accepted_date::DATE


), net_iacv_to_net_arr_ratio AS (

    SELECT '2. New - Connected'     AS "ORDER_TYPE_STAMPED",
          'Mid-Market'              AS "USER_SEGMENT_STAMPED",
          0.999691784               AS "RATIO_NET_IACV_TO_NET_ARR"
    UNION
    SELECT '1. New - First Order'   AS "ORDER_TYPE_STAMPED",
          'SMB'                     AS "USER_SEGMENT_STAMPED",
          0.998590143               AS "RATIO_NET_IACV_TO_NET_ARR"
    UNION
    SELECT '1. New - First Order'   AS "ORDER_TYPE_STAMPED",
          'Large'                   AS "USER_SEGMENT_STAMPED",
          0.992289340               AS "RATIO_NET_IACV_TO_NET_ARR"
    UNION
    SELECT '3. Growth'              AS "ORDER_TYPE_STAMPED",
          'SMB'                     AS "USER_SEGMENT_STAMPED",
          0.927846192               AS "RATIO_NET_IACV_TO_NET_ARR"
    UNION
    SELECT '3. Growth'              AS "ORDER_TYPE_STAMPED",
          'Large'                   AS "USER_SEGMENT_STAMPED",
          0.852915435               AS "RATIO_NET_IACV_TO_NET_ARR"
    UNION
    SELECT '2. New - Connected'     AS "ORDER_TYPE_STAMPED",
          'SMB'                     AS "USER_SEGMENT_STAMPED",
          1.009262672               AS "RATIO_NET_IACV_TO_NET_ARR"
    UNION
    SELECT '3. Growth'              AS "ORDER_TYPE_STAMPED",
          'Mid-Market'              AS "USER_SEGMENT_STAMPED",
          0.793618079               AS "RATIO_NET_IACV_TO_NET_ARR"
    UNION
    SELECT '1. New - First Order'   AS "ORDER_TYPE_STAMPED",
          'Mid-Market'              AS "USER_SEGMENT_STAMPED",
          0.988527875               AS "RATIO_NET_IACV_TO_NET_ARR"
    UNION
    SELECT '2. New - Connected'     AS "ORDER_TYPE_STAMPED",
          'Large'                   AS "USER_SEGMENT_STAMPED",
          1.010081083               AS "RATIO_NET_IACV_TO_NET_ARR"
    UNION
    SELECT '1. New - First Order'   AS "ORDER_TYPE_STAMPED",
          'PubSec'                  AS "USER_SEGMENT_STAMPED",
          1.000000000               AS "RATIO_NET_IACV_TO_NET_ARR"
    UNION
    SELECT '2. New - Connected'     AS "ORDER_TYPE_STAMPED",
          'PubSec'                  AS "USER_SEGMENT_STAMPED",
          1.002741689               AS "RATIO_NET_IACV_TO_NET_ARR"
    UNION
    SELECT '3. Growth'              AS "ORDER_TYPE_STAMPED",
          'PubSec'                  AS "USER_SEGMENT_STAMPED",
          0.965670500               AS "RATIO_NET_IACV_TO_NET_ARR"

), sfdc_opportunity_snapshot_history_xf AS (

  SELECT DISTINCT

      opp_snapshot.*,
      ------------------------------------------------------------------------------------------------------
      ------------------------------------------------------------------------------------------------------
      -- Historical Net ARR Logic Summary
      -- closed deals use net_incremental_acv
      -- open deals use incremental acv
      -- closed won deals with net_arr > 0 use that opportunity calculated ratio
      -- deals with no opty with net_arr use a default ratio for segment / order type
      -- deals before 2021-02-01 use always net_arr calculated from ratio
      -- deals after 2021-02-01 use net_arr if > 0, if open and not net_arr uses ratio version

      -- If the opportunity exists, use the ratio from the opportunity sheetload
      -- I am faking that using the opportunity table directly
      CASE
        WHEN sfdc_opportunity_xf.is_won = 1 -- only consider won deals
          AND sfdc_opportunity_xf.opportunity_category <> 'Contract Reset' -- contract resets have a special way of calculating net iacv
          AND COALESCE(sfdc_opportunity_xf.raw_net_arr,0) <> 0
          AND COALESCE(sfdc_opportunity_xf.net_incremental_acv,0) <> 0
            THEN COALESCE(sfdc_opportunity_xf.raw_net_arr / sfdc_opportunity_xf.net_incremental_acv,0)
        ELSE NULL
      END                                                                     AS opportunity_based_iacv_to_net_arr_ratio,

      -- If there is no opportnity, use a default table ratio
      -- I am faking that using the upper CTE, that should be replaced by the official table
      COALESCE(net_iacv_to_net_arr_ratio.ratio_net_iacv_to_net_arr,0)         AS segment_order_type_iacv_to_net_arr_ratio,

      -- calculated net_arr
      -- uses ratios to estimate the net_arr based on iacv if open or net_iacv if closed
      -- if there is an opportunity based ratio, use that, if not, use default from segment / order type

      -- NUANCE: Lost deals might not have net_incremental_acv populated, so we must rely on iacv
      -- Using opty ratio for open deals doesn't seem to work well
      CASE
        WHEN opp_snapshot.stage_name NOT IN ('8-Closed Lost', '9-Unqualified', 'Closed Won', '10-Duplicate')  -- OPEN DEAL
            THEN COALESCE(opp_snapshot.incremental_acv,0) * COALESCE(segment_order_type_iacv_to_net_arr_ratio,0)
        WHEN opp_snapshot.stage_name IN ('8-Closed Lost')                       -- CLOSED LOST DEAL and no Net IACV
          AND COALESCE(opp_snapshot.net_incremental_acv,0) = 0
            THEN COALESCE(opp_snapshot.incremental_acv,0) * COALESCE(segment_order_type_iacv_to_net_arr_ratio,0)
        WHEN opp_snapshot.stage_name IN ('8-Closed Lost', 'Closed Won')         -- REST of CLOSED DEAL
            THEN COALESCE(opp_snapshot.net_incremental_acv,0) * COALESCE(opportunity_based_iacv_to_net_arr_ratio,segment_order_type_iacv_to_net_arr_ratio)
        ELSE NULL
      END                                                                     AS calculated_from_ratio_net_arr,

      -- For opportunities before start of FY22, as Net ARR was WIP, there are a lot of opties with IACV or Net IACV and no Net ARR
      -- Those were later fixed in the opportunity object but stayed in the snapshot table.
      -- To account for those issues and give a directionally correct answer, we apply a ratio to everything before FY22
      CASE
        WHEN  opp_snapshot.snapshot_date < '2021-02-01'::DATE -- All deals before cutoff and that were not updated to Net ARR
          THEN calculated_from_ratio_net_arr
        WHEN  opp_snapshot.snapshot_date >= '2021-02-01'::DATE  -- After cutoff date, for all deals earlier than FY19 that are closed and have no net arr
              AND opp_snapshot.close_date < '2018-02-01'::DATE
              AND opp_snapshot.is_open = 0
              AND COALESCE(opp_snapshot.raw_net_arr,0) = 0
          THEN calculated_from_ratio_net_arr
        ELSE COALESCE(opp_snapshot.raw_net_arr,0) -- Rest of deals after cut off date
      END                                                                     AS net_arr,

      ------------------------------------------------------------------------------------------------------
      ------------------------------------------------------------------------------------------------------
      -- opportunity driven fields

      sfdc_opportunity_xf.opportunity_owner_manager,
      sfdc_opportunity_xf.is_edu_oss,
      sfdc_opportunity_xf.sales_qualified_source,
      sfdc_opportunity_xf.account_id,
      sfdc_opportunity_xf.opportunity_category,


      CASE
        WHEN sfdc_opportunity_xf.stage_1_date <= opp_snapshot.snapshot_date
          THEN sfdc_opportunity_xf.stage_1_date
        ELSE NULL
      END                                               AS stage_1_date,

      CASE
        WHEN sfdc_opportunity_xf.stage_1_date <= opp_snapshot.snapshot_date
          THEN sfdc_opportunity_xf.stage_1_date_month
        ELSE NULL
      END                                               AS stage_1_date_month,

      CASE
        WHEN sfdc_opportunity_xf.stage_1_date <= opp_snapshot.snapshot_date
          THEN sfdc_opportunity_xf.stage_1_fiscal_year
        ELSE NULL
      END                                               AS stage_1_fiscal_year,

      CASE
        WHEN sfdc_opportunity_xf.stage_1_date <= opp_snapshot.snapshot_date
          THEN sfdc_opportunity_xf.stage_1_fiscal_quarter_name
        ELSE NULL
      END                                               AS stage_1_fiscal_quarter_name,

      CASE
        WHEN sfdc_opportunity_xf.stage_1_date <= opp_snapshot.snapshot_date
          THEN sfdc_opportunity_xf.stage_1_fiscal_quarter_date
        ELSE NULL
      END                                               AS stage_1_fiscal_quarter_date,

      ------------------------------------------------------------------------------------------------------
      ------------------------------------------------------------------------------------------------------
      -- DEPRECATED IACV METRICS
      -- Use Net ARR instead
      CASE
        WHEN opp_snapshot.pipeline_created_fiscal_quarter_name= opp_snapshot.close_fiscal_quarter_name
          AND opp_snapshot.is_won = 1
            THEN opp_snapshot.incremental_acv
        ELSE 0
      END                                                         AS created_and_won_same_quarter_iacv,

      -- created within quarter
      CASE
        WHEN opp_snapshot.pipeline_created_fiscal_quarter_name = opp_snapshot.snapshot_fiscal_quarter_name
          THEN opp_snapshot.incremental_acv
        ELSE 0
      END                                                         AS created_in_snapshot_quarter_iacv,

      -- field used for FY21 bookings reporitng

      -- Team Segment / ASM - RD
      -- As the snapshot history table is used to compare current perspective with the past, I leverage the most recent version
      -- of the truth ato cut the data, that's why instead of using the stampped version, I take the current fields.
      -- https://gitlab.my.salesforce.com/00N6100000ICcrD?setupid=OpportunityFields

      /*

      FY23 - NF 2022-01-28

      At this point I still think the best is to keep taking the owner / account demographics cuts from the most recent version of the opportunity object.

      The snapshot history at this point is mainly used to track how current performance compares with previous quarters and years
      and to do that effectively the patches / territories must be the same. Any error that is corrected in the future should be incorporated
      into the overview

      */

      sfdc_opportunity_xf.opportunity_owner_user_segment,
      sfdc_opportunity_xf.opportunity_owner_user_region,
      sfdc_opportunity_xf.opportunity_owner_user_area,
      sfdc_opportunity_xf.opportunity_owner_user_geo,

      --- target fields for reporting, changing their name might help to isolate their logic from the actual field
      -------------------
      --  NF 2022-01-28 TO BE DEPRECATED once pipeline velocity reports in Sisense are updated
      sfdc_opportunity_xf.sales_team_rd_asm_level,
      sfdc_opportunity_xf.sales_team_cro_level,

      -- this fields use the opportunity owner version for current FY and account fields for previous years
      sfdc_opportunity_xf.report_opportunity_user_segment,
      sfdc_opportunity_xf.report_opportunity_user_geo,
      sfdc_opportunity_xf.report_opportunity_user_region,
      sfdc_opportunity_xf.report_opportunity_user_area,

      -- NF 2022-02-17 new aggregated keys
      sfdc_opportunity_xf.report_user_segment_geo_region_area,
      sfdc_opportunity_xf.report_user_segment_geo_region_area_sqs_ot,

      sfdc_opportunity_xf.key_sqs,
      sfdc_opportunity_xf.key_ot,

      sfdc_opportunity_xf.key_segment,
      sfdc_opportunity_xf.key_segment_sqs,
      sfdc_opportunity_xf.key_segment_ot,

      sfdc_opportunity_xf.key_segment_geo,
      sfdc_opportunity_xf.key_segment_geo_sqs,
      sfdc_opportunity_xf.key_segment_geo_ot,

      sfdc_opportunity_xf.key_segment_geo_region,
      sfdc_opportunity_xf.key_segment_geo_region_sqs,
      sfdc_opportunity_xf.key_segment_geo_region_ot,

      sfdc_opportunity_xf.key_segment_geo_region_area,
      sfdc_opportunity_xf.key_segment_geo_region_area_sqs,
      sfdc_opportunity_xf.key_segment_geo_region_area_ot,

      sfdc_opportunity_xf.key_segment_geo_area,

      -- using current opportunity perspective instead of historical
      sfdc_opportunity_xf.order_type_stamped,

      -- top level grouping of the order type field
      sfdc_opportunity_xf.deal_group,

      -- medium level grouping of the order type field
      sfdc_opportunity_xf.deal_category,

      -- duplicates flag
      sfdc_opportunity_xf.is_duplicate_flag                               AS current_is_duplicate_flag,

      -- the owner name in the opportunity is not clean.
      opportunity_owner.name AS opportunity_owner,

      ------------------------------------------------------------------------------------------------------
      ------------------------------------------------------------------------------------------------------
      -- account driven fields
      sfdc_accounts_xf.account_name,
      sfdc_accounts_xf.ultimate_parent_account_id,
      upa.account_name                        AS ultimate_parent_account_name,
      sfdc_accounts_xf.is_jihu_account,
      sfdc_accounts_xf.ultimate_parent_id,

      sfdc_accounts_xf.account_owner_user_segment,
      sfdc_accounts_xf.account_owner_user_geo,
      sfdc_accounts_xf.account_owner_user_region,
      sfdc_accounts_xf.account_owner_user_area,
      -- account_owner_subarea_stamped

      sfdc_accounts_xf.account_demographics_sales_segment AS account_demographics_segment,
      sfdc_accounts_xf.account_demographics_geo,
      sfdc_accounts_xf.account_demographics_region,
      sfdc_accounts_xf.account_demographics_area,
      sfdc_accounts_xf.account_demographics_territory,
      -- account_demographics_subarea_stamped

      upa.account_demographics_sales_segment    AS upa_demographics_segment,
      upa.account_demographics_geo              AS upa_demographics_geo,
      upa.account_demographics_region           AS upa_demographics_region,
      upa.account_demographics_area             AS upa_demographics_area,
      upa.account_demographics_territory        AS upa_demographics_territory


    FROM sfdc_opportunity_snapshot_history opp_snapshot
    INNER JOIN sfdc_opportunity_xf
      ON sfdc_opportunity_xf.opportunity_id = opp_snapshot.opportunity_id
    LEFT JOIN sfdc_accounts_xf
      ON sfdc_opportunity_xf.account_id = sfdc_accounts_xf.account_id
    LEFT JOIN sfdc_accounts_xf upa
      ON upa.account_id = sfdc_accounts_xf.ultimate_parent_account_id
    LEFT JOIN sfdc_users_xf account_owner
      ON account_owner.user_id = sfdc_accounts_xf.owner_id
    LEFT JOIN sfdc_users_xf opportunity_owner
      ON opportunity_owner.user_id = opp_snapshot.owner_id
    -- Net IACV to Net ARR conversion table
    LEFT JOIN net_iacv_to_net_arr_ratio
      ON net_iacv_to_net_arr_ratio.user_segment_stamped = sfdc_opportunity_xf.opportunity_owner_user_segment
      AND net_iacv_to_net_arr_ratio.order_type_stamped = sfdc_opportunity_xf.order_type_stamped
    WHERE opp_snapshot.raw_account_id NOT IN ('0014M00001kGcORQA0')                           -- remove test account
      AND (sfdc_accounts_xf.ultimate_parent_account_id NOT IN ('0016100001YUkWVAA1')
            OR sfdc_accounts_xf.account_id IS NULL)                                        -- remove test account
      AND opp_snapshot.is_deleted = 0
      -- NF 20210906 remove JiHu opties from the models
      AND sfdc_accounts_xf.is_jihu_account = 0

)
-- in Q2 FY21 a few deals where created in the wrong stage, and as they were purely aspirational,
-- they needed to be removed from stage 1, eventually by the end of the quarter they were removed
-- The goal of this list is to use in the Created Pipeline flag, to exclude those deals that at
-- day 90 had stages of less than 1, that should smooth the chart
, vision_opps  AS (

      SELECT opp_snapshot.opportunity_id,
             opp_snapshot.stage_name,
             opp_snapshot.snapshot_fiscal_quarter_date
      FROM sfdc_opportunity_snapshot_history_xf opp_snapshot
      WHERE opp_snapshot.snapshot_fiscal_quarter_name = 'FY21-Q2'
        And opp_snapshot.pipeline_created_fiscal_quarter_date = opp_snapshot.snapshot_fiscal_quarter_date
        AND opp_snapshot.snapshot_day_of_fiscal_quarter_normalised = 90
        AND opp_snapshot.stage_name in ('00-Pre Opportunity', '0-Pending Acceptance', '0-Qualifying')
      GROUP BY 1, 2, 3


), add_compound_metrics AS (

    SELECT
      opp_snapshot.*,

      ------------------------------
      -- compound metrics for reporting
      ------------------------------
      -- current deal size field, it was creasted by the data team and the original doesn't work
      CASE
        WHEN opp_snapshot.net_arr > 0 AND net_arr < 5000
          THEN '1 - Small (<5k)'
        WHEN opp_snapshot.net_arr >=5000 AND net_arr < 25000
          THEN '2 - Medium (5k - 25k)'
        WHEN opp_snapshot.net_arr >=25000 AND net_arr < 100000
          THEN '3 - Big (25k - 100k)'
        WHEN opp_snapshot.net_arr >= 100000
          THEN '4 - Jumbo (>100k)'
        ELSE 'Other'
      END                                                          AS deal_size,

      -- extended version of the deal size
      CASE
        WHEN net_arr > 0 AND net_arr < 1000
          THEN '1. (0k -1k)'
        WHEN net_arr >=1000 AND net_arr < 10000
          THEN '2. (1k - 10k)'
        WHEN net_arr >=10000 AND net_arr < 50000
          THEN '3. (10k - 50k)'
        WHEN net_arr >=50000 AND net_arr < 100000
          THEN '4. (50k - 100k)'
        WHEN net_arr >= 100000 AND net_arr < 250000
          THEN '5. (100k - 250k)'
        WHEN net_arr >= 250000 AND net_arr < 500000
          THEN '6. (250k - 500k)'
        WHEN net_arr >= 500000 AND net_arr < 1000000
          THEN '7. (500k-1000k)'
        WHEN net_arr >= 1000000
          THEN '8. (>1000k)'
        ELSE 'Other'
      END                                                           AS calculated_deal_size,

      -- Open pipeline eligibility definition
      CASE
        WHEN lower(opp_snapshot.deal_group) LIKE ANY ('%growth%', '%new%')
          AND opp_snapshot.is_edu_oss = 0
          AND opp_snapshot.is_stage_1_plus = 1
          AND opp_snapshot.forecast_category_name != 'Omitted'
          AND opp_snapshot.is_open = 1
         THEN 1
         ELSE 0
      END                                                   AS is_eligible_open_pipeline_flag,


      -- Created pipeline eligibility definition
      -- https://gitlab.com/gitlab-com/sales-team/field-operations/systems/-/issues/2389
      CASE
        WHEN opp_snapshot.order_type_stamped IN ('1. New - First Order' ,'2. New - Connected', '3. Growth')
          AND opp_snapshot.is_edu_oss = 0
          AND opp_snapshot.pipeline_created_fiscal_quarter_date IS NOT NULL
          AND opp_snapshot.opportunity_category IN ('Standard','Internal Correction','Ramp Deal','Credit','Contract Reset')
          AND opp_snapshot.stage_name NOT IN ('00-Pre Opportunity','10-Duplicate', '9-Unqualified','0-Pending Acceptance')
          AND (opp_snapshot.net_arr > 0
            OR opp_snapshot.opportunity_category = 'Credit')
          -- exclude vision opps from FY21-Q2
          AND (opp_snapshot.pipeline_created_fiscal_quarter_name != 'FY21-Q2'
                OR vision_opps.opportunity_id IS NULL)
          -- 20220128 Updated to remove webdirect SQS deals
          AND opp_snapshot.sales_qualified_source  != 'Web Direct Generated'
              THEN 1
         ELSE 0
      END                                                      AS is_eligible_created_pipeline_flag,

      -- SAO alignment issue: https://gitlab.com/gitlab-com/sales-team/field-operations/sales-operations/-/issues/2656
      -- 2022-08-23 JK: using the central is_sao logic
      CASE
        WHEN opp_snapshot.sales_accepted_date IS NOT NULL
          AND opp_snapshot.is_edu_oss = 0
          AND opp_snapshot.stage_name NOT IN ('10-Duplicate')
            THEN 1
        ELSE 0
      END                                                     AS is_eligible_sao_flag,

      -- ASP Analysis eligibility issue: https://gitlab.com/gitlab-com/sales-team/field-operations/sales-operations/-/issues/2606
      CASE
        WHEN opp_snapshot.is_edu_oss = 0
          AND opp_snapshot.is_deleted = 0
          -- For ASP we care mainly about add on, new business, excluding contraction / churn
          AND opp_snapshot.order_type_stamped IN ('1. New - First Order','2. New - Connected','3. Growth')
          -- Exclude Decomissioned as they are not aligned to the real owner
          -- Contract Reset, Decomission
          AND opp_snapshot.opportunity_category IN ('Standard','Ramp Deal','Internal Correction')
          -- Exclude Deals with nARR < 0
          AND net_arr > 0
          -- Not JiHu
            THEN 1
          ELSE 0
      END                                                    AS is_eligible_asp_analysis_flag,

      -- Age eligibility issue: https://gitlab.com/gitlab-com/sales-team/field-operations/sales-operations/-/issues/2606
      CASE
        WHEN opp_snapshot.is_edu_oss = 0
          AND opp_snapshot.is_deleted = 0
          -- Renewals are not having the same motion as rest of deals
          AND opp_snapshot.is_renewal = 0
          -- For stage age we exclude only ps/other
          AND opp_snapshot.order_type_stamped IN ('1. New - First Order','2. New - Connected','3. Growth','4. Contraction','6. Churn - Final','5. Churn - Partial')
          -- Only include deal types with meaningful journeys through the stages
          AND opp_snapshot.opportunity_category IN ('Standard','Ramp Deal','Decommissioned')
          -- Web Purchase have a different dynamic and should not be included
          AND opp_snapshot.is_web_portal_purchase = 0
          -- Not JiHu
            THEN 1
          ELSE 0
      END                                                   AS is_eligible_age_analysis_flag,

      -- TODO: This is the same as FP&A Boookings Flag
      CASE
        WHEN opp_snapshot.is_edu_oss = 0
          AND opp_snapshot.is_deleted = 0
          AND (opp_snapshot.is_won = 1
              OR (opp_snapshot.is_renewal = 1 AND opp_snapshot.is_lost = 1))
          AND opp_snapshot.order_type_stamped IN ('1. New - First Order','2. New - Connected','3. Growth','4. Contraction','6. Churn - Final','5. Churn - Partial')
          -- Not JiHu
            THEN 1
          ELSE 0
      END                                                   AS is_booked_net_arr_flag,

      CASE
        WHEN opp_snapshot.is_edu_oss = 0
          AND opp_snapshot.is_deleted = 0
          AND opp_snapshot.order_type_stamped IN ('4. Contraction','6. Churn - Final','5. Churn - Partial')
          -- Not JiHu
            THEN 1
          ELSE 0
      END                                                  AS is_eligible_churn_contraction_flag,

      -- created within quarter
      CASE
        WHEN opp_snapshot.pipeline_created_fiscal_quarter_name = opp_snapshot.snapshot_fiscal_quarter_name
          AND is_eligible_created_pipeline_flag = 1
            THEN opp_snapshot.net_arr
        ELSE 0
      END                                                 AS created_in_snapshot_quarter_net_arr,

   -- created and closed within the quarter net arr
      CASE
        WHEN opp_snapshot.pipeline_created_fiscal_quarter_name = opp_snapshot.close_fiscal_quarter_name
           AND is_won = 1
           AND is_eligible_created_pipeline_flag = 1
            THEN opp_snapshot.net_arr
        ELSE 0
      END                                                 AS created_and_won_same_quarter_net_arr,


      CASE
        WHEN opp_snapshot.pipeline_created_fiscal_quarter_name = opp_snapshot.snapshot_fiscal_quarter_name
          AND is_eligible_created_pipeline_flag = 1
            THEN opp_snapshot.calculated_deal_count
        ELSE 0
      END                                                 AS created_in_snapshot_quarter_deal_count,

      ---------------------------------------------------------------------------------------------------------
      ---------------------------------------------------------------------------------------------------------
      -- Fields created to simplify report building down the road. Specially the pipeline velocity.

      -- deal count
      CASE
        WHEN is_eligible_open_pipeline_flag = 1
          AND opp_snapshot.is_stage_1_plus = 1
            THEN opp_snapshot.calculated_deal_count
        ELSE 0
      END                                               AS open_1plus_deal_count,

      CASE
        WHEN is_eligible_open_pipeline_flag = 1
          AND opp_snapshot.is_stage_3_plus = 1
            THEN opp_snapshot.calculated_deal_count
        ELSE 0
      END                                               AS open_3plus_deal_count,

      CASE
        WHEN is_eligible_open_pipeline_flag = 1
          AND opp_snapshot.is_stage_4_plus = 1
            THEN opp_snapshot.calculated_deal_count
        ELSE 0
      END                                               AS open_4plus_deal_count,

      -- booked deal count
      CASE
        WHEN opp_snapshot.is_won = 1
          THEN opp_snapshot.calculated_deal_count
        ELSE 0
      END                                               AS booked_deal_count,

      -- churned contraction deal count as OT
      CASE
        WHEN ((opp_snapshot.is_renewal = 1
            AND opp_snapshot.is_lost = 1)
            OR opp_snapshot.is_won = 1 )
            AND opp_snapshot.order_type_stamped IN ('5. Churn - Partial' ,'6. Churn - Final', '4. Contraction')
        THEN opp_snapshot.calculated_deal_count
        ELSE 0
      END                                               AS churned_contraction_deal_count,

      -----------------
      -- Net ARR

      CASE
        WHEN is_eligible_open_pipeline_flag = 1
          THEN opp_snapshot.net_arr
        ELSE 0
      END                                                AS open_1plus_net_arr,

      CASE
        WHEN is_eligible_open_pipeline_flag = 1
          AND opp_snapshot.is_stage_3_plus = 1
            THEN opp_snapshot.net_arr
        ELSE 0
      END                                                AS open_3plus_net_arr,

      CASE
        WHEN is_eligible_open_pipeline_flag = 1
          AND opp_snapshot.is_stage_4_plus = 1
            THEN opp_snapshot.net_arr
        ELSE 0
      END                                                AS open_4plus_net_arr,

      -- booked net arr (won + renewals / lost)
      CASE
        WHEN (opp_snapshot.is_won = 1
            OR (opp_snapshot.is_renewal = 1
                  AND opp_snapshot.is_lost = 1))
          THEN opp_snapshot.net_arr
        ELSE 0
      END                                                 AS booked_net_arr,

      -- churned contraction deal count as OT
      CASE
        WHEN ((opp_snapshot.is_renewal = 1
            AND opp_snapshot.is_lost = 1)
            OR opp_snapshot.is_won = 1 )
            AND opp_snapshot.order_type_stamped IN ('5. Churn - Partial' ,'6. Churn - Final', '4. Contraction')
        THEN net_arr
        ELSE 0
      END                                                 AS churned_contraction_net_arr,

      -- 20201021 NF: This should be replaced by a table that keeps track of excluded deals for forecasting purposes
      CASE
        WHEN opp_snapshot.ultimate_parent_id IN ('001610000111bA3','0016100001F4xla','0016100001CXGCs','00161000015O9Yn','0016100001b9Jsc')
          AND opp_snapshot.close_date < '2020-08-01'
            THEN 1
        -- NF 2021 - Pubsec extreme deals
        WHEN opp_snapshot.opportunity_id IN ('0064M00000WtZKUQA3','0064M00000Xb975QAB')
          AND opp_snapshot.snapshot_date < '2021-05-01'
          THEN 1
        -- exclude vision opps from FY21-Q2
        WHEN opp_snapshot.pipeline_created_fiscal_quarter_name = 'FY21-Q2'
                AND vision_opps.opportunity_id IS NOT NULL
          THEN 1
        -- NF 20220415 PubSec duplicated deals on Pipe Gen -- Lockheed Martin GV - 40000 Ultimate Renewal
        WHEN opp_snapshot.opportunity_id IN ('0064M00000ZGpfQQAT','0064M00000ZGpfVQAT','0064M00000ZGpfGQAT')
          THEN 1
        ELSE 0
      END                                                         AS is_excluded_flag

    FROM sfdc_opportunity_snapshot_history_xf opp_snapshot
      LEFT JOIN vision_opps
        ON vision_opps.opportunity_id = opp_snapshot.opportunity_id
        AND vision_opps.snapshot_fiscal_quarter_date = opp_snapshot.snapshot_fiscal_quarter_date
)

SELECT *
FROM add_compound_metrics
