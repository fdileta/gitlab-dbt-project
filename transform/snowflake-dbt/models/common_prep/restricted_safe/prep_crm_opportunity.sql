{{ simple_cte([
    ('sfdc_user_roles_source','sfdc_user_roles_source'),
    ('net_iacv_to_net_arr_ratio', 'net_iacv_to_net_arr_ratio'),
    ('dim_date', 'dim_date')
]) }}

--- union live and snapshot sources with a 

, first_contact  AS (

    SELECT
      opportunity_id,                                                             -- opportunity_id
      contact_id                                                                  AS sfdc_contact_id,
      md5(cast(coalesce(cast(contact_id as varchar), '') as varchar))             AS dim_crm_person_id,
      ROW_NUMBER() OVER (PARTITION BY opportunity_id ORDER BY created_date ASC)   AS row_num
    FROM {{ ref('sfdc_opportunity_contact_role_source')}}

), attribution_touchpoints AS (

    SELECT *
    FROM {{ ref('sfdc_bizible_attribution_touchpoint_source') }}
    WHERE is_deleted = 'FALSE'

), linear_attribution_base AS ( --the number of attribution touches a given opp has in total
    --linear attribution IACV of an opp / all touches (count_touches) for each opp - weighted by the number of touches in the given bucket (campaign,channel,etc)
    SELECT
     opportunity_id                                         AS dim_crm_opportunity_id,
     COUNT(DISTINCT attribution_touchpoints.touchpoint_id)  AS count_crm_attribution_touchpoints
    FROM  attribution_touchpoints
    GROUP BY 1

), campaigns_per_opp as (

    SELECT
      opportunity_id                                        AS dim_crm_opportunity_id,
      COUNT(DISTINCT attribution_touchpoints.campaign_id)   AS count_campaigns
    FROM attribution_touchpoints
    GROUP BY 1

), sfdc_opportunity_stage AS (

    SELECT *
    FROM {{ref('sfdc_opportunity_stage_source')}}

), sfdc_opportunity_base AS (

    SELECT
      account_id                                                         AS dim_crm_account_id,
      opportunity_id                                                     AS dim_crm_opportunity_id,
      owner_id                                                           AS dim_crm_user_id,
      order_type_stamped                                                 AS order_type,
      opportunity_term                                                   AS opportunity_term_base,
      {{ sales_qualified_source_cleaning('sales_qualified_source') }}    AS sales_qualified_source,
      user_segment_stamped                                               AS crm_opp_owner_sales_segment_stamped,
      user_geo_stamped                                                   AS crm_opp_owner_geo_stamped,
      user_region_stamped                                                AS crm_opp_owner_region_stamped,
      user_area_stamped                                                  AS crm_opp_owner_area_stamped,
      user_segment_geo_region_area_stamped                               AS crm_opp_owner_sales_segment_geo_region_area_stamped,
      created_date::DATE                                                 AS created_date,
      sales_accepted_date::DATE                                          AS sales_accepted_date,
      close_date::DATE                                                   AS close_date,
      net_arr                                                            AS raw_net_arr,
      CASE
        WHEN sfdc_opportunity_source.stage_name IN ('8-Closed Lost', 'Closed Lost', '9-Unqualified', 'Closed Won', '10-Duplicate')
            THEN 0
        ELSE 1
      END                                                                                         AS is_open,
        {{ dbt_utils.star(from=ref('sfdc_opportunity_source'), except=["ACCOUNT_ID", "OPPORTUNITY_ID", "OWNER_ID", "ORDER_TYPE_STAMPED", "IS_WON", "ORDER_TYPE", "OPPORTUNITY_TERM","SALES_QUALIFIED_SOURCE", "DBT_UPDATED_AT", "CREATED_DATE", "SALES_ACCEPTED_DATE", "CLOSE_DATE", "NET_ARR", "DEAL_SIZE"])}}
    FROM {{ref('sfdc_opportunity_source')}}
    WHERE account_id IS NOT NULL
      AND is_deleted = FALSE

), sfdc_zqu_quote_source AS (

    SELECT *
    FROM {{ ref('sfdc_zqu_quote_source') }}
    WHERE is_deleted = FALSE

), quote AS (

    SELECT DISTINCT
      sfdc_zqu_quote_source.zqu__opportunity                AS dim_crm_opportunity_id,
      sfdc_zqu_quote_source.zqu_quote_id                    AS dim_quote_id,
      sfdc_zqu_quote_source.zqu__start_date::DATE           AS quote_start_date,
      (ROW_NUMBER() OVER (PARTITION BY sfdc_zqu_quote_source.zqu__opportunity ORDER BY sfdc_zqu_quote_source.created_date DESC))
                                                            AS record_number
    FROM sfdc_zqu_quote_source
    INNER JOIN sfdc_opportunity_base
      ON sfdc_zqu_quote_source.zqu__opportunity = sfdc_opportunity_base.dim_crm_opportunity_id
    WHERE stage_name IN ('Closed Won', '8-Closed Lost')
      AND zqu__primary = TRUE
    QUALIFY record_number = 1

), sfdc_account AS (

    SELECT *
    FROM {{ ref('sfdc_account_source') }}
    WHERE account_id IS NOT NULL

), sfdc_user AS (

    SELECT *
    FROM {{ ref('sfdc_users_source') }}
    WHERE user_id IS NOT NULL

), sfdc_opportunity AS (


    SELECT 
      sfdc_opportunity_base.*,
      close_date.first_day_of_fiscal_quarter AS close_fiscal_quarter_date,
      -- account owner information
      account_owner.user_segment AS crm_account_owner_sales_segment,
      account_owner.user_geo AS crm_account_owner_geo,
      account_owner.user_region AS crm_account_owner_region,
      account_owner.user_area AS crm_account_owner_area,
      account_owner.user_segment_geo_region_area AS crm_account_owner_sales_segment_geo_region_area,
      -- opportunity owner information
      CASE
        WHEN sfdc_opportunity_base.user_segment_stamped IS NULL
          OR sfdc_opportunity_base.is_open = 1
          THEN account_owner.user_segment
        ELSE sfdc_opportunity_base.user_segment_stamped
      END AS opportunity_owner_user_segment,
      CASE
        WHEN sfdc_opportunity_base.user_geo_stamped IS NULL
            OR sfdc_opportunity_base.is_open = 1
          THEN account_owner.user_geo
        ELSE sfdc_opportunity_base.user_geo_stamped
      END AS opportunity_owner_user_geo,
      CASE
        WHEN sfdc_opportunity_base.user_region_stamped IS NULL
             OR sfdc_opportunity_base.is_open = 1
          THEN account_owner.user_region
          ELSE sfdc_opportunity_base.user_region_stamped
      END AS opportunity_owner_user_region,
      CASE
        WHEN sfdc_opportunity_base.user_area_stamped IS NULL
             OR sfdc_opportunity_base.is_open = 1
          THEN account_owner.user_area
        ELSE sfdc_opportunity_base.user_area_stamped
      END AS opportunity_owner_user_area,
      sfdc_user_roles_source.name AS opportunity_owner_role,
      account_owner.title  AS opportunity_owner_title,
      LOWER(
        CASE
          WHEN sfdc_opportunity_base.close_date < close_date.current_first_day_of_fiscal_year
            THEN account_owner.user_segment
          ELSE opportunity_owner_user_segment
        END
      ) AS report_opportunity_user_segment,
      LOWER(
        CASE
          WHEN sfdc_opportunity_base.close_date < close_date.current_first_day_of_fiscal_year
            THEN account_owner.user_geo
          ELSE opportunity_owner_user_geo
        END
      ) AS report_opportunity_user_geo,
      LOWER(
        CASE
          WHEN sfdc_opportunity_base.close_date < close_date.current_first_day_of_fiscal_year
            THEN account_owner.user_region
          ELSE opportunity_owner_user_region
        END
      ) AS report_opportunity_user_region,
      LOWER(
        CASE
          WHEN sfdc_opportunity_base.close_date < close_date.current_first_day_of_fiscal_year
            THEN account_owner.user_area
          ELSE opportunity_owner_user_area
        END
      ) AS report_opportunity_user_area,
      LOWER(
        CONCAT(
          report_opportunity_user_segment,'-',report_opportunity_user_geo,'-',report_opportunity_user_region,'-',report_opportunity_user_area
        )
      ) AS report_user_segment_geo_region_area
    FROM sfdc_opportunity_base
    LEFT JOIN dim_date AS close_date
      ON sfdc_opportunity_base.close_date = close_date.date_actual
    LEFT JOIN sfdc_account
      ON sfdc_opportunity_base.dim_crm_account_id= sfdc_account.account_id
    LEFT JOIN sfdc_user AS account_owner
      ON sfdc_account.owner_id = account_owner.user_id
    LEFT JOIN sfdc_user_roles_source
      ON account_owner.user_role_id = sfdc_user_roles_source.id

), final AS (

    SELECT
      -- opportunity information
      sfdc_opportunity.*,

      -- dates & date ids
      {{ get_date_id('sfdc_opportunity.created_date') }}                                          AS created_date_id,
      {{ get_date_id('sfdc_opportunity.sales_accepted_date') }}                                   AS sales_accepted_date_id,
      {{ get_date_id('sfdc_opportunity.close_date') }}                                            AS close_date_id,
      {{ get_date_id('sfdc_opportunity.stage_0_pending_acceptance_date') }}                       AS stage_0_pending_acceptance_date_id,
      {{ get_date_id('sfdc_opportunity.stage_1_discovery_date') }}                                AS stage_1_discovery_date_id,
      {{ get_date_id('sfdc_opportunity.stage_2_scoping_date') }}                                  AS stage_2_scoping_date_id,
      {{ get_date_id('sfdc_opportunity.stage_3_technical_evaluation_date') }}                     AS stage_3_technical_evaluation_date_id,
      {{ get_date_id('sfdc_opportunity.stage_4_proposal_date') }}                                 AS stage_4_proposal_date_id,
      {{ get_date_id('sfdc_opportunity.stage_5_negotiating_date') }}                              AS stage_5_negotiating_date_id,
      {{ get_date_id('sfdc_opportunity.stage_6_awaiting_signature_date') }}                       AS stage_6_awaiting_signature_date_id,
      {{ get_date_id('sfdc_opportunity.stage_6_closed_won_date') }}                               AS stage_6_closed_won_date_id,
      {{ get_date_id('sfdc_opportunity.stage_6_closed_lost_date') }}                              AS stage_6_closed_lost_date_id,
      {{ get_date_id('sfdc_opportunity.technical_evaluation_date') }}                             AS technical_evaluation_date_id,
      {{ get_date_id('sfdc_opportunity.last_activity_date') }}                                    AS last_activity_date_id,
      {{ get_date_id('sfdc_opportunity.subscription_start_date') }}                               AS subscription_start_date_id,
      {{ get_date_id('sfdc_opportunity.subscription_end_date') }}                                 AS subscription_end_date_id,
      {{ get_date_id('sfdc_opportunity.sales_qualified_date') }}                                  AS sales_qualified_date_id,

      {{ get_date_id('sfdc_opportunity.iacv_created_date')}}                                      AS arr_created_date_id,
      sfdc_opportunity.iacv_created_date                                                          AS arr_created_date,
      arr_created_date.fiscal_quarter_name_fy                                                     AS arr_created_fiscal_quarter_name,
      arr_created_date.first_day_of_fiscal_quarter                                                AS arr_created_fiscal_quarter_date,

      subscription_start_date.fiscal_quarter_name_fy                                              AS subscription_start_date_fiscal_quarter_name,
      subscription_start_date.first_day_of_fiscal_quarter                                         AS subscription_start_date_fiscal_quarter_date,

      COALESCE(net_iacv_to_net_arr_ratio.ratio_net_iacv_to_net_arr,0)                             AS segment_order_type_iacv_to_net_arr_ratio,

      -- net arr
      -- calculated net_arr
      -- uses ratios to estimate the net_arr based on iacv if open or net_iacv if closed
      -- NUANCE: Lost deals might not have net_incremental_acv populated, so we must rely on iacv
      -- Using opty ratio for open deals doesn't seem to work well
      CASE
        WHEN sfdc_opportunity.stage_name NOT IN ('8-Closed Lost', '9-Unqualified', 'Closed Won', '10-Duplicate')  -- OPEN DEAL
            THEN COALESCE(sfdc_opportunity.incremental_acv,0) * COALESCE(segment_order_type_iacv_to_net_arr_ratio,0)
        WHEN sfdc_opportunity.stage_name IN ('8-Closed Lost')                       -- CLOSED LOST DEAL and no Net IACV
          AND COALESCE(sfdc_opportunity.net_incremental_acv,0) = 0
           THEN COALESCE(sfdc_opportunity.incremental_acv,0) * COALESCE(segment_order_type_iacv_to_net_arr_ratio,0)
        WHEN sfdc_opportunity.stage_name IN ('8-Closed Lost', 'Closed Won')         -- REST of CLOSED DEAL
            THEN COALESCE(sfdc_opportunity.net_incremental_acv,0) * COALESCE(segment_order_type_iacv_to_net_arr_ratio,0)
        ELSE NULL
      END                                                                     AS calculated_from_ratio_net_arr,

      -- Calculated NET ARR is only used for deals closed earlier than FY19 and that have no raw_net_arr
      CASE
        WHEN sfdc_opportunity.close_date::DATE < '2018-02-01'
              AND COALESCE(sfdc_opportunity.raw_net_arr,0) = 0
          THEN calculated_from_ratio_net_arr
        ELSE COALESCE(sfdc_opportunity.raw_net_arr,0) -- Rest of deals after cut off date
      END                                                                     AS net_arr,

      -- opportunity flags
      CASE
        WHEN (sfdc_opportunity.days_in_stage > 30
          OR sfdc_opportunity.incremental_acv > 100000
          OR sfdc_opportunity.pushed_count > 0)
          THEN TRUE
          ELSE FALSE
      END                                                                                         AS is_risky,
      CASE
        WHEN sfdc_opportunity.opportunity_term_base IS NULL THEN
          DATEDIFF('month', quote.quote_start_date, sfdc_opportunity.subscription_end_date)
        ELSE sfdc_opportunity.opportunity_term_base
      END                                                                                         AS opportunity_term,
      -- opportunity stage information
      sfdc_opportunity_stage.is_active                                                            AS is_active,
      sfdc_opportunity_stage.is_won                                                               AS is_won,
      CASE
        WHEN sfdc_opportunity.stage_name
          IN ('1-Discovery', '2-Developing', '2-Scoping','3-Technical Evaluation', '4-Proposal', 'Closed Won','5-Negotiating', '6-Awaiting Signature', '7-Closing')
        THEN 1
        ELSE 0
      END                                                                                         AS is_stage_1_plus,
      CASE
        WHEN sfdc_opportunity.stage_name
          IN ('3-Technical Evaluation', '4-Proposal', 'Closed Won','5-Negotiating', '6-Awaiting Signature', '7-Closing')
            THEN 1
        ELSE 0
      END                                                                                         AS is_stage_3_plus,
      CASE
        WHEN sfdc_opportunity.stage_name
          IN ('4-Proposal', 'Closed Won','5-Negotiating', '6-Awaiting Signature', '7-Closing')
            THEN 1
        ELSE 0
      END                                                                                         AS is_stage_4_plus,
      CASE
        WHEN sfdc_opportunity.stage_name IN ('8-Closed Lost', 'Closed Lost')
          THEN 1 ELSE 0
      END                                                                                         AS is_lost,
      CASE
        WHEN LOWER(sfdc_opportunity.sales_type) like '%renewal%'
          THEN 1
        ELSE 0
      END                                                                                         AS is_renewal,
      CASE
        WHEN sfdc_opportunity.opportunity_category IN ('Decommission')
          THEN 1
        ELSE 0
      END                                                                                         AS is_decommissed,
      CASE
        WHEN sfdc_opportunity.sales_accepted_date IS NOT NULL
          AND sfdc_opportunity.is_edu_oss = 0
          AND sfdc_opportunity.stage_name != '10-Duplicate'
            THEN TRUE
        ELSE FALSE
      END                                                                                         AS is_sao,
      CASE
        WHEN is_sao = TRUE
          AND sfdc_opportunity.sales_qualified_source IN (
                                        'SDR Generated'
                                        , 'BDR Generated'
                                        )
            THEN TRUE
        ELSE FALSE
      END                                                                                         AS is_sdr_sao,
      CASE
        WHEN (
               (sfdc_opportunity.sales_type = 'Renewal' AND sfdc_opportunity.stage_name = '8-Closed Lost')
                 OR sfdc_opportunity.stage_name = 'Closed Won'
              )
            AND sfdc_account.is_jihu_account = FALSE
          THEN TRUE
        ELSE FALSE
      END                                                                                         AS is_net_arr_closed_deal,
      CASE
        WHEN (sfdc_opportunity.new_logo_count = 1
          OR sfdc_opportunity.new_logo_count = -1
          )
          AND sfdc_account.is_jihu_account = FALSE
          THEN TRUE
        ELSE FALSE
      END                                                                                         AS is_new_logo_first_order,
      sfdc_opportunity.is_pipeline_created_eligible                                               AS is_net_arr_pipeline_created,
      CASE
        WHEN sfdc_opportunity_stage.is_closed = TRUE
          AND sfdc_opportunity.amount >= 0
          AND (sfdc_opportunity.reason_for_loss IS NULL OR sfdc_opportunity.reason_for_loss != 'Merged into another opportunity')
          AND sfdc_opportunity.is_edu_oss = 0
            THEN TRUE
        ELSE FALSE
      END                                                                                         AS is_win_rate_calc,
      CASE
        WHEN sfdc_opportunity_stage.is_won = 'TRUE'
          AND sfdc_opportunity.is_closed = 'TRUE'
          AND sfdc_opportunity.is_edu_oss = 0
            THEN TRUE
        ELSE FALSE
      END                                                                                         AS is_closed_won,
      CASE
        WHEN LOWER(sfdc_opportunity.order_type_grouped) LIKE ANY ('%growth%', '%new%')
          AND sfdc_opportunity.is_edu_oss = 0
          AND is_stage_1_plus = 1
          AND sfdc_opportunity.forecast_category_name != 'Omitted'
          AND sfdc_opportunity.is_open = 1
         THEN 1
         ELSE 0
      END                                                                                         AS is_eligible_open_pipeline,
      CASE
        WHEN sfdc_opportunity.sales_accepted_date IS NOT NULL
          AND sfdc_opportunity.is_edu_oss = 0
          AND sfdc_opportunity.is_deleted = 0
            THEN 1
        ELSE 0
      END                                                                                         AS is_eligible_sao,
      CASE
        WHEN sfdc_opportunity.is_edu_oss = 0
          AND sfdc_opportunity.is_deleted = 0
          -- For ASP we care mainly about add on, new business, excluding contraction / churn
          AND sfdc_opportunity.order_type IN ('1. New - First Order','2. New - Connected','3. Growth')
          -- Exclude Decomissioned as they are not aligned to the real owner
          -- Contract Reset, Decomission
          AND sfdc_opportunity.opportunity_category IN ('Standard','Ramp Deal','Internal Correction')
          -- Exclude Deals with nARR < 0
          AND net_arr > 0
            THEN 1
          ELSE 0
      END                                                           AS is_eligible_asp_analysis,
      CASE
        WHEN sfdc_opportunity.is_edu_oss = 0
          AND sfdc_opportunity.is_deleted = 0
          AND is_renewal = 0
          AND sfdc_opportunity.order_type IN ('1. New - First Order','2. New - Connected','3. Growth','4. Contraction','6. Churn - Final','5. Churn - Partial')
          AND sfdc_opportunity.opportunity_category IN ('Standard','Ramp Deal','Decommissioned')
          AND sfdc_opportunity.is_web_portal_purchase = 0
            THEN 1
          ELSE 0
      END                                                                                         AS is_eligible_age_analysis,
      CASE
        WHEN sfdc_opportunity.is_edu_oss = 0
          AND sfdc_opportunity.is_deleted = 0
          AND (sfdc_opportunity_stage.is_won = 1
              OR (is_renewal = 1 AND is_lost = 1))
          AND sfdc_opportunity.order_type IN ('1. New - First Order','2. New - Connected','3. Growth','4. Contraction','6. Churn - Final','5. Churn - Partial')
            THEN 1
          ELSE 0
      END                                                                                         AS is_eligible_net_arr,
      CASE
        WHEN sfdc_opportunity.is_edu_oss = 0
          AND sfdc_opportunity.is_deleted = 0
          AND sfdc_opportunity.order_type IN ('4. Contraction','6. Churn - Final','5. Churn - Partial')
            THEN 1
          ELSE 0
      END                                                                                         AS is_eligible_churn_contraction,
      CASE
        WHEN sfdc_opportunity.stage_name IN ('10-Duplicate')
            THEN 1
        ELSE 0
      END                                                                                         AS is_duplicate,
      CASE
        WHEN sfdc_opportunity.opportunity_category IN ('Credit')
          THEN 1
        ELSE 0
      END                                                                                         AS is_credit,
      CASE
        WHEN sfdc_opportunity.opportunity_category IN ('Contract Reset')
          THEN 1
        ELSE 0
      END                                                                                         AS is_contract_reset,

      -- alliance type fields

      {{ alliance_partner_current('fulfillment_partner.account_name', 'partner_account.account_name', 'sfdc_opportunity.partner_track', 'sfdc_opportunity.resale_partner_track', 'sfdc_opportunity.deal_path') }} AS alliance_type_current,
      {{ alliance_partner_short_current('fulfillment_partner.account_name', 'partner_account.account_name', 'sfdc_opportunity.partner_track', 'sfdc_opportunity.resale_partner_track', 'sfdc_opportunity.deal_path') }} AS alliance_type_short_current,

      {{ alliance_partner('fulfillment_partner.account_name', 'partner_account.account_name', 'sfdc_opportunity.close_date', 'sfdc_opportunity.partner_track', 'sfdc_opportunity.resale_partner_track', 'sfdc_opportunity.deal_path') }} AS alliance_type,
      {{ alliance_partner_short('fulfillment_partner.account_name', 'partner_account.account_name', 'sfdc_opportunity.close_date', 'sfdc_opportunity.partner_track', 'sfdc_opportunity.resale_partner_track', 'sfdc_opportunity.deal_path') }} AS alliance_type_short,

      fulfillment_partner.account_name AS resale_partner_name,

      --  quote information
      quote.dim_quote_id,
      quote.quote_start_date,

      -- contact information
      first_contact.dim_crm_person_id,
      first_contact.sfdc_contact_id,

      -- attribution information
      linear_attribution_base.count_crm_attribution_touchpoints,
      campaigns_per_opp.count_campaigns,
      sfdc_opportunity.incremental_acv/linear_attribution_base.count_crm_attribution_touchpoints   AS weighted_linear_iacv,

     -- opportunity attributes
      CASE
        WHEN sfdc_opportunity.days_in_sao < 0                  THEN '1. Closed in < 0 days'
        WHEN sfdc_opportunity.days_in_sao BETWEEN 0 AND 30     THEN '2. Closed in 0-30 days'
        WHEN sfdc_opportunity.days_in_sao BETWEEN 31 AND 60    THEN '3. Closed in 31-60 days'
        WHEN sfdc_opportunity.days_in_sao BETWEEN 61 AND 90    THEN '4. Closed in 61-90 days'
        WHEN sfdc_opportunity.days_in_sao BETWEEN 91 AND 180   THEN '5. Closed in 91-180 days'
        WHEN sfdc_opportunity.days_in_sao BETWEEN 181 AND 270  THEN '6. Closed in 181-270 days'
        WHEN sfdc_opportunity.days_in_sao > 270                THEN '7. Closed in > 270 days'
        ELSE NULL
      END                                                                                         AS closed_buckets,
      CASE
        WHEN net_arr > -5000
            AND is_eligible_churn_contraction = 1
          THEN '1. < 5k'
        WHEN net_arr > -20000
          AND net_arr <= -5000
          AND is_eligible_churn_contraction = 1
          THEN '2. 5k-20k'
        WHEN net_arr > -50000
          AND net_arr <= -20000
          AND is_eligible_churn_contraction = 1
          THEN '3. 20k-50k'
        WHEN net_arr > -100000
          AND net_arr <= -50000
          AND is_eligible_churn_contraction = 1
          THEN '4. 50k-100k'
        WHEN net_arr < -100000
          AND is_eligible_churn_contraction = 1
          THEN '5. 100k+'
      END                                                 AS churn_contraction_net_arr_bucket,
      CASE
        WHEN sfdc_opportunity.created_date < '2022-02-01'
          THEN 'Legacy'
        WHEN sfdc_opportunity.opportunity_sales_development_representative IS NOT NULL AND sfdc_opportunity.opportunity_business_development_representative IS NOT NULL
          THEN 'SDR & BDR'
        WHEN sfdc_opportunity.opportunity_sales_development_representative IS NOT NULL
          THEN 'SDR'
        WHEN sfdc_opportunity.opportunity_business_development_representative IS NOT NULL
          THEN 'BDR'
        WHEN sfdc_opportunity.opportunity_business_development_representative IS NULL AND sfdc_opportunity.opportunity_sales_development_representative IS NULL
          THEN 'No XDR Assigned'
      END                                               AS sdr_or_bdr,
      CASE
        WHEN sfdc_opportunity_stage.is_won = 1
          THEN '1.Won'
        WHEN is_lost = 1
          THEN '2.Lost'
        WHEN sfdc_opportunity.is_open = 1
          THEN '0. Open'
        ELSE 'N/A'
      END                                                                                         AS stage_category,
      CASE
       WHEN sfdc_opportunity.order_type = '1. New - First Order'
         THEN '1. New'
       WHEN sfdc_opportunity.order_type IN ('2. New - Connected', '3. Growth', '5. Churn - Partial','6. Churn - Final','4. Contraction')
         THEN '2. Growth'
       ELSE '3. Other'
     END                                                                   AS deal_group,
     CASE
       WHEN sfdc_opportunity.order_type = '1. New - First Order'
         THEN '1. New'
       WHEN sfdc_opportunity.order_type IN ('2. New - Connected', '3. Growth')
         THEN '2. Growth'
       WHEN sfdc_opportunity.order_type IN ('4. Contraction')
         THEN '3. Contraction'
       WHEN sfdc_opportunity.order_type IN ('5. Churn - Partial','6. Churn - Final')
         THEN '4. Churn'
       ELSE '5. Other'
      END                                                                                       AS deal_category,
      COALESCE(sfdc_opportunity.reason_for_loss, sfdc_opportunity.downgrade_reason)               AS reason_for_loss_staged,
      CASE
        WHEN reason_for_loss_staged IN ('Do Nothing','Other','Competitive Loss','Operational Silos')
          OR reason_for_loss_staged IS NULL
          THEN 'Unknown'
        WHEN reason_for_loss_staged IN ('Missing Feature','Product value/gaps','Product Value / Gaps',
                                          'Stayed with Community Edition','Budget/Value Unperceived')
          THEN 'Product Value / Gaps'
        WHEN reason_for_loss_staged IN ('Lack of Engagement / Sponsor','Went Silent','Evangelist Left')
          THEN 'Lack of Engagement / Sponsor'
        WHEN reason_for_loss_staged IN ('Loss of Budget','No budget')
          THEN 'Loss of Budget'
        WHEN reason_for_loss_staged = 'Merged into another opportunity'
          THEN 'Merged Opp'
        WHEN reason_for_loss_staged = 'Stale Opportunity'
          THEN 'No Progression - Auto-close'
        WHEN reason_for_loss_staged IN ('Product Quality / Availability','Product quality/availability')
          THEN 'Product Quality / Availability'
        ELSE reason_for_loss_staged
     END                                                                                        AS reason_for_loss_calc,
     CASE
       WHEN sfdc_opportunity.order_type IN ('4. Contraction','5. Churn - Partial')
        THEN 'Contraction'
        ELSE 'Churn'
     END                                                                                        AS churn_contraction_type,
     CASE
        WHEN is_renewal = 1
          AND subscription_start_date_fiscal_quarter_date >= sfdc_opportunity.close_fiscal_quarter_date
         THEN 'On-Time'
        WHEN is_renewal = 1
          AND subscription_start_date_fiscal_quarter_date < sfdc_opportunity.close_fiscal_quarter_date
            THEN 'Late'
      END                                                                                       AS renewal_timing_status,
      CASE
        WHEN net_arr > -5000
          THEN '1. < 5k'
        WHEN net_arr > -20000 AND net_arr <= -5000
          THEN '2. 5k-20k'
        WHEN net_arr > -50000 AND net_arr <= -20000
          THEN '3. 20k-50k'
        WHEN net_arr > -100000 AND net_arr <= -50000
          THEN '4. 50k-100k'
        WHEN net_arr < -100000
          THEN '5. 100k+'
      END                                                                                       AS churned_contraction_net_arr_bucket,
      CASE
        WHEN sfdc_opportunity.deal_path = 'Direct'
          THEN 'Direct'
        WHEN sfdc_opportunity.deal_path = 'Web Direct'
          THEN 'Web Direct'
        WHEN sfdc_opportunity.deal_path = 'Channel'
            AND sfdc_opportunity.sales_qualified_source = 'Channel Generated'
          THEN 'Partner Sourced'
        WHEN sfdc_opportunity.deal_path = 'Channel'
            AND sfdc_opportunity.sales_qualified_source != 'Channel Generated'
          THEN 'Partner Co-Sell'
      END                                                                                       AS deal_path_engagement,
      CASE
        WHEN net_arr > 0 AND net_arr < 5000
          THEN '1 - Small (<5k)'
        WHEN net_arr >=5000 AND net_arr < 25000
          THEN '2 - Medium (5k - 25k)'
        WHEN net_arr >=25000 AND net_arr < 100000
          THEN '3 - Big (25k - 100k)'
        WHEN net_arr >= 100000
          THEN '4 - Jumbo (>100k)'
        ELSE 'Other'
      END                                                          AS deal_size,
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
      END                                                                                         AS calculated_deal_size,
      CASE
        WHEN
          sfdc_opportunity.stage_name IN (
            '00-Pre Opportunity',
            '0-Pending Acceptance',
            '0-Qualifying',
            'Developing',
            '1-Discovery',
            '2-Developing',
            '2-Scoping'
          )
          THEN 'Pipeline'
        WHEN
          sfdc_opportunity.stage_name IN (
            '3-Technical Evaluation',
            '4-Proposal',
            '5-Negotiating',
            '6-Awaiting Signature',
            '7-Closing'
          )
          THEN '3+ Pipeline'
        WHEN sfdc_opportunity.stage_name IN ('8-Closed Lost', 'Closed Lost')
          THEN 'Lost'
        WHEN sfdc_opportunity.stage_name IN ('Closed Won')
          THEN 'Closed Won'
        ELSE 'Other'
      END AS stage_name_3plus,
      CASE
        WHEN
          sfdc_opportunity.stage_name IN (
            '00-Pre Opportunity',
            '0-Pending Acceptance',
            '0-Qualifying',
            'Developing',
            '1-Discovery',
            '2-Developing',
            '2-Scoping',
            '3-Technical Evaluation'
          )
          THEN 'Pipeline'
        WHEN
          sfdc_opportunity.stage_name IN (
            '4-Proposal', '5-Negotiating', '6-Awaiting Signature', '7-Closing'
          )
          THEN '4+ Pipeline'
        WHEN sfdc_opportunity.stage_name IN ('8-Closed Lost', 'Closed Lost')
          THEN 'Lost'
        WHEN sfdc_opportunity.stage_name IN ('Closed Won')
          THEN 'Closed Won'
        ELSE 'Other'
      END AS stage_name_4plus,

      -- counts and arr totals by pipeline stage
       CASE
        WHEN is_decommissed = 1
          THEN -1
        WHEN is_credit = 1
          THEN 0
        ELSE 1
      END                                               AS calculated_deal_count,
      CASE
        WHEN is_eligible_open_pipeline = 1
          AND is_stage_1_plus = 1
            THEN calculated_deal_count
        ELSE 0
      END                                               AS open_1plus_deal_count,

      CASE
        WHEN is_eligible_open_pipeline = 1
          AND is_stage_3_plus = 1
            THEN calculated_deal_count
        ELSE 0
      END                                               AS open_3plus_deal_count,

      CASE
        WHEN is_eligible_open_pipeline = 1
          AND is_stage_4_plus = 1
            THEN calculated_deal_count
        ELSE 0
      END                                               AS open_4plus_deal_count,
      CASE
        WHEN sfdc_opportunity_stage.is_won = 1
          THEN calculated_deal_count
        ELSE 0
      END                                               AS booked_deal_count,
      CASE
        WHEN is_eligible_churn_contraction = 1
          THEN calculated_deal_count
        ELSE 0
      END                                               AS churned_contraction_deal_count,
      CASE
        WHEN (
              (
                is_renewal = 1
                  AND is_lost = 1
               )
                OR sfdc_opportunity_stage.is_won = 1
              )
              AND is_eligible_churn_contraction = 1
          THEN calculated_deal_count
        ELSE 0
      END                                                 AS booked_churned_contraction_deal_count,
      CASE
        WHEN
          (
            (
              is_renewal = 1
                AND is_lost = 1
              )
            OR sfdc_opportunity_stage.is_won = 1
            )
            AND is_eligible_churn_contraction = 1
          THEN net_arr
        ELSE 0
      END                                                 AS booked_churned_contraction_net_arr,

      CASE
        WHEN is_eligible_churn_contraction = 1
          THEN net_arr
        ELSE 0
      END                                                 AS churned_contraction_net_arr,
      CASE
        WHEN is_eligible_open_pipeline = 1
          THEN net_arr
        ELSE 0
      END                                                AS open_1plus_net_arr,

      CASE
        WHEN is_eligible_open_pipeline = 1
          AND is_stage_3_plus = 1
            THEN net_arr
        ELSE 0
      END                                                AS open_3plus_net_arr,

      CASE
        WHEN is_eligible_open_pipeline = 1
          AND is_stage_4_plus = 1
            THEN net_arr
        ELSE 0
      END                                                AS open_4plus_net_arr,
      CASE
        WHEN (
                sfdc_opportunity_stage.is_won = 1
                OR (
                    is_renewal = 1
                      AND is_lost = 1
                   )
             )
          THEN net_arr
        ELSE 0
      END                                                 AS booked_net_arr,
      CASE
        WHEN sfdc_opportunity.is_edu_oss = 0
          AND sfdc_opportunity.is_deleted = 0
          AND (
               sfdc_opportunity_stage.is_won = 1
                OR (
                    is_renewal = 1
                     AND is_lost = 1)
                   )
          AND sfdc_opportunity.order_type IN ('1. New - First Order','2. New - Connected','3. Growth','4. Contraction','6. Churn - Final','5. Churn - Partial')
            THEN 1
          ELSE 0
      END                                                           AS is_booked_net_arr,
      CASE
        WHEN sfdc_opportunity.deal_path = 'Channel'
          THEN REPLACE(COALESCE(sfdc_opportunity.partner_track, partner_account.partner_track, fulfillment_partner.partner_track,'Open'),'select','Select')
        ELSE 'Direct'
      END                                                                                           AS calculated_partner_track,
      CASE
        WHEN sfdc_account.ultimate_parent_id IN ('001610000111bA3','0016100001F4xla','0016100001CXGCs','00161000015O9Yn','0016100001b9Jsc')
          AND sfdc_opportunity.close_date < '2020-08-01'
            THEN 1
        -- NF 2021 - Pubsec extreme deals
        WHEN sfdc_opportunity.dim_crm_opportunity_id IN ('0064M00000WtZKUQA3','0064M00000Xb975QAB')
          THEN 1
        -- NF 20220415 PubSec duplicated deals on Pipe Gen -- Lockheed Martin GV - 40000 Ultimate Renewal
        WHEN sfdc_opportunity.dim_crm_opportunity_id IN ('0064M00000ZGpfQQAT','0064M00000ZGpfVQAT','0064M00000ZGpfGQAT')
          THEN 1
        ELSE 0
      END                                                                       AS is_excluded_from_pipeline_created,
      CASE
        WHEN sfdc_opportunity.is_open = 1
          THEN DATEDIFF(days, sfdc_opportunity.created_date, CURRENT_DATE)
        ELSE DATEDIFF(days, sfdc_opportunity.created_date, sfdc_opportunity.close_date)
      END                                                           AS calculated_age_in_days,
      CASE
        WHEN arr_created_fiscal_quarter_date = sfdc_opportunity.close_fiscal_quarter_date
          AND is_net_arr_pipeline_created = 1
            THEN net_arr
        ELSE 0
      END                                                         AS created_and_won_same_quarter_net_arr,
      CASE
        WHEN sfdc_opportunity.comp_new_logo_override = 'Yes'
          THEN 1
        ELSE 0
      END                                 AS is_comp_new_logo_override,
      CASE
        WHEN CONTAINS(sfdc_opportunity.competitors, 'Other')
          THEN 1
        ELSE 0
      END AS competitors_other_flag,
      CASE
        WHEN CONTAINS(sfdc_opportunity.competitors, 'GitLab Core')
          THEN 1
        ELSE 0
      END AS competitors_gitlab_core_flag,
      CASE
        WHEN CONTAINS(sfdc_opportunity.competitors, 'None')
          THEN 1
        ELSE 0
      END AS competitors_none_flag,
      CASE
        WHEN CONTAINS(sfdc_opportunity.competitors, 'GitHub Enterprise')
          THEN 1
        ELSE 0
      END AS competitors_github_enterprise_flag,
      CASE
        WHEN CONTAINS(sfdc_opportunity.competitors, 'BitBucket Server')
          THEN 1
        ELSE 0
      END AS competitors_bitbucket_server_flag,
      CASE
        WHEN CONTAINS(sfdc_opportunity.competitors, 'Unknown')
          THEN 1
        ELSE 0
      END AS competitors_unknown_flag,
      CASE
        WHEN CONTAINS(sfdc_opportunity.competitors, 'GitHub.com')
          THEN 1
        ELSE 0
      END AS competitors_github_flag,
      CASE
        WHEN CONTAINS(sfdc_opportunity.competitors, 'GitLab.com')
          THEN 1
        ELSE 0
      END AS competitors_gitlab_flag,
      CASE
        WHEN CONTAINS(sfdc_opportunity.competitors, 'Jenkins')
          THEN 1
        ELSE 0
      END AS competitors_jenkins_flag,
      CASE
        WHEN CONTAINS(sfdc_opportunity.competitors, 'Azure DevOps')
          THEN 1
        ELSE 0
      END AS competitors_azure_devops_flag,
      CASE
        WHEN CONTAINS(sfdc_opportunity.competitors, 'SVN')
          THEN 1
        ELSE 0
      END AS competitors_svn_flag,
      CASE
        WHEN CONTAINS(sfdc_opportunity.competitors, 'BitBucket.Org')
          THEN 1
        ELSE 0
      END AS competitors_bitbucket_flag,
      CASE
        WHEN CONTAINS(sfdc_opportunity.competitors, 'Atlassian')
          THEN 1
        ELSE 0
      END AS competitors_atlassian_flag,
      CASE
        WHEN CONTAINS(sfdc_opportunity.competitors, 'Perforce')
          THEN 1
        ELSE 0
      END AS competitors_perforce_flag,
      CASE
        WHEN CONTAINS(sfdc_opportunity.competitors, 'Visual Studio Team Services')
          THEN 1
        ELSE 0
      END AS competitors_visual_studio_flag,
      CASE
        WHEN CONTAINS(sfdc_opportunity.competitors, 'Azure')
          THEN 1
        ELSE 0
      END AS competitors_azure_flag,
      CASE
        WHEN CONTAINS(sfdc_opportunity.competitors, 'Amazon Code Commit')
          THEN 1
        ELSE 0
      END AS competitors_amazon_code_commit_flag,
      CASE
        WHEN CONTAINS(sfdc_opportunity.competitors, 'CircleCI')
          THEN 1
        ELSE 0
      END AS competitors_circleci_flag,
      CASE
        WHEN CONTAINS(sfdc_opportunity.competitors, 'Bamboo')
          THEN 1
        ELSE 0
      END AS competitors_bamboo_flag,
      CASE
        WHEN CONTAINS(sfdc_opportunity.competitors, 'AWS')
          THEN 1
        ELSE 0
      END AS competitors_aws_flag,

      -- Reporting Keys/Groupings
      COALESCE(sfdc_opportunity.sales_qualified_source, 'Missing sales_qualified_source_name') AS key_sqs,
      LOWER(
        CONCAT(
          sfdc_opportunity.report_user_segment_geo_region_area,
          '-',
          key_sqs,
          '-',
          COALESCE(sfdc_opportunity.order_type, 'Missing order_type_name')
        )
      ) AS report_user_segment_geo_region_area_sqs_ot,
      COALESCE(report_opportunity_user_segment, 'other') AS key_segment,
      COALESCE(deal_group, 'other') AS key_ot,
      COALESCE(sfdc_opportunity.report_opportunity_user_segment || '_' || key_sqs, 'other') AS key_segment_sqs,
      COALESCE(sfdc_opportunity.report_opportunity_user_segment || '_' || deal_group, 'other') AS key_segment_ot,
      COALESCE(sfdc_opportunity.report_opportunity_user_segment || '_' || sfdc_opportunity.report_opportunity_user_geo, 'other') AS key_segment_geo,
      COALESCE(sfdc_opportunity.report_opportunity_user_segment || '_' || sfdc_opportunity.report_opportunity_user_geo || '_' || key_sqs, 'other') AS key_segment_geo_sqs,
      COALESCE(sfdc_opportunity.report_opportunity_user_segment || '_' || sfdc_opportunity.report_opportunity_user_geo || '_' || deal_group, 'other') AS key_segment_geo_ot,
      COALESCE(sfdc_opportunity.report_opportunity_user_segment || '_' || sfdc_opportunity.report_opportunity_user_geo || '_' || sfdc_opportunity.report_opportunity_user_region, 'other') AS key_segment_geo_region,
      COALESCE(sfdc_opportunity.report_opportunity_user_segment || '_' || sfdc_opportunity.report_opportunity_user_geo || '_' || sfdc_opportunity.report_opportunity_user_region || '_' || key_sqs, 'other') AS key_segment_geo_region_sqs,
      COALESCE(sfdc_opportunity.report_opportunity_user_segment || '_' || sfdc_opportunity.report_opportunity_user_geo || '_' || sfdc_opportunity.report_opportunity_user_region || '_' || deal_group, 'other') AS key_segment_geo_region_ot,
      COALESCE(sfdc_opportunity.report_opportunity_user_segment || '_' || sfdc_opportunity.report_opportunity_user_geo || '_' || sfdc_opportunity.report_opportunity_user_region || '_' || sfdc_opportunity.report_opportunity_user_area, 'other') AS key_segment_geo_region_area,
      COALESCE(sfdc_opportunity.report_opportunity_user_segment || '_' || sfdc_opportunity.report_opportunity_user_geo || '_' || sfdc_opportunity.report_opportunity_user_region || '_' || sfdc_opportunity.report_opportunity_user_area || '_' || key_sqs, 'other') AS key_segment_geo_region_area_sqs,
      COALESCE(sfdc_opportunity.report_opportunity_user_segment || '_' || sfdc_opportunity.report_opportunity_user_geo || '_' || sfdc_opportunity.report_opportunity_user_region || '_' || sfdc_opportunity.report_opportunity_user_area || '_' || deal_group, 'other') AS key_segment_geo_region_area_ot,
      COALESCE(sfdc_opportunity.report_opportunity_user_segment || '_' || sfdc_opportunity.report_opportunity_user_geo || '_' || sfdc_opportunity.report_opportunity_user_area, 'other') AS key_segment_geo_area,
      COALESCE(
        report_opportunity_user_segment, 'other'
      ) AS sales_team_cro_level,
      -- This code replicates the reporting structured of FY22, to keep current tools working
      CASE
        WHEN sfdc_opportunity.report_opportunity_user_segment = 'large'
          AND sfdc_opportunity.report_opportunity_user_geo = 'emea'
          THEN 'large_emea'
        WHEN sfdc_opportunity.report_opportunity_user_segment = 'mid-market'
          AND sfdc_opportunity.report_opportunity_user_region = 'amer'
          AND LOWER(sfdc_opportunity.report_opportunity_user_area) LIKE '%west%'
          THEN 'mid-market_west'
        WHEN sfdc_opportunity.report_opportunity_user_segment = 'mid-market'
          AND sfdc_opportunity.report_opportunity_user_region = 'amer'
          AND LOWER(sfdc_opportunity.report_opportunity_user_area) NOT LIKE '%west%'
          THEN 'mid-market_east'
        WHEN sfdc_opportunity.report_opportunity_user_segment = 'smb'
          AND sfdc_opportunity.report_opportunity_user_region = 'amer'
          AND LOWER(sfdc_opportunity.report_opportunity_user_area) LIKE '%west%'
          THEN 'smb_west'
        WHEN sfdc_opportunity.report_opportunity_user_segment = 'smb'
          AND sfdc_opportunity.report_opportunity_user_region = 'amer'
          AND LOWER(sfdc_opportunity.report_opportunity_user_area) NOT LIKE '%west%'
          THEN 'smb_east'
        WHEN sfdc_opportunity.report_opportunity_user_segment = 'smb'
          AND sfdc_opportunity.report_opportunity_user_region = 'latam'
          THEN 'smb_east'
        WHEN (sfdc_opportunity.report_opportunity_user_segment IS NULL
          OR sfdc_opportunity.report_opportunity_user_region IS NULL)
          THEN 'other'
        WHEN
          CONCAT(sfdc_opportunity.report_opportunity_user_segment, '_', sfdc_opportunity.report_opportunity_user_region) LIKE '%other%'
          THEN 'other'
        ELSE CONCAT(sfdc_opportunity.report_opportunity_user_segment, '_', sfdc_opportunity.report_opportunity_user_region)
      END AS sales_team_rd_asm_level,
      COALESCE(
        CONCAT(sfdc_opportunity.report_opportunity_user_segment, '_', sfdc_opportunity.report_opportunity_user_geo), 'other'
      ) AS sales_team_vp_level,
      COALESCE(
        CONCAT(
          sfdc_opportunity.report_opportunity_user_segment,
          '_',
          sfdc_opportunity.report_opportunity_user_geo,
          '_',
          sfdc_opportunity.report_opportunity_user_region
        ),
        'other'
      ) AS sales_team_avp_rd_level,
      COALESCE(
        CONCAT(
          sfdc_opportunity.report_opportunity_user_segment,
          '_',
          sfdc_opportunity.report_opportunity_user_geo,
          '_',
          sfdc_opportunity.report_opportunity_user_region,
          '_',
          sfdc_opportunity.report_opportunity_user_area
        ),
        'other'
      ) AS sales_team_asm_level,
      CASE
        WHEN
          sfdc_opportunity.account_owner_team_stamped IN (
            'Commercial - SMB', 'SMB', 'SMB - US', 'SMB - International'
          )
          THEN 'SMB'
        WHEN
          sfdc_opportunity.account_owner_team_stamped IN (
            'APAC', 'EMEA', 'Channel', 'US West', 'US East', 'Public Sector'
          )
          THEN 'Large'
        WHEN
          sfdc_opportunity.account_owner_team_stamped IN (
            'MM - APAC', 'MM - East', 'MM - EMEA', 'Commercial - MM', 'MM - West', 'MM-EMEA'
          )
          THEN 'Mid-Market'
        ELSE 'SMB'
      END AS account_owner_team_stamped_cro_level

    FROM sfdc_opportunity
    INNER JOIN sfdc_opportunity_stage
      ON sfdc_opportunity.stage_name = sfdc_opportunity_stage.primary_label
    LEFT JOIN quote
      ON sfdc_opportunity.dim_crm_opportunity_id = quote.dim_crm_opportunity_id
    LEFT JOIN linear_attribution_base
      ON sfdc_opportunity.dim_crm_opportunity_id = linear_attribution_base.dim_crm_opportunity_id
    LEFT JOIN campaigns_per_opp
      ON sfdc_opportunity.dim_crm_opportunity_id = campaigns_per_opp.dim_crm_opportunity_id
    LEFT JOIN first_contact
      ON sfdc_opportunity.dim_crm_opportunity_id = first_contact.opportunity_id 
        AND first_contact.row_num = 1
    LEFT JOIN dim_date AS arr_created_date
      ON sfdc_opportunity.iacv_created_date::DATE = arr_created_date.date_actual
    LEFT JOIN dim_date AS subscription_start_date
      ON sfdc_opportunity.subscription_start_date::DATE = subscription_start_date.date_actual
    LEFT JOIN sfdc_account AS fulfillment_partner
      ON sfdc_opportunity.fulfillment_partner = fulfillment_partner.account_id
    LEFT JOIN sfdc_account AS partner_account
      ON sfdc_opportunity.partner_account = partner_account.account_id
    LEFT JOIN sfdc_account
      ON sfdc_opportunity.dim_crm_account_id= sfdc_account.account_id
    LEFT JOIN net_iacv_to_net_arr_ratio
      ON sfdc_opportunity.opportunity_owner_user_segment = net_iacv_to_net_arr_ratio.user_segment_stamped
        AND sfdc_opportunity.order_type = net_iacv_to_net_arr_ratio.order_type

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@michellecooper",
    updated_by="@michellecooper",
    created_date="2022-02-01",
    updated_date="2022-11-07"
) }}