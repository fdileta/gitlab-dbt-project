WITH date_details AS (

    SELECT *
    FROM prod.workspace_sales.date_details

), agg_demo_keys AS (

    SELECT *
    FROM prod.restricted_safe_workspace_sales.report_agg_demo_sqs_ot_keys

),  today_date AS (

    SELECT
        date_actual,                              
        first_day_of_fiscal_quarter,              
        fiscal_quarter_name_fy,                   
        day_of_fiscal_quarter_normalised,         
        fiscal_year                              
   FROM date_details
   WHERE date_actual = CURRENT_DATE

), sfdc_opportunity_xf AS (

    SELECT
        opties.*,
        today_date.date_actual                      AS snapshot_date,
        today_date.day_of_fiscal_quarter_normalised AS snapshot_day_of_fiscal_quarter_normalised,
        today_date.fiscal_quarter_name_fy           AS snapshot_fiscal_quarter_name,
        today_date.first_day_of_fiscal_quarter      AS snapshot_fiscal_quarter_date,
        today_date.fiscal_year                      AS snapshot_fiscal_year
    FROM prod.restricted_safe_workspace_sales.sfdc_opportunity_xf AS opties
    CROSS JOIN  today_date
    WHERE opties.is_deleted = 0
      AND opties.is_excluded_flag = 0
      AND opties.is_edu_oss = 0
      AND opties.net_arr is not null
      AND lower(opties.deal_group) LIKE ANY ('%growth%', '%new%')
      AND opties.stage_name NOT IN ('0-Pending Acceptance','Unqualified','00-Pre Opportunity','9-Unqualified','10-Duplicate')
      AND opties.key_segment != 'jihu'

), sfdc_opportunity_snapshot_history_xf AS (

    SELECT *
    FROM prod.restricted_safe_workspace_sales.sfdc_opportunity_snapshot_history_xf AS opp_snapshot
    WHERE opp_snapshot.is_deleted = 0
      AND opp_snapshot.is_excluded_flag = 0
      AND opp_snapshot.is_edu_oss = 0
      AND opp_snapshot.net_arr is not null
      AND lower(opp_snapshot.deal_group) LIKE ANY ('%growth%', '%new%')
      -- include up to current date, where we use the current opportunity object
      AND opp_snapshot.snapshot_date < CURRENT_DATE
      -- stage 1 plus, won & lost excluded omitted deals
      AND opp_snapshot.stage_name NOT IN ('0-Pending Acceptance','Unqualified','00-Pre Opportunity','9-Unqualified','10-Duplicate')
       -- Not JiHu account
      AND opp_snapshot.key_segment != 'jihu'

), pipeline_snapshot AS (

    SELECT
      -----------------------------------------------------------------------------------
      -- report keys
      opp_snapshot.key_segment,
      opp_snapshot.report_user_segment_geo_region_area_sqs_ot,
      -----------------------------------------------------------------------------------
      -- snapshot date fields
      opp_snapshot.snapshot_date,
      opp_snapshot.snapshot_fiscal_year,
      opp_snapshot.snapshot_fiscal_quarter_name,
      opp_snapshot.snapshot_fiscal_quarter_date,
      opp_snapshot.snapshot_day_of_fiscal_quarter_normalised,
      -----------------------------------------------------------------------------------

      opp_snapshot.close_fiscal_quarter_name,
      opp_snapshot.close_fiscal_quarter_date,
      opp_snapshot.created_fiscal_quarter_name,
      opp_snapshot.created_fiscal_quarter_date,
      -----------------------------------------------------------------------------------
      -- measures
      opp_snapshot.net_arr,

      opp_snapshot.open_1plus_deal_count,
      opp_snapshot.open_3plus_deal_count,
      opp_snapshot.open_4plus_deal_count,
      opp_snapshot.booked_deal_count,

      opp_snapshot.open_1plus_net_arr,
      opp_snapshot.open_3plus_net_arr,
      opp_snapshot.open_4plus_net_arr,

      opp_snapshot.booked_net_arr
    FROM sfdc_opportunity_snapshot_history_xf opp_snapshot

    UNION ALL

    SELECT
      -----------------------------------------------------------------------------------
      -- report keys
      opties.key_segment,
      opties.report_user_segment_geo_region_area_sqs_ot,
      -----------------------------------------------------------------------------------
      -- snapshot date fields
      opties.snapshot_date,
      opties.snapshot_fiscal_year,
      opties.snapshot_fiscal_quarter_name,
      opties.snapshot_fiscal_quarter_date,
      opties.snapshot_day_of_fiscal_quarter_normalised,
      -----------------------------------------------------------------------------------

      opties.close_fiscal_quarter_name,
      opties.close_fiscal_quarter_date,
      opties.created_fiscal_quarter_name,
      opties.created_fiscal_quarter_date,
      -----------------------------------------------------------------------------------
      -- measures
      opties.net_arr,

      opties.open_1plus_deal_count,
      opties.open_3plus_deal_count,
      opties.open_4plus_deal_count,
      opties.booked_deal_count,

      opties.open_1plus_net_arr,
      opties.open_3plus_net_arr,
      opties.open_4plus_net_arr,

      opties.booked_net_arr
    FROM sfdc_opportunity_xf opties

), target_periods AS (

    SELECT 
        start_q.first_day_of_fiscal_quarter                   AS start_fiscal_quarter_date,
        end_q.first_day_of_fiscal_quarter                     AS end_fiscal_quarter_date,
        end_q.last_day_of_fiscal_quarter                      AS end_last_day_of_fiscal_quarter,
        start_q.fiscal_quarter_name_fy                        AS start_fiscal_quarter_name,
        end_q.fiscal_quarter_name_fy                          AS end_fiscal_quarter_name
    FROM date_details AS start_q
    INNER JOIN date_details AS end_q
        ON end_q.date_actual = DATEADD(month, 9, start_q.first_day_of_fiscal_quarter)
    WHERE start_q.date_actual BETWEEN DATEADD(YEAR,-5,CURRENT_DATE)
        AND  DATEADD(YEAR,5,CURRENT_DATE)

), pipeline_combined AS (

    SELECT DISTINCT
        target_periods.start_fiscal_quarter_date,
        target_periods.end_fiscal_quarter_date,
        target_periods.start_fiscal_quarter_name,
        target_periods.end_fiscal_quarter_name,
        365 - DATEDIFF(DAY, pipeline_snapshot.snapshot_date, target_periods.end_last_day_of_fiscal_quarter) AS day_of_combined_4q_normalised,
        pipeline_snapshot.*
    FROM pipeline_snapshot
    INNER JOIN target_periods
        ON pipeline_snapshot.snapshot_fiscal_quarter_date >= target_periods.start_fiscal_quarter_date
        AND pipeline_snapshot.snapshot_fiscal_quarter_date <= target_periods.end_fiscal_quarter_date
        AND pipeline_snapshot.close_fiscal_quarter_date >= target_periods.start_fiscal_quarter_date
        AND pipeline_snapshot.close_fiscal_quarter_date <= target_periods.end_fiscal_quarter_date

), total_booked AS (

    SELECT
        report_user_segment_geo_region_area_sqs_ot,
        start_fiscal_quarter_date,
        SUM(booked_net_arr) AS total_booked_net_arr
    FROM pipeline_combined
    WHERE day_of_combined_4q_normalised = 365
    GROUP BY 1, 2

), open_pipeline AS (

    SELECT
        key_segment,
        report_user_segment_geo_region_area_sqs_ot,
        start_fiscal_quarter_date,
        end_fiscal_quarter_date,
        start_fiscal_quarter_name,
        end_fiscal_quarter_name,
        day_of_combined_4q_normalised,
        SUM(open_1plus_deal_count)                                  AS open_1plus_deal_count,
        SUM(open_3plus_deal_count)                                  AS open_3plus_deal_count,
        SUM(open_4plus_deal_count)                                  AS open_4plus_deal_count,
        SUM(booked_deal_count)                                      AS booked_deal_count,
        SUM(open_1plus_net_arr)                                     AS open_1plus_net_arr,
        SUM(open_3plus_net_arr)                                     AS open_3plus_net_arr,
        SUM(open_4plus_net_arr)                                     AS open_4plus_net_arr,
        SUM(booked_net_arr)                                         AS booked_net_arr

    FROM pipeline_combined
    GROUP BY 1, 2, 3, 4, 5, 6, 7

), final AS (

    SELECT
        o_p.report_user_segment_geo_region_area_sqs_ot,
        o_p.start_fiscal_quarter_date,
        o_p.end_fiscal_quarter_date,
        o_p.start_fiscal_quarter_name,
        o_p.end_fiscal_quarter_name,
        o_p.day_of_combined_4q_normalised,

        agg_demo_keys.report_opportunity_user_segment,
        agg_demo_keys.report_opportunity_user_geo,
        agg_demo_keys.report_opportunity_user_region,
        agg_demo_keys.report_opportunity_user_area,

        agg_demo_keys.sales_team_cro_level,
        agg_demo_keys.sales_team_vp_level,
        agg_demo_keys.sales_team_avp_rd_level,
        agg_demo_keys.sales_team_asm_level,
        agg_demo_keys.deal_category,
        agg_demo_keys.deal_group,
        agg_demo_keys.sales_qualified_source,
        agg_demo_keys.sales_team_rd_asm_level,

        agg_demo_keys.key_sqs,
        agg_demo_keys.key_ot,

        agg_demo_keys.key_segment,
        agg_demo_keys.key_segment_sqs,
        agg_demo_keys.key_segment_ot,

        agg_demo_keys.key_segment_geo,
        agg_demo_keys.key_segment_geo_sqs,
        agg_demo_keys.key_segment_geo_ot,

        agg_demo_keys.key_segment_geo_region,
        agg_demo_keys.key_segment_geo_region_sqs,
        agg_demo_keys.key_segment_geo_region_ot,

        agg_demo_keys.key_segment_geo_region_area,
        agg_demo_keys.key_segment_geo_region_area_sqs,
        agg_demo_keys.key_segment_geo_region_area_ot,

        agg_demo_keys.key_segment_geo_area,

        agg_demo_keys.report_user_segment_geo_region_area,

        ----------------------------
        COALESCE(o_p.booked_deal_count, 0)                          AS n4q_booked_deal_count,
        COALESCE(o_p.open_1plus_deal_count, 0)                      AS n4q_open_1plus_deal_count,
        COALESCE(o_p.open_3plus_deal_count, 0)                      AS n4q_open_3plus_deal_count,
        COALESCE(o_p.open_4plus_deal_count, 0)                      AS n4q_open_4plus_deal_count,
        COALESCE(o_p.open_1plus_net_arr, 0)                         AS n4q_open_1plus_net_arr,
        COALESCE(o_p.open_3plus_net_arr, 0)                         AS n4q_open_3plus_net_arr,
        COALESCE(o_p.open_4plus_net_arr, 0)                         AS n4q_open_4plus_net_arr,
        COALESCE(o_p.booked_net_arr, 0)                             AS n4q_booked_net_arr,
        COALESCE(total.total_booked_net_arr, 0)                     AS total_n4q_booked_net_arr


    FROM open_pipeline AS o_p
    LEFT JOIN agg_demo_keys
      ON o_p.report_user_segment_geo_region_area_sqs_ot = agg_demo_keys.report_user_segment_geo_region_area_sqs_ot
    LEFT JOIN total_booked As total
      ON o_p.report_user_segment_geo_region_area_sqs_ot = total.report_user_segment_geo_region_area_sqs_ot
      AND o_p.start_fiscal_quarter_date = total.start_fiscal_quarter_date

)

SELECT *
FROM final