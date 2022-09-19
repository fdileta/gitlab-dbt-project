{{ config(alias='report_pipeline_deltas') }}

WITH sfdc_opportunity_xf AS (

    SELECT *
    FROM {{ref('wk_sales_sfdc_opportunity_xf')}}
    --FROM restricted_safe_workspace_sales.sfdc_opportunity_xf

), sfdc_opportunity_snapshot_history_xf AS (

    SELECT *
    FROM {{ref('wk_sales_sfdc_opportunity_snapshot_history_xf')}}
    --FROM restricted_safe_workspace_sales.sfdc_opportunity_snapshot_history_xf

), date_details AS (

    SELECT *
    FROM {{ ref('wk_sales_date_details') }}
    --FROM workspace_sales.date_details

), snapshot_oppty AS (

    SELECT
      snapshot_oppty.*,
      CASE
          WHEN oppty.pipeline_created_fiscal_quarter_name = snapshot_oppty.snapshot_fiscal_quarter_name
              AND snapshot_oppty.is_eligible_created_pipeline_flag = 1
            THEN 1
        ELSE 0
      END AS is_pipeline_created_flag,

      -- if the deal was not created in the snapshot quarter (or not eligible) we asume it was crerated
      -- in previous quarters
      CASE
          WHEN is_pipeline_created_flag = 1
            THEN 'Pipe Gen'
          ELSE 'Existing Pipe'
        END AS pipeline_category,

        -- Based on deal stage of the deal at the snapshot tiime
        'Open' AS deal_status,
        'Open' AS deal_status_group,
        
        -- This field track if the type of pipeline "motion", if it expands / contract / create pipeline, or if it closes
        -- it
        'Expansion / Contraction' AS pipeline_motion

    FROM sfdc_opportunity_snapshot_history_xf snapshot_oppty
      INNER JOIN sfdc_opportunity_xf oppty -- we use the latest ARR Creaated date to accommodate corrections
        ON oppty.opportunity_id = snapshot_oppty.opportunity_id
    WHERE snapshot_oppty.is_open = 1
        AND snapshot_oppty.is_stage_1_plus = 1
        AND snapshot_oppty.opportunity_category NOT IN  ('Ramp Deal')

), net_arr_delta AS (

    -- The goal of this CTE is to calculate the delta between the current considered line and the last
    -- one, we are interested in keeping <> 0 deltas within the model, together with the date of the 
    -- event.

    SELECT 
      snapshot_date                                 AS report_date,
      snapshot_fiscal_quarter_date                  AS report_fiscal_quarter_date,
      snapshot_fiscal_quarter_name                  AS report_fiscal_quarter_name,
      snapshot_day_of_fiscal_quarter_normalised     AS report_day_of_fiscal_quarter_normalised,
      snapshot_fiscal_year                          AS report_fiscal_year,
      opportunity_id,
      is_pipeline_created_flag,
      pipeline_category,
      deal_status,
      deal_status_group,
      pipeline_motion,

      is_closed,
      is_open,
      is_won,
      is_lost,

      net_arr,
      LAG(net_arr, 1, 0) OVER (PARTITION BY opportunity_id ORDER BY snapshot_date)           AS prev_net_arr,
      net_arr - LAG(net_arr, 1, 0) OVER (PARTITION BY opportunity_id ORDER BY snapshot_date) AS delta_net_arr

    FROM snapshot_oppty
    QUALIFY delta_net_arr <> 0

 ), closed_pipeline AS (

    -- This CTE captures the closed deals that exited the pipeline, that's why positive net arr is
    -- converted into a negative value, to represent that they are removed from the total open pipeline 
    -- amount.

    SELECT
      oppty.opportunity_id,
      oppty.close_date                            AS report_date,
      oppty.close_fiscal_quarter_date             AS report_fiscal_quarter_date,
      oppty.close_fiscal_quarter_name             AS report_fiscal_quarter_name,
      oppty.close_fiscal_year                     AS report_fiscal_year,
      close_date.day_of_fiscal_quarter_normalised AS report_day_of_fiscal_quarter_normalised,

      CASE
        WHEN oppty.pipeline_created_fiscal_quarter_name = oppty.close_fiscal_quarter_name
          AND oppty.is_eligible_created_pipeline_flag = 1
            THEN 1
        ELSE 0
      END AS is_pipeline_created_flag,

      CASE
        WHEN is_pipeline_created_flag = 1
            THEN 'Pipe Gen'
        ELSE 'Existing Pipe'
      END AS pipeline_category,

      CASE
        WHEN oppty.is_won = 1
            THEN 'Won'
        ELSE 'Lost'
      END                                     AS deal_status,
      'Closed'                                AS deal_status_group,
      'Removal'                               AS pipeline_motion,

      net_arr,
      CASE
        WHEN oppty.net_arr > 0
            THEN -1 * oppty.net_arr
        WHEN oppty.net_arr < 0
            THEN oppty.net_arr
      END                                     AS delta_net_arr,

      is_closed,
      is_open,
      is_won,
      is_lost

    FROM sfdc_opportunity_xf oppty
      LEFT JOIN date_details close_date
        ON close_date.date_actual = oppty.close_date
    WHERE (oppty.is_won = 1 OR oppty.is_lost = 1)
      AND oppty.net_arr <> 0
      AND oppty.close_date < CURRENT_DATE


), deltas_consolidated AS (

    SELECT
      opportunity_id,
      report_date::date AS report_date,
      report_fiscal_quarter_date,
      report_fiscal_quarter_name,
      report_day_of_fiscal_quarter_normalised,
      report_fiscal_year,
      is_pipeline_created_flag,
      pipeline_category,
      deal_status,
      deal_status_group,
      pipeline_motion,
      --net_arr,
     -- NULL              AS prev_net_arr,
      delta_net_arr,
      is_closed,
      is_open,
      is_won,
      is_lost
    FROM closed_pipeline
    UNION ALL
    SELECT
      opportunity_id,
      report_date::date AS report_date,
      report_fiscal_quarter_date,
      report_fiscal_quarter_name,
      report_day_of_fiscal_quarter_normalised,
      report_fiscal_year,
      is_pipeline_created_flag,
      pipeline_category,
      deal_status,
      deal_status_group,
      pipeline_motion,
     -- net_arr,
     -- prev_net_arr,
      delta_net_arr,
      is_closed,
      is_open,
      is_won,
      is_lost
    FROM net_arr_delta

), final AS (

    SELECT 
      deltas.*,
      oppty.sales_qualified_source,
      oppty.order_type_stamped,
      oppty.opportunity_category,
      oppty.sales_type,
      oppty.owner_id AS opportuniy_owner_id,
      oppty.opportunity_owner,
      oppty.account_id,
      oppty.account_name,
      oppty.opportunity_name,

      -- NF 2022-02-17 These keys are used in the pipeline metrics models and on the X-Ray dashboard to link gSheets with
      -- different aggregation levels

      oppty.key_sqs,
      oppty.key_ot,
      oppty.key_segment,
      oppty.key_segment_sqs,
      oppty.key_segment_ot,
      oppty.key_segment_geo,
      oppty.key_segment_geo_sqs,
      oppty.key_segment_geo_ot,
      oppty.key_segment_geo_region,
      oppty.key_segment_geo_region_sqs,
      oppty.key_segment_geo_region_ot,
      oppty.key_segment_geo_region_area,
      oppty.key_segment_geo_region_area_sqs,
      oppty.key_segment_geo_region_area_ot,
      oppty.key_segment_geo_area,
      oppty.sales_team_cro_level,

      -- NF: This code replicates the reporting structured of FY22, to keep current tools working
      oppty.sales_team_rd_asm_level,

      oppty.sales_team_vp_level,
      oppty.sales_team_avp_rd_level,
      oppty.sales_team_asm_level

    FROM deltas_consolidated deltas
      LEFT JOIN sfdc_opportunity_xf oppty
        ON oppty.opportunity_id = deltas.opportunity_id

)

SELECT *
FROM final

