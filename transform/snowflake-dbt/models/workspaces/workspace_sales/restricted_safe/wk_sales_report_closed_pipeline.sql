{{ config(alias='report_closed_pipeline') }}

WITH sfdc_opportunity_xf AS (

    SELECT *
    FROM {{ref('wk_sales_sfdc_opportunity_xf')}}
    --FROM restricted_safe_workspace_sales.sfdc_opportunity_xf

), date_details AS (

    SELECT *
    FROM {{ ref('wk_sales_date_details') }}
    --FROM workspace_sales.date_details

), sfdc_users_xf AS (
    
    SELECT *
    FROM {{ref('wk_sales_sfdc_users_xf')}}
    --FROM prod.workspace_sales.sfdc_users_xf

), closed_pipeline AS (

    /* This CTE captures the closed deals that exited the pipeline, that's why positive net arr is
    converted into a negative value to represent that they are removed from the total open pipeline amount. */

    SELECT
      oppty.opportunity_id,
      oppty.owner_id,
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

    FROM sfdc_opportunity_xf AS oppty
    LEFT JOIN date_details AS close_date
      ON close_date.date_actual = oppty.close_date
    WHERE (oppty.is_won = 1 OR oppty.is_lost = 1)
      AND oppty.net_arr <> 0
      AND oppty.close_date < CURRENT_DATE


), final AS (

    SELECT 
      closed_pipeline.*,
      users.user_email,
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

    FROM closed_pipeline
    LEFT JOIN sfdc_opportunity_xf AS oppty
      ON oppty.opportunity_id = closed_pipeline.opportunity_id
    LEFT JOIN sfdc_users_xf AS users
      ON closed_pipeline.owner_id = users.user_id

)

SELECT *
FROM final

