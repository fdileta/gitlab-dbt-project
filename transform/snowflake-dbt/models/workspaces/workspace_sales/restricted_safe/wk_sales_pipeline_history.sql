{{ config(alias='report_pipeline_history') }}

WITH sfdc_opportunity_xf AS (

    SELECT *
    FROM {{ref('wk_sales_sfdc_opportunity_xf')}}

), sfdc_opportunity_snapshot_history_xf AS (

    SELECT *
    FROM {{ref('wk_sales_sfdc_opportunity_snapshot_history_xf')}}

), snapshot_oppty AS (

    SELECT *,
      CASE
         WHEN pipeline_created_fiscal_quarter_name = snapshot_fiscal_quarter_name
            THEN 1
        ELSE 0
     END AS is_pipeline_created_flag,
      CASE 
        WHEN is_open = 1
          AND is_stage_1_plus = 1
          AND opportunity_category = 'Standard'
            THEN 1
            ELSE 0
      END AS is_eligible_open_pipeline_expansion_flag
    FROM sfdc_opportunity_snapshot_history_xf
    WHERE is_open = 1
    AND is_stage_1_plus = 1
    AND opportunity_category = 'Standard'

), net_arr_delta AS (

    -- The goal of this CTE is to calculate the delta between the current considered line and the last
    -- one, we are interested in keeping <> 0 deltas within the model, together with the date of the 
    -- event.

    SELECT snapshot_date,
           snapshot_fiscal_quarter_date,
           snapshot_fiscal_quarter_name,
           snapshot_day_of_fiscal_quarter_normalised,
           snapshot_fiscal_year,
           opportunity_id,
           is_pipeline_created_flag,
           is_eligible_open_pipeline_expansion_flag,
           net_arr,
           LAG(net_arr, 1, 0) OVER (PARTITION BY opportunity_id ORDER BY snapshot_date)           AS prev_net_arr,
           net_arr - LAG(net_arr, 1, 0) OVER (PARTITION BY opportunity_id ORDER BY snapshot_date) AS delta_net_arr
           

    FROM snapshot_oppty
    QUALIFY delta_net_arr <> 0
  
), final AS (

  SELECT net_arr_delta.*,
        sfdc_opportunity_xf.opportunity_name,
        sfdc_opportunity_xf.account_name,
        sfdc_opportunity_xf.order_type_stamped,
        sfdc_opportunity_xf.sales_type,
        sfdc_opportunity_xf.sales_qualified_source
  
  FROM net_arr_delta
  LEFT JOIN sfdc_opportunity_xf
    ON sfdc_opportunity_xf.opportunity_id = net_arr_delta.opportunity_id


)

SELECT *
FROM final