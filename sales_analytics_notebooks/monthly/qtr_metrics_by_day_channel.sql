WITH sfdc_accounts_xf AS (


    SELECT *
    FROM prod.restricted_safe_legacy.sfdc_accounts_xf
 
), sfdc_users_xf AS (
  
  SELECT * 
  FROM prod.workspace_sales.sfdc_users_xf
  
), date_details AS (
  
    SELECT *
    FROM prod.workspace_sales.date_details

 ), agg_demo_keys AS (
    -- keys used for aggregated historical analysis

    SELECT *
    FROM prod.restricted_safe_workspace_sales.report_agg_demo_sqs_ot_keys

), sfdc_opportunity_snapshot_history_xf AS (
   
   
    SELECT opp_snapshot.*,

          CASE
            WHEN  opp_snapshot.order_type_stamped = '1. New - First Order'
                THEN 'First Order'          
            WHEN lower(account_owner.role_name) like ('pooled%')
                    AND opp_snapshot.key_segment IN ('smb','mid-market')
                    AND opp_snapshot.order_type_stamped != '1. New - First Order'
                THEN 'Pooled'
            WHEN lower(account_owner.role_name) like ('terr%') 
                    AND opp_snapshot.key_segment IN ('smb','mid-market')
                    AND opp_snapshot.order_type_stamped != '1. New - First Order'
                THEN 'Territory'
            WHEN lower(account_owner.role_name) like ('named%') 
                    AND opp_snapshot.key_segment IN ('smb','mid-market')
                    AND opp_snapshot.order_type_stamped != '1. New - First Order'
                THEN 'Named'
            WHEN  opp_snapshot.order_type_stamped IN ('2. New - Connected','4. Contraction','6. Churn - Final','5. Churn - Partial','3. Growth')
                   AND opp_snapshot.key_segment IN ('smb','mid-market')
                THEN 'Expansion'
            ELSE 'Other'
        END                                                            AS commercial_sal_category,

        CASE 
        WHEN opp_snapshot.sales_qualified_source = 'Channel Generated'
          THEN 'Channel Generated'
        WHEN opp_snapshot.sales_qualified_source != 'Channel Generated'            
          AND NOT(LOWER(resale.account_name) LIKE ANY ('%ibm%','%google%','%gcp%','%amazon%'))
          THEN 'Channel Co-Sell'
        WHEN opp_snapshot.sales_qualified_source != 'Channel Generated'            
          AND LOWER(resale.account_name ) LIKE ANY ('%ibm%','%google%','%gcp%','%amazon%')
          THEN 'Alliance Co-Sell'
        ELSE 'Direct'
      END                                               AS channel_category,
        /*CASE 
            WHEN LOWER(resale.account_name) LIKE ANY ('%ibm%','%google%','%gcp%','%amazon%')
              THEN 'Alliance-Cosell'
            WHEN LOWER(resale.account_name) IS NOT NULL
              THEN 'Channel-Cosell'
            ELSE 'Direct-Cosell'
          END                                             AS transaction_type,*/

          CASE
            WHEN LOWER(resale.account_name)LIKE '%ibm%'
              THEN 'IBM'
            WHEN LOWER(resale.account_name) LIKE ANY ('%google%','%gcp%')
              THEN 'GCP'
            WHEN LOWER(resale.account_name) LIKE '%amazon%'
              THEN 'AWS'
            WHEN LOWER(resale.account_name) IS NOT NULL
              THEN 'Channel'
            ELSE 'Direct'
          END                                               AS alliance_partner
    
    FROM prod.restricted_safe_workspace_sales.sfdc_opportunity_snapshot_history_xf opp_snapshot
    -- identify the account owner user roles
    LEFT JOIN sfdc_accounts_xf account
      ON account.account_id = opp_snapshot.account_id
    -- link resale partner from fullfilment partner
    LEFT JOIN sfdc_accounts_xf resale
      ON opp_snapshot.fulfillment_partner = resale.account_id
    LEFT JOIN sfdc_users_xf account_owner
      ON account_owner.user_id = account.owner_id
    WHERE opp_snapshot.is_deleted = 0
      AND opp_snapshot.is_excluded_flag = 0
      AND opp_snapshot.is_edu_oss = 0
      AND opp_snapshot.net_arr is not null
      AND lower(opp_snapshot.deal_group) LIKE ANY ('%growth%', '%new%')
      -- include up to current date, where we use the current opportunity object
      AND opp_snapshot.snapshot_date < CURRENT_DATE
      -- stage 1 plus, won & lost excluded ommited deals    
      AND opp_snapshot.stage_name NOT IN ('0-Pending Acceptance','Unqualified','00-Pre Opportunity','9-Unqualified','10-Duplicate') 
    --AND order_type_stamped != '1. New - First Order'
    
), pipeline_snapshot AS (

    SELECT 
      -------------------------------------
      -- report keys
      opp_snapshot.report_user_segment_geo_region_area_sqs_ot,
      opp_snapshot.commercial_sal_category,
      opp_snapshot.alliance_partner,
      opp_snapshot.channel_category,
       
      -------------------------------------
      
      -----------------------------------------------------------------------------------
      -- snapshot date fields
      opp_snapshot.snapshot_date,
      opp_snapshot.snapshot_fiscal_year,
      opp_snapshot.snapshot_fiscal_quarter_name,
      opp_snapshot.snapshot_fiscal_quarter_date,
      opp_snapshot.snapshot_day_of_fiscal_quarter_normalised,
      -----------------------------------------------------------------------------------
    
      opp_snapshot.stage_name,
      opp_snapshot.forecast_category_name,
      opp_snapshot.is_renewal,
      opp_snapshot.is_won,
      opp_snapshot.is_lost,
      opp_snapshot.is_open,
      opp_snapshot.is_excluded_flag,
      
      opp_snapshot.close_fiscal_quarter_name,
      opp_snapshot.close_fiscal_quarter_date,
      opp_snapshot.created_fiscal_quarter_name,
      opp_snapshot.created_fiscal_quarter_date,

      opp_snapshot.net_arr,

      opp_snapshot.calculated_deal_count                AS deal_count,

      opp_snapshot.open_1plus_deal_count,
      opp_snapshot.open_3plus_deal_count,
      opp_snapshot.open_4plus_deal_count,
  
      -- booked deal count
      opp_snapshot.booked_deal_count,
      opp_snapshot.churned_contraction_deal_count,
  
      -----------------------------------------------------------------------------------
      -- NF: 20210201  NET ARR fields

      opp_snapshot.open_1plus_net_arr,
      opp_snapshot.open_3plus_net_arr,
      opp_snapshot.open_4plus_net_arr,
  
      -- booked net _arr
      opp_snapshot.booked_net_arr,

      -- churned net_arr
      opp_snapshot.churned_contraction_net_arr,
  
      opp_snapshot.created_and_won_same_quarter_net_arr,
      opp_snapshot.created_in_snapshot_quarter_net_arr,
      opp_snapshot.created_in_snapshot_quarter_deal_count

    FROM sfdc_opportunity_snapshot_history_xf opp_snapshot
  
), reported_quarter AS (
  
    -- daily snapshot of pipeline metrics per quarter within the quarter
    SELECT 
      pipeline_snapshot.snapshot_fiscal_quarter_date                AS close_fiscal_quarter_date,
      pipeline_snapshot.snapshot_day_of_fiscal_quarter_normalised   AS close_day_of_fiscal_quarter_normalised,
      
      -------------------
      -- report keys
      -- FY23 needs to be updated to the new logic
      pipeline_snapshot.report_user_segment_geo_region_area_sqs_ot,
      pipeline_snapshot.commercial_sal_category,
      pipeline_snapshot.alliance_partner,
      pipeline_snapshot.channel_category,
      -------------------

      SUM(pipeline_snapshot.deal_count)                           AS deal_count,
      SUM(pipeline_snapshot.booked_deal_count)                    AS booked_deal_count,
      SUM(pipeline_snapshot.churned_contraction_deal_count)       AS churned_contraction_deal_count,

      SUM(pipeline_snapshot.open_1plus_deal_count)                AS open_1plus_deal_count,
      SUM(pipeline_snapshot.open_3plus_deal_count)                AS open_3plus_deal_count,
      SUM(pipeline_snapshot.open_4plus_deal_count)                AS open_4plus_deal_count,

      -----------------------------------------------------------------------------------
      -- NF: 20210201  NET ARR fields
      
      SUM(pipeline_snapshot.open_1plus_net_arr)                   AS open_1plus_net_arr,
      SUM(pipeline_snapshot.open_3plus_net_arr)                   AS open_3plus_net_arr,
      SUM(pipeline_snapshot.open_4plus_net_arr)                   AS open_4plus_net_arr,
      SUM(pipeline_snapshot.booked_net_arr)                       AS booked_net_arr,
      
      -- churned net_arr
      SUM(pipeline_snapshot.churned_contraction_net_arr)          AS churned_contraction_net_arr,
  
      SUM(pipeline_snapshot.created_and_won_same_quarter_net_arr) AS created_and_won_same_quarter_net_arr

      -----------------------------------------------------------------------------------

    FROM pipeline_snapshot
    -- snapshot quarter rows that close within the same quarter
    WHERE pipeline_snapshot.snapshot_fiscal_quarter_name = pipeline_snapshot.close_fiscal_quarter_name
    GROUP BY 1,2,3,4,5,6
  
-- Quarter plus 1, from the reported quarter perspective
), report_quarter_plus_1 AS (
    
    SELECT 
      pipeline_snapshot.snapshot_fiscal_quarter_date                AS close_fiscal_quarter_date,
      pipeline_snapshot.snapshot_day_of_fiscal_quarter_normalised   AS close_day_of_fiscal_quarter_normalised,

      pipeline_snapshot.close_fiscal_quarter_name                   AS rq_plus_1_close_fiscal_quarter_name,
      pipeline_snapshot.close_fiscal_quarter_date                   AS rq_plus_1_close_fiscal_quarter_date,
 
      -------------------
      -- report keys
      pipeline_snapshot.report_user_segment_geo_region_area_sqs_ot,
      pipeline_snapshot.commercial_sal_category,
      pipeline_snapshot.alliance_partner,
      pipeline_snapshot.channel_category,
      -------------------
     
      SUM(pipeline_snapshot.open_1plus_deal_count)         AS rq_plus_1_open_1plus_deal_count,
      SUM(pipeline_snapshot.open_3plus_deal_count)         AS rq_plus_1_open_3plus_deal_count,
      SUM(pipeline_snapshot.open_4plus_deal_count)         AS rq_plus_1_open_4plus_deal_count,

      ------------------------------
      -- Net ARR 

      SUM(pipeline_snapshot.open_1plus_net_arr)            AS rq_plus_1_open_1plus_net_arr,
      SUM(pipeline_snapshot.open_3plus_net_arr)            AS rq_plus_1_open_3plus_net_arr,
      SUM(pipeline_snapshot.open_4plus_net_arr)            AS rq_plus_1_open_4plus_net_arr

    FROM pipeline_snapshot
    -- restrict the report to show rows in quarter plus 1 of snapshot quarter
    WHERE pipeline_snapshot.snapshot_fiscal_quarter_date = DATEADD(month, -3,pipeline_snapshot.close_fiscal_quarter_date) 
      -- exclude lost deals from pipeline
      AND pipeline_snapshot.is_lost = 0  
    GROUP BY 1,2,3,4,5,6,7,8
  
-- Quarter plus 2, from the reported quarter perspective
), report_quarter_plus_2 AS (
    
    SELECT 
      pipeline_snapshot.snapshot_fiscal_quarter_date                AS close_fiscal_quarter_date,
      pipeline_snapshot.snapshot_day_of_fiscal_quarter_normalised   AS close_day_of_fiscal_quarter_normalised,

      pipeline_snapshot.close_fiscal_quarter_name                   AS rq_plus_2_close_fiscal_quarter_name,
      pipeline_snapshot.close_fiscal_quarter_date                   AS rq_plus_2_close_fiscal_quarter_date,

      -------------------
      -- report keys
      pipeline_snapshot.report_user_segment_geo_region_area_sqs_ot,
      pipeline_snapshot.commercial_sal_category,
      pipeline_snapshot.alliance_partner,
      pipeline_snapshot.channel_category,
      -------------------
     
      SUM(pipeline_snapshot.open_1plus_deal_count)           AS rq_plus_2_open_1plus_deal_count,
      SUM(pipeline_snapshot.open_3plus_deal_count)           AS rq_plus_2_open_3plus_deal_count,
      SUM(pipeline_snapshot.open_4plus_deal_count)           AS rq_plus_2_open_4plus_deal_count,
      
      -------------------
      -- Net ARR 
      -- Use Net ARR instead

      SUM(pipeline_snapshot.open_1plus_net_arr)              AS rq_plus_2_open_1plus_net_arr,
      SUM(pipeline_snapshot.open_3plus_net_arr)              AS rq_plus_2_open_3plus_net_arr,
      SUM(pipeline_snapshot.open_4plus_net_arr)              AS rq_plus_2_open_4plus_net_arr

    FROM pipeline_snapshot
    -- restrict the report to show rows in quarter plus 2 of snapshot quarter
    WHERE pipeline_snapshot.snapshot_fiscal_quarter_date = DATEADD(month, -6,pipeline_snapshot.close_fiscal_quarter_date) 
      -- exclude lost deals from pipeline
      AND pipeline_snapshot.is_lost = 0  
    GROUP BY 1,2,3,4,5,6,7,8
  
), pipeline_gen AS (

    SELECT
      opp_history.snapshot_fiscal_quarter_date              AS close_fiscal_quarter_date,
      opp_history.snapshot_day_of_fiscal_quarter_normalised AS close_day_of_fiscal_quarter_normalised,

      -------------------
      -- report keys
      opp_history.report_user_segment_geo_region_area_sqs_ot,
      opp_history.commercial_sal_category,
      opp_history.alliance_partner,
      opp_history.channel_category,
      -------------------

      SUM(opp_history.created_in_snapshot_quarter_deal_count)     AS pipe_gen_count,

      -- Net ARR 
      SUM(opp_history.created_in_snapshot_quarter_net_arr)        AS pipe_gen_net_arr

    FROM sfdc_opportunity_snapshot_history_xf opp_history
    -- restrict the rows to pipeline created on the quarter of the snapshot
    WHERE opp_history.snapshot_fiscal_quarter_name = opp_history.pipeline_created_fiscal_quarter_name
      AND opp_history.is_eligible_created_pipeline_flag = 1
    GROUP BY 1,2,3,4,5,6


--Sales Accepted Opportunities
), sao_gen AS (

    SELECT
      opp_history.snapshot_fiscal_quarter_date              AS close_fiscal_quarter_date,
      opp_history.snapshot_day_of_fiscal_quarter_normalised AS close_day_of_fiscal_quarter_normalised,

      -------------------
      -- report keys
      opp_history.report_user_segment_geo_region_area_sqs_ot,
      opp_history.commercial_sal_category,
      opp_history.alliance_partner,
      opp_history.channel_category,
      -------------------

      SUM(opp_history.calculated_deal_count)     AS sao_deal_count,

      -- Net ARR 
      SUM(opp_history.net_arr)                  AS sao_net_arr

    FROM sfdc_opportunity_snapshot_history_xf opp_history
    -- restrict the rows to pipeline created on the quarter of the snapshot
    WHERE opp_history.snapshot_fiscal_quarter_name = opp_history.sales_accepted_fiscal_quarter_name
      AND opp_history.is_eligible_sao_flag = 1
    GROUP BY 1,2,3,4,5,6
    
    
), consolidated_targets AS (
  
 SELECT 
        opp_snapshot.snapshot_fiscal_quarter_name   AS close_fiscal_quarter_name,
        opp_snapshot.snapshot_fiscal_quarter_date   AS close_fiscal_quarter_date,
        -------------------------
        -- keys
        opp_snapshot.report_user_segment_geo_region_area_sqs_ot,
        opp_snapshot.commercial_sal_category,
        opp_snapshot.alliance_partner,
        opp_snapshot.channel_category,
        -------------------------

        SUM(CASE 
                WHEN opp_snapshot.close_fiscal_quarter_date = opp_snapshot.snapshot_fiscal_quarter_date
                    THEN opp_snapshot.booked_net_arr
                ELSE 0
             END)                                               AS total_booked_net_arr,
        SUM(CASE 
                WHEN opp_snapshot.close_fiscal_quarter_date = opp_snapshot.snapshot_fiscal_quarter_date
                    THEN opp_snapshot.churned_contraction_net_arr
                ELSE 0
             END)                                               AS total_churned_contraction_net_arr,       
        SUM(CASE 
                WHEN opp_snapshot.close_fiscal_quarter_date = opp_snapshot.snapshot_fiscal_quarter_date
                    THEN opp_snapshot.booked_deal_count
                ELSE 0
             END)                                               AS total_booked_deal_count,
        SUM(CASE 
                WHEN opp_snapshot.close_fiscal_quarter_date = opp_snapshot.snapshot_fiscal_quarter_date
                    THEN opp_snapshot.churned_contraction_deal_count
                ELSE 0
        END)                                                    AS total_churned_contraction_deal_count,   
        
        -- Pipe gen totals
        SUM(CASE 
                WHEN opp_snapshot.pipeline_created_fiscal_quarter_date = opp_snapshot.snapshot_fiscal_quarter_date
                  AND opp_snapshot.is_eligible_created_pipeline_flag = 1
                    THEN opp_snapshot.created_in_snapshot_quarter_net_arr
                ELSE 0
             END )                                              AS total_pipe_generation_net_arr,
        SUM(CASE 
                WHEN opp_snapshot.pipeline_created_fiscal_quarter_date = opp_snapshot.snapshot_fiscal_quarter_date
                  AND opp_snapshot.is_eligible_created_pipeline_flag = 1
                    THEN opp_snapshot.created_in_snapshot_quarter_deal_count
                ELSE 0
             END )                                              AS total_pipe_generation_deal_count,
        
        -- SAO totals per quarter
        SUM(CASE 
                WHEN opp_snapshot.sales_accepted_fiscal_quarter_date = opp_snapshot.snapshot_fiscal_quarter_date
                  AND opp_snapshot.is_eligible_sao_flag = 1
                    THEN opp_snapshot.net_arr
                ELSE 0
             END )                                              AS total_sao_generation_net_arr,
        SUM(CASE 
                WHEN opp_snapshot.sales_accepted_fiscal_quarter_date = opp_snapshot.snapshot_fiscal_quarter_date
                  AND opp_snapshot.is_eligible_sao_flag = 1
                    THEN opp_snapshot.calculated_deal_count
                ELSE 0
             END )                                              AS total_sao_generation_deal_count,
        
        -- Created & Landed totals
        SUM(CASE 
                WHEN opp_snapshot.close_fiscal_quarter_date = opp_snapshot.snapshot_fiscal_quarter_date
                    THEN opp_snapshot.created_and_won_same_quarter_net_arr
                ELSE 0
             END)                                               AS total_created_and_booked_same_quarter_net_arr
   FROM sfdc_opportunity_snapshot_history_xf opp_snapshot
   WHERE opp_snapshot.is_excluded_flag = 0
     AND opp_snapshot.is_deleted = 0
     AND opp_snapshot.snapshot_day_of_fiscal_quarter_normalised = 90
   GROUP BY 1,2,3,4,5,6
    
-- These CTE builds a complete set of values 
), key_fields AS (
  
    
    SELECT *

    FROM (SELECT DISTINCT report_user_segment_geo_region_area_sqs_ot
            FROM sfdc_opportunity_snapshot_history_xf) a
    CROSS JOIN (SELECT DISTINCT snapshot_fiscal_quarter_date AS close_fiscal_quarter_date
            FROM sfdc_opportunity_snapshot_history_xf) b
    CROSS JOIN (SELECT DISTINCT commercial_sal_category
            FROM sfdc_opportunity_snapshot_history_xf) c    
    CROSS JOIN (SELECT DISTINCT alliance_partner
            FROM sfdc_opportunity_snapshot_history_xf) d 
    CROSS JOIN (SELECT DISTINCT channel_category
            FROM sfdc_opportunity_snapshot_history_xf) e

), base_fields AS (
  
  SELECT 
      key_fields.*,
      close_date.fiscal_quarter_name_fy               AS close_fiscal_quarter_name,
      close_date.date_actual                          AS close_date,
      close_date.day_of_fiscal_quarter_normalised     AS close_day_of_fiscal_quarter_normalised,
      close_date.fiscal_year                          AS close_fiscal_year,
      rq_plus_1.first_day_of_fiscal_quarter           AS rq_plus_1_close_fiscal_quarter_date,
      rq_plus_1.fiscal_quarter_name_fy                AS rq_plus_1_close_fiscal_quarter_name,
      rq_plus_2.first_day_of_fiscal_quarter           AS rq_plus_2_close_fiscal_quarter_date,
      rq_plus_2.fiscal_quarter_name_fy                AS rq_plus_2_close_fiscal_quarter_name
  FROM key_fields
  INNER JOIN date_details close_date
    ON close_date.first_day_of_fiscal_quarter = key_fields.close_fiscal_quarter_date
  LEFT JOIN date_details rq_plus_1
    ON rq_plus_1.date_actual = dateadd(month,3,close_date.first_day_of_fiscal_quarter) 
  LEFT JOIN date_details rq_plus_2
    ON rq_plus_2.date_actual = dateadd(month,6,close_date.first_day_of_fiscal_quarter)  

), consolidated_metrics AS (
      
    SELECT 
      -----------------------------
      -- keys
      base_fields.report_user_segment_geo_region_area_sqs_ot,
      base_fields.commercial_sal_category,
      base_fields.alliance_partner,
      base_fields.channel_category,
      -----------------------------

      base_fields.close_fiscal_quarter_date,
      base_fields.close_fiscal_quarter_name,
      base_fields.close_fiscal_year,
      base_fields.close_date,
      base_fields.close_day_of_fiscal_quarter_normalised,
      -----------------------------

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

      agg_demo_keys.report_user_segment_geo_region_area,
    
      COALESCE(LOWER(base_fields.alliance_partner),'other')                      AS key_alliance_partner,
      CONCAT(COALESCE(LOWER(base_fields.alliance_partner),'other'),'_',COALESCE(agg_demo_keys.key_ot,'other')) AS key_alliance_partner_ot,
  
      -- key channel categories
      COALESCE(LOWER(base_fields.channel_category),'other')                      AS key_channel_category,
    
        CONCAT(COALESCE(LOWER(base_fields.channel_category),'other'),'_',COALESCE(agg_demo_keys.key_segment,'other'))     AS key_channel_category_segment,
        CONCAT(COALESCE(LOWER(base_fields.channel_category),'other'),'_',COALESCE(agg_demo_keys.key_segment_geo,'other')) AS key_channel_category_segment_geo,
  
    
    
  
      CONCAT(COALESCE(LOWER(base_fields.channel_category),'other'),'_',COALESCE(agg_demo_keys.key_ot,'other')) AS key_channel_category_ot,
      CONCAT(COALESCE(LOWER(base_fields.channel_category),'other'),'_',COALESCE(agg_demo_keys.key_segment_ot,'other')) AS key_channel_category_segment_ot,
     CONCAT(COALESCE(LOWER(base_fields.channel_category),'other'),'_',COALESCE(agg_demo_keys.key_segment_geo_ot,'other')) AS key_channel_category_segment_geo_ot,
    
        COALESCE(CONCAT(agg_demo_keys.key_segment,'_',LOWER(base_fields.commercial_sal_category)),'other')      AS key_segment_comcat,
        COALESCE(CONCAT(agg_demo_keys.key_segment_geo,'_',LOWER(base_fields.commercial_sal_category)),'other')  AS key_segment_geo_comcat,

    
    
    
      -- used to track the latest updated day in the model
      -- this might be different to the latest available information in the source models
      -- as dbt runs are not necesarly in synch
      CASE 
        WHEN base_fields.close_date = CURRENT_DATE
          THEN 1
          ELSE 0
      END                                                         AS is_today_flag,

      -- report quarter plus 1 / 2 date fields
      base_fields.rq_plus_1_close_fiscal_quarter_name,
      base_fields.rq_plus_1_close_fiscal_quarter_date,
      base_fields.rq_plus_2_close_fiscal_quarter_name,
      base_fields.rq_plus_2_close_fiscal_quarter_date,    
  
      -- reported quarter
      COALESCE(reported_quarter.deal_count,0)                     AS deal_count,
      COALESCE(reported_quarter.open_1plus_deal_count,0)          AS open_1plus_deal_count,
      COALESCE(reported_quarter.open_3plus_deal_count,0)          AS open_3plus_deal_count,
      COALESCE(reported_quarter.open_4plus_deal_count,0)          AS open_4plus_deal_count, 
      COALESCE(reported_quarter.booked_deal_count,0)              AS booked_deal_count,
      -- churned deal count
      COALESCE(reported_quarter.churned_contraction_deal_count,0)         AS churned_contraction_deal_count,
     


      -- reported quarter + 1
      COALESCE(report_quarter_plus_1.rq_plus_1_open_1plus_deal_count,0)    AS rq_plus_1_open_1plus_deal_count,
      COALESCE(report_quarter_plus_1.rq_plus_1_open_3plus_deal_count,0)    AS rq_plus_1_open_3plus_deal_count,
      COALESCE(report_quarter_plus_1.rq_plus_1_open_4plus_deal_count,0)    AS rq_plus_1_open_4plus_deal_count,
  
      -- reported quarter + 2
      COALESCE(report_quarter_plus_2.rq_plus_2_open_1plus_deal_count,0)    AS rq_plus_2_open_1plus_deal_count,
      COALESCE(report_quarter_plus_2.rq_plus_2_open_3plus_deal_count,0)    AS rq_plus_2_open_3plus_deal_count,
      COALESCE(report_quarter_plus_2.rq_plus_2_open_4plus_deal_count,0)    AS rq_plus_2_open_4plus_deal_count,

      ------------------------------
      -- Net ARR 
      -- Use Net ARR instead     
      -- created and closed

      -- reported quarter
      COALESCE(reported_quarter.booked_net_arr,0)                 AS booked_net_arr,
      -- churned net_arr
      COALESCE(reported_quarter.churned_contraction_net_arr,0)    AS churned_contraction_net_arr,
      COALESCE(reported_quarter.open_1plus_net_arr,0)             AS open_1plus_net_arr,
      COALESCE(reported_quarter.open_3plus_net_arr,0)             AS open_3plus_net_arr, 
      COALESCE(reported_quarter.open_4plus_net_arr,0)             AS open_4plus_net_arr, 

      COALESCE(reported_quarter.created_and_won_same_quarter_net_arr,0)     AS created_and_won_same_quarter_net_arr,


        -- reported quarter + 1
      COALESCE(report_quarter_plus_1.rq_plus_1_open_1plus_net_arr,0)       AS rq_plus_1_open_1plus_net_arr,
      COALESCE(report_quarter_plus_1.rq_plus_1_open_3plus_net_arr,0)       AS rq_plus_1_open_3plus_net_arr,
      COALESCE(report_quarter_plus_1.rq_plus_1_open_4plus_net_arr,0)       AS rq_plus_1_open_4plus_net_arr,

      -- reported quarter + 2
      COALESCE(report_quarter_plus_2.rq_plus_2_open_1plus_net_arr,0)       AS rq_plus_2_open_1plus_net_arr,
      COALESCE(report_quarter_plus_2.rq_plus_2_open_3plus_net_arr,0)       AS rq_plus_2_open_3plus_net_arr,
      COALESCE(report_quarter_plus_2.rq_plus_2_open_4plus_net_arr,0)       AS rq_plus_2_open_4plus_net_arr,

      -- pipe gen
      COALESCE(pipeline_gen.pipe_gen_count,0)                             AS pipe_gen_count,
      COALESCE(pipeline_gen.pipe_gen_net_arr,0)                           AS pipe_gen_net_arr,

       -- sao gen
      COALESCE(sao_gen.sao_deal_count,0)                                  AS sao_deal_count,
      COALESCE(sao_gen.sao_net_arr,0)                                     AS sao_net_arr,

       -- one year ago sao gen
      COALESCE(minus_1_year_sao_gen.sao_net_arr,0)                      AS minus_1_year_sao_net_arr,
      COALESCE(minus_1_year_sao_gen.sao_deal_count,0)                   AS minus_1_year_sao_deal_count,

      -- one year ago pipe gen
      COALESCE(minus_1_year_pipe_gen.pipe_gen_net_arr,0)                AS minus_1_year_pipe_gen_net_arr,
      COALESCE(minus_1_year_pipe_gen.pipe_gen_count,0)                  AS minus_1_year_pipe_gen_deal_count,
    
    
      -----------
      -- TARGETS
      -- targets current quarter
        COALESCE(targets.total_booked_net_arr,0)                        AS target_net_arr,
        COALESCE(targets.total_booked_deal_count,0)                     AS target_deal_count,
        COALESCE(targets.total_pipe_generation_net_arr,0)               AS target_pipe_generation_net_arr, 

        COALESCE(targets.total_booked_net_arr,0)                            AS total_booked_net_arr,
        COALESCE(targets.total_churned_contraction_net_arr,0)               AS total_churned_contraction_net_arr,
        COALESCE(targets.total_booked_deal_count,0)                         AS total_booked_deal_count,
        COALESCE(targets.total_churned_contraction_deal_count,0)            AS total_churned_contraction_deal_count,        
        COALESCE(targets.total_pipe_generation_net_arr,0)                   AS total_pipe_generation_net_arr,
        COALESCE(targets.total_pipe_generation_deal_count,0)                AS total_pipe_generation_deal_count,
        COALESCE(targets.total_created_and_booked_same_quarter_net_arr,0)   AS total_created_and_booked_same_quarter_net_arr,
        COALESCE(targets.total_sao_generation_net_arr,0)                    AS total_sao_generation_net_arr,
        COALESCE(targets.total_sao_generation_deal_count,0)                 AS total_sao_generation_deal_count,

        COALESCE(targets.total_booked_net_arr,0)             AS calculated_target_net_arr, 
        COALESCE(targets.total_booked_deal_count,0)          AS calculated_target_deal_count,  
        COALESCE(targets.total_pipe_generation_net_arr,0)     AS calculated_target_pipe_generation,

        -- totals quarter plus 1
        COALESCE(rq_plus_one.total_booked_net_arr,0)            AS rq_plus_1_total_booked_net_arr,
        COALESCE(rq_plus_one.total_booked_deal_count,0)         AS rq_plus_1_total_booked_deal_count,
        COALESCE(rq_plus_one.total_booked_net_arr,0)                  AS rq_plus_1_target_net_arr,
        COALESCE(rq_plus_one.total_booked_deal_count,0)               AS rq_plus_1_target_deal_count,
        COALESCE(rq_plus_one.total_booked_net_arr,0)       AS rq_plus_1_calculated_target_net_arr,
        COALESCE(rq_plus_one.total_booked_deal_count,0)    AS rq_plus_1_calculated_target_deal_count,

         -- totals quarter plus 2
        COALESCE(rq_plus_two.total_booked_net_arr,0)              AS rq_plus_2_total_booked_net_arr,
        COALESCE(rq_plus_two.total_booked_deal_count,0)           AS rq_plus_2_total_booked_deal_count,
        COALESCE(rq_plus_two.total_booked_net_arr,0)                    AS rq_plus_2_target_net_arr,
        COALESCE(rq_plus_two.total_booked_deal_count,0)                 AS rq_plus_2_target_deal_count,
        COALESCE(rq_plus_two.total_booked_net_arr,0)         AS rq_plus_2_calculated_target_net_arr,
        COALESCE(rq_plus_two.total_booked_deal_count,0)      AS rq_plus_2_calculated_target_deal_count,

        -- totals one year ago
        COALESCE(year_minus_one.total_booked_net_arr,0)             AS minus_1_year_total_booked_net_arr,
        COALESCE(year_minus_one.total_booked_deal_count,0)          AS minus_1_year_total_booked_deal_count,
        COALESCE(year_minus_one.total_pipe_generation_net_arr,0)    AS minus_1_year_total_pipe_generation_net_arr,
        COALESCE(year_minus_one.total_pipe_generation_deal_count,0) AS minus_1_year_total_pipe_generation_deal_count,

      -- TIMESTAMP
      current_timestamp                                                 AS dbt_last_run_at

    -- created a list of all options to avoid having blanks when attaching metrics
    FROM base_fields
    -- base keys dictionary
    LEFT JOIN agg_demo_keys
      ON base_fields.report_user_segment_geo_region_area_sqs_ot = agg_demo_keys.report_user_segment_geo_region_area_sqs_ot
    -- historical quarter
    LEFT JOIN reported_quarter
      ON base_fields.close_day_of_fiscal_quarter_normalised = reported_quarter.close_day_of_fiscal_quarter_normalised
      AND base_fields.close_fiscal_quarter_date = reported_quarter.close_fiscal_quarter_date   
      AND base_fields.report_user_segment_geo_region_area_sqs_ot = reported_quarter.report_user_segment_geo_region_area_sqs_ot
      AND reported_quarter.commercial_sal_category = base_fields.commercial_sal_category
      AND reported_quarter.alliance_partner = base_fields.alliance_partner
      AND reported_quarter.channel_category = base_fields.channel_category
    -- next quarter in relation to the considered reported quarter
    LEFT JOIN  report_quarter_plus_1
      ON base_fields.close_day_of_fiscal_quarter_normalised = report_quarter_plus_1.close_day_of_fiscal_quarter_normalised
        AND base_fields.close_fiscal_quarter_date = report_quarter_plus_1.close_fiscal_quarter_date   
        AND base_fields.report_user_segment_geo_region_area_sqs_ot = report_quarter_plus_1.report_user_segment_geo_region_area_sqs_ot         
        AND report_quarter_plus_1.commercial_sal_category = base_fields.commercial_sal_category
        AND report_quarter_plus_1.alliance_partner = base_fields.alliance_partner
        AND report_quarter_plus_1.channel_category = base_fields.channel_category
    -- 2 quarters ahead in relation to the considered reported quarter
    LEFT JOIN  report_quarter_plus_2
      ON base_fields.close_day_of_fiscal_quarter_normalised = report_quarter_plus_2.close_day_of_fiscal_quarter_normalised
        AND base_fields.close_fiscal_quarter_date = report_quarter_plus_2.close_fiscal_quarter_date   
        AND base_fields.report_user_segment_geo_region_area_sqs_ot = report_quarter_plus_2.report_user_segment_geo_region_area_sqs_ot
        AND report_quarter_plus_2.commercial_sal_category = base_fields.commercial_sal_category
        AND report_quarter_plus_2.alliance_partner = base_fields.alliance_partner
        AND report_quarter_plus_2.channel_category = base_fields.channel_category
    -- Pipe generation piece
    LEFT JOIN pipeline_gen 
      ON base_fields.close_day_of_fiscal_quarter_normalised = pipeline_gen.close_day_of_fiscal_quarter_normalised
        AND base_fields.close_fiscal_quarter_date = pipeline_gen.close_fiscal_quarter_date   
        AND base_fields.report_user_segment_geo_region_area_sqs_ot = pipeline_gen.report_user_segment_geo_region_area_sqs_ot
        AND pipeline_gen.commercial_sal_category = base_fields.commercial_sal_category
        AND pipeline_gen.alliance_partner = base_fields.alliance_partner
        AND pipeline_gen.channel_category = base_fields.channel_category
    -- Sales Accepted Opportunity Generation
    LEFT JOIN sao_gen
       ON base_fields.close_day_of_fiscal_quarter_normalised = sao_gen.close_day_of_fiscal_quarter_normalised
        AND base_fields.close_fiscal_quarter_date = sao_gen.close_fiscal_quarter_date   
        AND base_fields.report_user_segment_geo_region_area_sqs_ot = sao_gen.report_user_segment_geo_region_area_sqs_ot
        AND sao_gen.commercial_sal_category = base_fields.commercial_sal_category
        AND sao_gen.alliance_partner = base_fields.alliance_partner
        AND sao_gen.channel_category = base_fields.channel_category
    -- One Year Ago  pipeline generation
    LEFT JOIN pipeline_gen  minus_1_year_pipe_gen
      ON minus_1_year_pipe_gen.close_day_of_fiscal_quarter_normalised = base_fields.close_day_of_fiscal_quarter_normalised
        AND minus_1_year_pipe_gen.close_fiscal_quarter_date = DATEADD(month, -12, base_fields.close_fiscal_quarter_date)
        AND minus_1_year_pipe_gen.report_user_segment_geo_region_area_sqs_ot = base_fields.report_user_segment_geo_region_area_sqs_ot
        AND minus_1_year_pipe_gen.commercial_sal_category = base_fields.commercial_sal_category
        AND minus_1_year_pipe_gen.alliance_partner = base_fields.alliance_partner
        AND minus_1_year_pipe_gen.channel_category = base_fields.channel_category
    -- One Year Ago Sales Accepted Opportunity Generation
    LEFT JOIN sao_gen minus_1_year_sao_gen
      ON minus_1_year_sao_gen.close_day_of_fiscal_quarter_normalised = base_fields.close_day_of_fiscal_quarter_normalised
        AND minus_1_year_sao_gen.close_fiscal_quarter_date = DATEADD(month, -12, base_fields.close_fiscal_quarter_date)
        AND minus_1_year_sao_gen.report_user_segment_geo_region_area_sqs_ot = base_fields.report_user_segment_geo_region_area_sqs_ot
        AND minus_1_year_sao_gen.commercial_sal_category = base_fields.commercial_sal_category
        AND minus_1_year_sao_gen.alliance_partner = base_fields.alliance_partner
        AND minus_1_year_sao_gen.channel_category = base_fields.channel_category
 -- current quarter
    LEFT JOIN consolidated_targets targets 
      ON targets.close_fiscal_quarter_date = base_fields.close_fiscal_quarter_date
         AND targets.report_user_segment_geo_region_area_sqs_ot = base_fields.report_user_segment_geo_region_area_sqs_ot
        AND targets.commercial_sal_category = base_fields.commercial_sal_category
        AND targets.alliance_partner = base_fields.alliance_partner
        AND targets.channel_category = base_fields.channel_category
    -- quarter plus 1 targets
    LEFT JOIN consolidated_targets rq_plus_one
      ON rq_plus_one.close_fiscal_quarter_date = base_fields.rq_plus_1_close_fiscal_quarter_date
        AND rq_plus_one.report_user_segment_geo_region_area_sqs_ot = base_fields.report_user_segment_geo_region_area_sqs_ot
        AND rq_plus_one.commercial_sal_category = base_fields.commercial_sal_category
        AND rq_plus_one.alliance_partner = base_fields.alliance_partner
        AND rq_plus_one.channel_category = base_fields.channel_category
    -- quarter plus 2 targets
    LEFT JOIN consolidated_targets rq_plus_two
      ON rq_plus_two.close_fiscal_quarter_date = base_fields.rq_plus_2_close_fiscal_quarter_date
        AND rq_plus_two.report_user_segment_geo_region_area_sqs_ot = base_fields.report_user_segment_geo_region_area_sqs_ot
        AND rq_plus_two.commercial_sal_category = base_fields.commercial_sal_category
        AND rq_plus_two.alliance_partner = base_fields.alliance_partner
        AND rq_plus_two.channel_category = base_fields.channel_category
   -- one year ago totals
    LEFT JOIN consolidated_targets year_minus_one
      ON year_minus_one.close_fiscal_quarter_date = dateadd(month,-12,base_fields.close_fiscal_quarter_date)
         AND year_minus_one.report_user_segment_geo_region_area_sqs_ot = base_fields.report_user_segment_geo_region_area_sqs_ot
        AND year_minus_one.commercial_sal_category = base_fields.commercial_sal_category
        AND year_minus_one.alliance_partner = base_fields.alliance_partner
        AND year_minus_one.channel_category = base_fields.channel_category

)

SELECT *
FROM consolidated_metrics
WHERE (total_booked_net_arr <> 0
    OR rq_plus_1_total_booked_net_arr <> 0
    OR rq_plus_2_total_booked_net_arr <> 0
    OR rq_plus_1_open_1plus_net_arr <> 0 
    OR rq_plus_2_open_1plus_net_arr <> 0 
    OR open_1plus_net_arr <> 0)

-- SELECT DISTINCT order_type_stamped FROM restricted_safe_workspace_sales.sfdc_opportunity_xf