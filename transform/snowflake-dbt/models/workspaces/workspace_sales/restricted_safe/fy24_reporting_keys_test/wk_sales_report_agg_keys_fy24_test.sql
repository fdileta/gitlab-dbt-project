{{ config(alias='report_agg_keys_fy24_test') }}

-- FY24 reporting grain: bu_geo_region_area_segment_rt_pc_ap

WITH sfdc_account_xf AS (

  SELECT *
  -- FROM prod.restricted_safe_workspace_sales.sfdc_accounts_xf_refactored_edm
  FROM {{ref('wk_sales_sfdc_accounts_xf_edm_marts')}}

), sfdc_users_xf AS (

  SELECT *
  FROM {{ref('wk_sales_sfdc_users_xf')}}
  -- FROM prod.workspace_sales.sfdc_users_xf

), mart_crm_opportunity AS (

  SELECT *
  FROM {{ref('mart_crm_opportunity')}}

), field_for_keys AS ( 

  SELECT
    CASE report_opportunity_user_segment
      WHEN 'large' THEN 'ent-g'
      WHEN 'pubsec' THEN 'ent-g'
      WHEN 'mid-market' THEN 'comm'
      WHEN 'smb' THEN 'comm'
      WHEN 'jihu' THEN 'jihu'
      ELSE 'other'
    END AS business_unit,

    CASE
      WHEN  order_type = '1. New - First Order'
          THEN 'First Order'
      WHEN lower(account_owner.role_name) like ('pooled%')
              AND key_segment IN ('smb','mid-market')
              AND order_type != '1. New - First Order'
          THEN 'Pooled'
      WHEN lower(account_owner.role_name) like ('terr%')
              AND key_segment IN ('smb','mid-market')
              AND order_type != '1. New - First Order'
          THEN 'Territory'
      WHEN lower(account_owner.role_name) like ('named%')
              AND key_segment IN ('smb','mid-market')
              AND order_type != '1. New - First Order'
          THEN 'Named'
      WHEN order_type IN ('2. New - Connected','4. Contraction','6. Churn - Final','5. Churn - Partial','3. Growth')
              AND key_segment IN ('smb','mid-market')
          THEN 'Expansion'
      ELSE 'Other'
    END AS role_type,

    CASE
      WHEN sales_qualified_source_name = 'Channel Generated'
        THEN 'Channel Generated'
      WHEN sales_qualified_source_name != 'Channel Generated'
        AND NOT(LOWER(resale.account_name) LIKE ANY ('%ibm%','%google%','%gcp%','%amazon%'))
        THEN 'Channel Co-Sell'
      WHEN sales_qualified_source_name != 'Channel Generated'
        AND LOWER(resale.account_name) LIKE ANY ('%ibm%','%google%','%gcp%','%amazon%')
        THEN 'Alliance Co-Sell'
      ELSE 'Direct'
    END                                               AS partner_category,   -- definition pending

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
    END                                               AS alliance_partner,  -- definition pending

    report_opportunity_user_segment,
    report_opportunity_user_geo,
    report_opportunity_user_region,
    report_opportunity_user_area,
    order_type AS order_type_stamped,
    sales_qualified_source_name AS sales_qualified_source,
    deal_category,
    deal_group,
    account_owner_user_segment,
    opp.account_owner_user_geo,
    opp.account_owner_user_region,
    opp.account_owner_user_area

  FROM mart_crm_opportunity AS opp
  LEFT JOIN sfdc_users_xf AS account_owner
    ON opp.owner_id = account_owner.user_id
  LEFT JOIN sfdc_account_xf AS resale
    ON opp.fulfillment_partner = resale.account_id


), eligible AS (

  SELECT
    LOWER(business_unit)                         AS business_unit,
    LOWER(report_opportunity_user_geo)           AS report_opportunity_user_geo,
    LOWER(report_opportunity_user_region)        AS report_opportunity_user_region,
    LOWER(report_opportunity_user_area)          AS report_opportunity_user_area,
    LOWER(report_opportunity_user_segment)       AS report_opportunity_user_segment,
    LOWER(role_type)                             AS role_type,
    LOWER(partner_category)                      AS partner_category,
    LOWER(alliance_partner)                      AS alliance_partner,

    -- LOWER(sales_qualified_source)                AS sales_qualified_source,
    -- LOWER(order_type_stamped)                    AS order_type_stamped,

    -- LOWER(deal_category)                         AS deal_category,
    -- LOWER(deal_group)                            AS deal_group,

    LOWER(CONCAT(business_unit, '-',report_opportunity_user_geo, '-',report_opportunity_user_region, '-',report_opportunity_user_area, '-', report_opportunity_user_segment, '-', role_type))  AS bu_geo_region_area_segment_rt,
    LOWER(CONCAT(business_unit, '-',report_opportunity_user_geo, '-',report_opportunity_user_region, '-',report_opportunity_user_area, '-', report_opportunity_user_segment, '-', role_type, '-', partner_category, '-', alliance_partner))  AS bu_geo_region_area_segment_rt_pc_ap

  FROM field_for_keys
  
  UNION ALL
  
  SELECT
    LOWER(business_unit)                         AS business_unit,
    LOWER(account_owner_user_geo)                AS report_opportunity_user_geo,
    LOWER(account_owner_user_region)             AS report_opportunity_user_region,
    LOWER(account_owner_user_area)               AS report_opportunity_user_area,
    LOWER(account_owner_user_segment)            AS report_opportunity_user_segment,
    LOWER(role_type)                             AS role_type,
    LOWER(partner_category)                      AS partner_category,
    LOWER(alliance_partner)                      AS alliance_partner,

    -- LOWER(sales_qualified_source)                AS sales_qualified_source,
    -- LOWER(order_type_stamped)                    AS order_type_stamped,

    -- LOWER(deal_category)                         AS deal_category,
    -- LOWER(deal_group)                            AS deal_group,

    LOWER(CONCAT(business_unit, '-',account_owner_user_geo, '-',account_owner_user_region, '-',account_owner_user_area, '-', account_owner_user_segment, '-', role_type))  AS bu_geo_region_area_segment_rt,
    LOWER(CONCAT(business_unit, '-',account_owner_user_geo, '-',account_owner_user_region, '-',account_owner_user_area, '-', account_owner_user_segment, '-', role_type, '-', partner_category, '-', alliance_partner))  AS bu_geo_region_area_segment_rt_pc_ap

  FROM field_for_keys
  
  
), valid_keys AS (

  SELECT DISTINCT 

  -- FY24 reporting structure
    -- BU
    -- Role Type
    -- Partner Category
    -- Alliance Partner

    -- BU - Geo
    -- Partner Category - BU
    -- Alliance Partner - BU

    -- (ENT) BU - Geo - Region
    -- (Comm) BU - Geo - Segment
    -- BU - Geo - Role Type
    -- Partner Category - BU - Geo
    -- Alliance Partner - BU - Geo

    -- (ENT) BU - Geo - Region - Area
    -- (ENT) BU - Geo - Region - Role Type
    -- (Comm) BU - Geo - Segment - Region
    -- (Comm) BU - Geo - Segment - Role Type


    -- (ENT) BU - Geo - Region - Area - Segment
    -- (ENT) BU - Geo - Region - Area - Role_Type
    -- (Comm) BU - Geo - Segment - Region - Area
    -- (Comm) BU - Geo - Segment - Region - Role_Type

    -- (ENT) BU - Geo - Region - Area - Segment - Role_Type
    -- (Comm) BU - Geo - Segment - Region - Area - Role_Type


    eligible.*,

    business_unit AS key_bu,
    role_type AS key_rt,
    partner_category AS key_pc,
    alliance_partner AS key_ap,

    business_unit || '_' || report_opportunity_user_geo AS key_bu_geo,
    partner_category || '_' || business_unit AS key_pc_bu,
    alliance_partner || '_' || business_unit AS key_ap_bu,

    business_unit || '_' || report_opportunity_user_geo || '_' || report_opportunity_user_region AS key_bu_geo_region,
    business_unit || '_' || report_opportunity_user_geo || '_' || report_opportunity_user_segment AS key_bu_geo_segment,
    business_unit || '_' || report_opportunity_user_geo || '_' || role_type AS key_bu_geo_rt,
    partner_category || '_' || business_unit || '_' || report_opportunity_user_geo AS key_pc_bu_geo,
    alliance_partner || '_' || business_unit || '_' || report_opportunity_user_geo AS key_ap_bu_geo,

    business_unit || '_' || report_opportunity_user_geo || '_' || report_opportunity_user_region || '_' || report_opportunity_user_area AS key_bu_geo_region_area,
    business_unit || '_' || report_opportunity_user_geo || '_' || report_opportunity_user_region || '_' || role_type AS key_bu_geo_region_rt,
    business_unit || '_' || report_opportunity_user_geo || '_' || report_opportunity_user_segment || '_' || report_opportunity_user_region AS key_bu_geo_segment_region,
    business_unit || '_' || report_opportunity_user_geo || '_' || report_opportunity_user_segment || '_' || role_type AS key_bu_geo_segment_rt,

    business_unit || '_' || report_opportunity_user_geo || '_' || report_opportunity_user_region || '_' || report_opportunity_user_area || '_' || report_opportunity_user_segment AS key_bu_geo_region_area_segment,
    business_unit || '_' || report_opportunity_user_geo || '_' || report_opportunity_user_region || '_' || report_opportunity_user_area || '_' || role_type AS key_bu_geo_region_area_rt,
    business_unit || '_' || report_opportunity_user_geo || '_' || report_opportunity_user_segment || '_' || report_opportunity_user_region || '_' || report_opportunity_user_area AS key_bu_geo_segment_region_area,
    business_unit || '_' || report_opportunity_user_geo || '_' || report_opportunity_user_segment || '_' || report_opportunity_user_region || '_' || role_type AS key_bu_geo_segment_region_rt,

    business_unit || '_' || report_opportunity_user_geo || '_' || report_opportunity_user_region || '_' || report_opportunity_user_area || '_' || report_opportunity_user_segment || '_' || role_type AS key_bu_geo_region_area_segment_rt,
    business_unit || '_' || report_opportunity_user_geo || '_' || report_opportunity_user_segment || '_' || report_opportunity_user_region || '_' || report_opportunity_user_area || '_' || role_type AS key_bu_geo_segment_region_area_rt,

    
    --------------------------------- FY23 cuts   
    -- report_opportunity_user_segment   AS key_segment,
    -- sales_qualified_source            AS key_sqs,
    -- deal_group                        AS key_ot,

    -- report_opportunity_user_segment || '_' || sales_qualified_source             AS key_segment_sqs,
    -- report_opportunity_user_segment || '_' || deal_group                         AS key_segment_ot,    

    -- report_opportunity_user_segment || '_' || report_opportunity_user_geo                                               AS key_segment_geo,
    -- report_opportunity_user_segment || '_' || report_opportunity_user_geo || '_' ||  sales_qualified_source             AS key_segment_geo_sqs,
    -- report_opportunity_user_segment || '_' || report_opportunity_user_geo || '_' ||  deal_group                         AS key_segment_geo_ot,      


    -- report_opportunity_user_segment || '_' || report_opportunity_user_geo || '_' || report_opportunity_user_region                                     AS key_segment_geo_region,
    -- report_opportunity_user_segment || '_' || report_opportunity_user_geo || '_' || report_opportunity_user_region || '_' ||  sales_qualified_source   AS key_segment_geo_region_sqs,
    -- report_opportunity_user_segment || '_' || report_opportunity_user_geo || '_' || report_opportunity_user_region || '_' ||  deal_group               AS key_segment_geo_region_ot,   

    -- report_opportunity_user_segment || '_' || report_opportunity_user_geo || '_' || report_opportunity_user_region || '_' || report_opportunity_user_area                                       AS key_segment_geo_region_area,
    -- report_opportunity_user_segment || '_' || report_opportunity_user_geo || '_' || report_opportunity_user_region || '_' || report_opportunity_user_area || '_' ||  sales_qualified_source     AS key_segment_geo_region_area_sqs,
    -- report_opportunity_user_segment || '_' || report_opportunity_user_geo || '_' || report_opportunity_user_region || '_' || report_opportunity_user_area || '_' ||  deal_group                 AS key_segment_geo_region_area_ot,


    -- report_opportunity_user_segment || '_' || report_opportunity_user_geo || '_' || report_opportunity_user_area                                       AS key_segment_geo_area,

    COALESCE(report_opportunity_user_segment ,'other')                                    AS sales_team_cro_level,
  
  
    --------------------------------- FY22 cuts
    -- NF: This code replicates the reporting structured of FY22, to keep current tools working
    CASE 
      WHEN report_opportunity_user_segment = 'large'
        AND report_opportunity_user_geo = 'emea'
          THEN 'large_emea'
      WHEN report_opportunity_user_segment = 'mid-market'
        AND report_opportunity_user_region = 'amer'
        AND lower(report_opportunity_user_area) LIKE '%west%'
          THEN 'mid-market_west'
      WHEN report_opportunity_user_segment = 'mid-market'
        AND report_opportunity_user_region = 'amer'
        AND lower(report_opportunity_user_area) NOT LIKE '%west%'
          THEN 'mid-market_east'
      WHEN report_opportunity_user_segment = 'smb'
        AND report_opportunity_user_region = 'amer'
        AND lower(report_opportunity_user_area) LIKE '%west%'
          THEN 'smb_west'
      WHEN report_opportunity_user_segment = 'smb'
        AND report_opportunity_user_region = 'amer'
        AND lower(report_opportunity_user_area) NOT LIKE '%west%'
          THEN 'smb_east'
      WHEN report_opportunity_user_segment = 'smb'
        AND report_opportunity_user_region = 'latam'
          THEN 'smb_east'
      WHEN (report_opportunity_user_segment IS NULL
            OR report_opportunity_user_region IS NULL)
          THEN 'other'
      WHEN CONCAT(report_opportunity_user_segment,'_',report_opportunity_user_region) like '%other%'
        THEN 'other'
      ELSE CONCAT(report_opportunity_user_segment,'_',report_opportunity_user_region)
    END                                                                           AS sales_team_rd_asm_level,

    COALESCE(CONCAT(report_opportunity_user_segment,'_',report_opportunity_user_geo),'other')                                                                      AS sales_team_vp_level,
    COALESCE(CONCAT(report_opportunity_user_segment,'_',report_opportunity_user_geo,'_',report_opportunity_user_region),'other')                                   AS sales_team_avp_rd_level,
    COALESCE(CONCAT(report_opportunity_user_segment,'_',report_opportunity_user_geo,'_',report_opportunity_user_region,'_',report_opportunity_user_area),'other')  AS sales_team_asm_level

  FROM eligible
  
 )
 
 SELECT *
 FROM valid_keys