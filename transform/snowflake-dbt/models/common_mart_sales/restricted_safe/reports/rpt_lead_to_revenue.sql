{{ simple_cte([
    ('opportunity_base','mart_crm_opportunity'),
    ('person_base','mart_crm_person'),
    ('dim_crm_person','dim_crm_person'),
    ('mart_crm_opportunity_stamped_hierarchy_hist','mart_crm_opportunity_stamped_hierarchy_hist'),
    ('rpt_sfdc_bizible_tp_opp_linear_blended','rpt_sfdc_bizible_tp_opp_linear_blended'),
    ('dim_crm_account','dim_crm_account')
]) }}

, upa_base AS ( --pulls every account and it's UPA
  
    SELECT 
      dim_parent_crm_account_id,
      dim_crm_account_id
    FROM dim_crm_account

), first_order_opps AS ( -- pulls only FO CW Opps and their UPA/Account ID

    SELECT
      dim_parent_crm_account_id,
      dim_crm_account_id,
      dim_crm_opportunity_id,
      close_date,
      is_sao,
      sales_accepted_date
    FROM opportunity_base
    WHERE is_won = true
      AND order_type = '1. New - First Order'

), accounts_with_first_order_opps AS ( -- shows only UPA/Account with a FO Available Opp on it

    SELECT
      upa_base.dim_parent_crm_account_id,
      upa_base.dim_crm_account_id,
      first_order_opps.dim_crm_opportunity_id,
      FALSE AS is_first_order_available
    FROM upa_base 
    LEFT JOIN first_order_opps
      ON upa_base.dim_crm_account_id=first_order_opps.dim_crm_account_id
    WHERE dim_crm_opportunity_id IS NOT NULL

), person_order_type_base AS (

    SELECT DISTINCT
      person_base.email_hash, 
      person_base.dim_crm_account_id,
      person_base.mql_date_lastest_pt,
      upa_base.dim_parent_crm_account_id,
      opportunity_base.dim_crm_opportunity_id,
      opportunity_base.close_date,
      opportunity_base.order_type,
      CASE 
         WHEN is_first_order_available = False AND opportunity_base.order_type = '1. New - First Order' THEN '3. Growth'
         WHEN is_first_order_available = False AND opportunity_base.order_type != '1. New - First Order' THEN opportunity_base.order_type
      ELSE '1. New - First Order'
      END AS person_order_type,
      ROW_NUMBER() OVER( PARTITION BY email_hash ORDER BY person_order_type) AS person_order_type_number
    FROM person_base
    FULL JOIN upa_base
      ON person_base.dim_crm_account_id=upa_base.dim_crm_account_id
    LEFT JOIN accounts_with_first_order_opps
      ON upa_base.dim_parent_crm_account_id = accounts_with_first_order_opps.dim_parent_crm_account_id
    FULL JOIN opportunity_base
      ON upa_base.dim_parent_crm_account_id=opportunity_base.dim_parent_crm_account_id

), person_order_type_final AS (

    SELECT DISTINCT
      person_order_type_base.email_hash,
      dim_crm_person.sfdc_record_id,
      person_order_type_base.mql_date_lastest_pt,
      person_order_type_base.dim_crm_opportunity_id,
      person_order_type_base.close_date,
      person_order_type_base.order_type,
      person_order_type_base.dim_parent_crm_account_id,
      person_order_type_base.dim_crm_account_id,
      person_order_type_base.person_order_type
    FROM person_order_type_base
    INNER JOIN dim_crm_person ON
    person_order_type_base.email_hash=dim_crm_person.email_hash
    WHERE person_order_type_number=1

), mql_order_type_base AS (

    SELECT DISTINCT
      dim_crm_person.sfdc_record_id,
      person_base.email_hash, 
      CASE 
         WHEN mql_date_lastest_pt < opportunity_base.close_date THEN opportunity_base.order_type
         WHEN mql_date_lastest_pt > opportunity_base.close_date THEN '3. Growth'
      ELSE null
      END AS mql_order_type_historical,
      ROW_NUMBER() OVER( PARTITION BY person_base.email_hash ORDER BY mql_order_type_historical) AS mql_order_type_number
    FROM person_base
    INNER JOIN dim_crm_person ON
    person_base.dim_crm_person_id=dim_crm_person.dim_crm_person_id
    FULL JOIN upa_base ON 
    person_base.dim_crm_account_id=upa_base.dim_crm_account_id
    LEFT JOIN accounts_with_first_order_opps ON
    upa_base.dim_parent_crm_account_id = accounts_with_first_order_opps.dim_parent_crm_account_id
    FULL JOIN opportunity_base ON
    upa_base.dim_parent_crm_account_id=opportunity_base.dim_parent_crm_account_id
    
), mql_order_type_final AS (
  
  SELECT *
  FROM mql_order_type_base
  WHERE mql_order_type_number=1
    
), inquiry_order_type_base AS (

    SELECT DISTINCT
      dim_crm_person.sfdc_record_id,
      person_base.email_hash, 
      CASE 
         WHEN true_inquiry_date < opportunity_base.close_date THEN opportunity_base.order_type
         WHEN true_inquiry_date > opportunity_base.close_date THEN '3. Growth'
      ELSE null
      END AS inquiry_order_type_historical,
      ROW_NUMBER() OVER( PARTITION BY person_base.email_hash ORDER BY inquiry_order_type_historical) AS inquiry_order_type_number
    FROM person_base
    INNER JOIN dim_crm_person ON
    person_base.dim_crm_person_id=dim_crm_person.dim_crm_person_id
    FULL JOIN upa_base ON 
    person_base.dim_crm_account_id=upa_base.dim_crm_account_id
    LEFT JOIN accounts_with_first_order_opps ON
    upa_base.dim_parent_crm_account_id = accounts_with_first_order_opps.dim_parent_crm_account_id
    FULL JOIN opportunity_base ON
    upa_base.dim_parent_crm_account_id=opportunity_base.dim_parent_crm_account_id

), inquiry_order_type_final AS (
  
  SELECT *
  FROM inquiry_order_type_base
  WHERE inquiry_order_type_number=1
  
), order_type_final AS (
  
  SELECT 
    person_order_type_final.sfdc_record_id,
    person_order_type_final.email_hash,
    person_order_type_final.dim_crm_account_id,
    person_order_type_final.mql_date_lastest_pt,
    person_order_type_final.close_date,
    person_order_type_final.dim_parent_crm_account_id,
    person_order_type_final.dim_crm_opportunity_id,
    person_order_type_final.order_type,
    person_order_type_final.person_order_type,
    inquiry_order_type_final.inquiry_order_type_historical,
    mql_order_type_final.mql_order_type_historical
  FROM person_order_type_final
  LEFT JOIN inquiry_order_type_final ON
  person_order_type_final.email_hash=inquiry_order_type_final.email_hash
  LEFT JOIN mql_order_type_final ON
  person_order_type_final.email_hash=mql_order_type_final.email_hash

  ), cohort_base AS (

    SELECT DISTINCT
      person_base.email_hash,
      person_base.email_domain_type,
      person_base.true_inquiry_date,
      person_base.mql_date_lastest_pt,
      person_base.status,
      person_base.lead_source,
      person_base.dim_crm_person_id,
      person_base.dim_crm_account_id,
      person_base.is_mql,
      dim_crm_person.sfdc_record_id,
      person_base.account_demographics_sales_segment,
      person_base.account_demographics_region,
      person_base.account_demographics_geo,
      person_base.account_demographics_area,
      person_base.account_demographics_upa_country,
      person_base.account_demographics_territory,
      dim_crm_person.employee_bucket,
      dim_crm_person.leandata_matched_account_sales_segment,
      COALESCE(dim_crm_person.account_demographics_employee_count,dim_crm_person.zoominfo_company_employee_count,dim_crm_person.cognism_employee_count) AS employee_count,
      lower(COALESCE(dim_crm_person.zoominfo_company_country,dim_crm_person.zoominfo_contact_country,dim_crm_person.cognism_company_office_country,dim_crm_person.cognism_country)) 
                                                                                                                                                        AS first_country,
      is_first_order_available,
      order_type_final.person_order_type,
      order_type_final.inquiry_order_type_historical,
      order_type_final.mql_order_type_historical,
      opp.order_type AS opp_order_type,
      opp.sales_qualified_source_name,
      opp.deal_path_name,
      opp.sales_type,
      opp.dim_crm_opportunity_id,
      opp.sales_accepted_date,
      opp.created_date AS opp_created_date,
      opp.close_date,
      opp.is_won,
      opp.is_sao,
      opp.new_logo_count,
      opp.net_arr,
      opp.is_net_arr_closed_deal,
      opp.crm_opp_owner_sales_segment_stamped,
      opp.crm_opp_owner_region_stamped,
      opp.crm_opp_owner_area_stamped,
      opp.crm_opp_owner_geo_stamped,
      opp.parent_crm_account_demographics_upa_country,
      opp.parent_crm_account_demographics_territory
    FROM person_base
    INNER JOIN dim_crm_person
      ON person_base.dim_crm_person_id=dim_crm_person.dim_crm_person_id
    LEFT JOIN upa_base
    ON person_base.dim_crm_account_id=upa_base.dim_crm_account_id
    LEFT JOIN accounts_with_first_order_opps
      ON upa_base.dim_parent_crm_account_id = accounts_with_first_order_opps.dim_parent_crm_account_id
    FULL JOIN mart_crm_opportunity_stamped_hierarchy_hist opp
      ON upa_base.dim_parent_crm_account_id=opp.dim_parent_crm_account_id
    LEFT JOIN order_type_final
      ON person_base.email_hash=order_type_final.email_hash

), fo_inquiry_with_tp AS (
  
  SELECT DISTINCT
  
    --Key IDs
    cohort_base.email_hash,
    cohort_base.dim_crm_person_id,
    cohort_base.dim_crm_opportunity_id,
    rpt_sfdc_bizible_tp_opp_linear_blended.dim_crm_touchpoint_id,
    cohort_base.sfdc_record_id,
  
    --person data
    CASE 
      WHEN cohort_base.person_order_type IS null AND cohort_base.opp_order_type IS null THEN 'Missing order_type_name'
      WHEN cohort_base.person_order_type IS null THEN cohort_base.opp_order_type
      ELSE person_order_type
    END AS person_order_type,
    cohort_base.inquiry_order_type_historical,
    cohort_base.mql_order_type_historical,
    cohort_base.lead_source,    
    cohort_base.email_domain_type,
    cohort_base.is_mql,
    cohort_base.account_demographics_sales_segment,
    cohort_base.account_demographics_geo,
    cohort_base.account_demographics_region,
    cohort_base.account_demographics_area,
    cohort_base.account_demographics_upa_country,
    cohort_base.account_demographics_territory,
    cohort_base.true_inquiry_date,
    cohort_base.mql_date_lastest_pt,
    CASE
    WHEN LOWER(leandata_matched_account_sales_segment) = 'pubsec' THEN 'PubSec'
    WHEN employee_count >=1 AND employee_count < 100 THEN 'SMB'
    WHEN employee_count >=100 AND employee_count < 2000 THEN 'MM'
    WHEN employee_count >=2000  THEN 'Large'
    ELSE 'Unknown'
  END AS employee_count_segment,
  CASE 
    WHEN LOWER(leandata_matched_account_sales_segment) = 'pubsec' THEN 'PubSec'
    WHEN employee_bucket = '1-99' THEN 'SMB'
    WHEN employee_bucket = '100-499' THEN 'MM'
    WHEN employee_bucket = '500-1,999' THEN 'MM'
    WHEN employee_bucket = '2,000-9,999' THEN 'Large'
    WHEN employee_bucket = '10,000+' THEN 'Large'
    ELSE 'Unknown'
  END AS employee_bucket_segment,
  CASE
    WHEN first_country = 'afghanistan' THEN 'emea'
    WHEN first_country = 'aland islands' THEN 'emea'
    WHEN first_country = 'albania' THEN 'emea'
    WHEN first_country = 'algeria' THEN 'emea'
    WHEN first_country = 'andorra' THEN 'emea'
    WHEN first_country = 'angola' THEN 'emea'
    WHEN first_country = 'anguilla' THEN 'amer'
    WHEN first_country = 'antigua and barbuda' THEN 'amer'
    WHEN first_country = 'argentina' THEN 'amer'
    WHEN first_country = 'armenia' THEN 'emea'
    WHEN first_country = 'aruba' THEN 'amer'
    WHEN first_country = 'australia' THEN 'apac'
    WHEN first_country = 'austria' THEN 'emea'
    WHEN first_country = 'azerbaijan' THEN 'emea'
    WHEN first_country = 'bahamas' THEN 'amer'
    WHEN first_country = 'bahrain' THEN 'emea'
    WHEN first_country = 'bangladesh' THEN 'apac'
    WHEN first_country = 'barbados' THEN 'amer'
    WHEN first_country = 'belarus' THEN 'emea'
    WHEN first_country = 'belgium' THEN 'emea'
    WHEN first_country = 'belize' THEN 'amer'
    WHEN first_country = 'benin' THEN 'emea'
    WHEN first_country = 'bermuda' THEN 'amer'
    WHEN first_country = 'bhutan' THEN 'apac'
    WHEN first_country = 'bolivia' THEN 'amer'
    WHEN first_country = 'bosnia and herzegovina' THEN 'emea'
    WHEN first_country = 'bosnia and herzegowina' THEN 'emea'
    WHEN first_country = 'botswana' THEN 'emea'
    WHEN first_country = 'brazil' THEN 'amer'
    WHEN first_country = 'british virgin islands' THEN 'amer'
    WHEN first_country = 'brunei darussalam' THEN 'apac'
    WHEN first_country = 'bulgaria' THEN 'emea'
    WHEN first_country = 'burkina faso' THEN 'emea'
    WHEN first_country = 'burundi' THEN 'emea'
    WHEN first_country = 'cambodia' THEN 'apac'
    WHEN first_country = 'cameroon' THEN 'emea'
    WHEN first_country = 'canada' THEN 'amer'
    WHEN first_country = 'cape verde' THEN 'emea'
    WHEN first_country = 'caribbean netherlands' THEN 'amer'
    WHEN first_country = 'cayman islands' THEN 'amer'
    WHEN first_country = 'central african republic' THEN 'emea'
    WHEN first_country = 'chad' THEN 'emea'
    WHEN first_country = 'chile' THEN 'amer'
    WHEN first_country = 'china' THEN 'jihu'
    WHEN first_country = 'colombia' THEN 'amer'
    WHEN first_country = 'comoros' THEN 'emea'
    WHEN first_country = 'congo' THEN 'emea'
    WHEN first_country = 'cook islands' THEN 'apac'
    WHEN first_country = 'costa rica' THEN 'amer'
    WHEN first_country = "cote d\'ivoire" THEN 'emea'
    WHEN first_country = 'croatia' THEN 'emea'
    WHEN first_country = 'cuba' THEN 'amer'
    WHEN first_country = 'curacao' THEN 'amer'
    WHEN first_country = 'cyprus' THEN 'emea'
    WHEN first_country = 'czech republic' THEN 'emea'
    WHEN first_country = 'democratic republic of the congo' THEN 'emea'
    WHEN first_country = 'denmark' THEN 'emea'
    WHEN first_country = 'djibouti' THEN 'emea'
    WHEN first_country = 'dominica' THEN 'amer'
    WHEN first_country = 'dominican republic' THEN 'amer'
    WHEN first_country = 'ecuador' THEN 'amer'
    WHEN first_country = 'egypt' THEN 'emea'
    WHEN first_country = 'el salvador' THEN 'amer'
    WHEN first_country = 'equatorial guinea' THEN 'emea'
    WHEN first_country = 'eritrea' THEN 'emea'
    WHEN first_country = 'estonia' THEN 'emea'
    WHEN first_country = 'eswatini' THEN 'emea'
    WHEN first_country = 'ethiopia' THEN 'emea'
    WHEN first_country = 'falkland islands' THEN 'amer'
    WHEN first_country = 'faroe islands' THEN 'emea'
    WHEN first_country = 'fiji' THEN 'apac'
    WHEN first_country = 'finland' THEN 'emea'
    WHEN first_country = 'france' THEN 'emea'
    WHEN first_country = 'french guiana' THEN 'amer'
    WHEN first_country = 'french polynesia' THEN 'apac'
    WHEN first_country = 'gabon' THEN 'emea'
    WHEN first_country = 'gambia' THEN 'emea'
    WHEN first_country = 'georgia' THEN 'emea'
    WHEN first_country = 'germany' THEN 'emea'
    WHEN first_country = 'ghana' THEN 'emea'
    WHEN first_country = 'gibraltar' THEN 'emea'
    WHEN first_country = 'greece' THEN 'emea'
    WHEN first_country = 'greenland' THEN 'emea'
    WHEN first_country = 'grenada' THEN 'amer'
    WHEN first_country = 'guadeloupe' THEN 'amer'
    WHEN first_country = 'guatemala' THEN 'amer'
    when first_country = 'guernsey' THEN 'emea'
    WHEN first_country = 'guinea' THEN 'emea'
    WHEN first_country = 'guinea-bissau' THEN 'emea'
    WHEN first_country = 'guyana' THEN 'amer'
    WHEN first_country = 'haiti' THEN 'amer'
    WHEN first_country = 'honduras' THEN 'amer'
    WHEN first_country = 'hungary' THEN 'emea'
    WHEN first_country = 'iceland' THEN 'emea'
    WHEN first_country = 'india' THEN 'apac'
    WHEN first_country = 'indonesia' THEN 'apac'
    WHEN first_country = 'iran' THEN 'emea'
    WHEN first_country = 'iran' THEN 'emea'
    WHEN first_country = 'iran, islamic republic of' THEN 'emea'
    WHEN first_country = 'ireland' THEN 'emea'
    WHEN first_country = 'isle of man' THEN 'emea'
    WHEN first_country = 'israel' THEN 'emea'
    WHEN first_country = 'italy' THEN 'emea'
    WHEN first_country = 'ivory coast' THEN 'emea'
    WHEN first_country = "côte d\'ivoire" THEN 'emea'
    WHEN first_country = 'jamaica' THEN 'amer'
    WHEN first_country = 'japan' THEN 'apac'
    WHEN first_country = 'jersey' THEN 'emea'
    WHEN first_country = 'jordan' THEN 'emea'
    WHEN first_country = 'kazakhstan' THEN 'emea'
    WHEN first_country = 'kenya' THEN 'emea'
    WHEN first_country = 'kosovo' THEN 'emea'
    WHEN first_country = 'kuwait' THEN 'emea'
    WHEN first_country = 'kyrgyzstan' THEN 'emea'
    WHEN first_country = "lao people\'s democratic republic" THEN 'apac'
    WHEN first_country = 'latvia' THEN 'emea'
    WHEN first_country = 'lebanon' THEN 'emea'
    WHEN first_country = 'lesotho' THEN 'emea'
    WHEN first_country = 'liberia' THEN 'emea'
    WHEN first_country = 'libya' THEN 'emea'
    WHEN first_country = 'liechtenstein' THEN 'emea'
    WHEN first_country = 'lithuania' THEN 'emea'
    WHEN first_country = 'luxembourg' THEN 'emea'
    WHEN first_country = 'macao' THEN 'jihu'
    WHEN first_country = 'macedonia' THEN 'emea'
    WHEN first_country = 'madagascar' THEN 'emea'
    WHEN first_country = 'malawi' THEN 'emea'
    WHEN first_country = 'malaysia' THEN 'apac'
    WHEN first_country = 'maldives' THEN 'apac'
    WHEN first_country = 'mali' THEN 'emea'
    WHEN first_country = 'malta' THEN 'emea'
    WHEN first_country = 'martinique' THEN 'amer'
    WHEN first_country = 'mauritania' THEN 'emea'
    WHEN first_country = 'mauritius' THEN 'emea'
    WHEN first_country = 'mayotte' THEN 'emea'
    WHEN first_country = 'mexico' THEN 'amer'
    WHEN first_country = 'moldova' THEN 'emea'
    WHEN first_country = 'monaco' THEN 'emea'
    WHEN first_country = 'mongolia' THEN 'apac'
    WHEN first_country = 'montenegro' THEN 'emea'
    WHEN first_country = 'montserrat' THEN 'amer'
    WHEN first_country = 'morocco' THEN 'emea'
    WHEN first_country = 'mozambique' THEN 'emea'
    WHEN first_country = 'myanmar' THEN 'apac'
    WHEN first_country = 'namibia' THEN 'emea'
    WHEN first_country = 'nauru' THEN 'apac'
    WHEN first_country = 'nepal' THEN 'apac'
    WHEN first_country = 'netherlands' THEN 'emea'
    WHEN first_country = 'new caledonia' THEN 'apac'
    WHEN first_country = 'new zealand' THEN 'apac'
    WHEN first_country = 'nicaragua' THEN 'amer'
    WHEN first_country = 'niger' THEN 'emea'
    WHEN first_country = 'nigeria' THEN 'emea'
    WHEN first_country = 'north macedonia' THEN 'emea'
    WHEN first_country = 'norway' THEN 'emea'
    WHEN first_country = 'oman' THEN 'emea'
    WHEN first_country = 'pakistan' THEN 'emea'
    WHEN first_country = 'palestine' THEN 'emea'
    WHEN first_country = 'palestine, state of' THEN 'emea'
    WHEN first_country = 'panama' THEN 'amer'
    WHEN first_country = 'papua new guinea' THEN 'apac'
    WHEN first_country = 'paraguay' THEN 'amer'
    WHEN first_country = 'peru' THEN 'amer'
    WHEN first_country = 'philippines' THEN 'apac'
    WHEN first_country = 'poland' THEN 'emea'
    WHEN first_country = 'portugal' THEN 'emea'
    WHEN first_country = 'puerto rico' THEN 'amer'
    WHEN first_country = 'qatar' THEN 'emea'
    WHEN first_country = 'reunion' THEN 'emea'
    WHEN first_country = 'romania' THEN 'emea'
    WHEN first_country = 'russia' THEN 'emea'
    WHEN first_country = 'rwanda' THEN 'emea'
    WHEN first_country = 'saint helena' THEN 'emea'
    WHEN first_country = 'saint kitts and nevis' THEN 'amer'
    WHEN first_country = 'saint lucia' THEN 'amer'
    WHEN first_country = 'saint martin' THEN 'amer'
    WHEN first_country = 'saint vincent and the grenadines' THEN 'amer'
    WHEN first_country = 'samoa' THEN 'apac'
    WHEN first_country = 'san marino' THEN 'emea'
    WHEN first_country = 'sao tome and principe' THEN 'emea'
    WHEN first_country = 'saudi arabia' THEN 'emea'
    WHEN first_country = 'senegal' THEN 'emea'
    WHEN first_country = 'serbia' THEN 'emea'
    WHEN first_country = 'seychelles' THEN 'emea'
    WHEN first_country = 'sierra leone' THEN 'emea'
    WHEN first_country = 'singapore' THEN 'apac'
    WHEN first_country = 'sint maarten' THEN 'amer'
    WHEN first_country = 'slovakia' THEN 'emea'
    WHEN first_country = 'slovenia' THEN 'emea'
    WHEN first_country = 'solomon islands' THEN 'apac'
    WHEN first_country = 'somalia' THEN 'emea'
    WHEN first_country = 'south africa' THEN 'emea'
    WHEN first_country = 'south korea' THEN 'apac'
    WHEN first_country = 'south sudan' THEN 'emea'
    WHEN first_country = 'spain' THEN 'emea'
    WHEN first_country = 'españa' THEN 'emea'
    WHEN first_country = 'sri lanka' THEN 'apac'
    WHEN first_country = 'sudan' THEN 'emea'
    WHEN first_country = 'suriname' THEN 'amer'
    WHEN first_country = 'swaziland' THEN 'emea'
    WHEN first_country = 'sweden' THEN 'emea'
    WHEN first_country = 'switzerland' THEN 'emea'
    WHEN first_country = 'syria' THEN 'emea'
    WHEN first_country = 'taiwan' THEN 'apac'
    WHEN first_country = 'taiwan, province of china' THEN 'apac'
    WHEN first_country = 'tajikistan' THEN 'emea'
    WHEN first_country = 'tanzania' THEN 'emea'
    WHEN first_country = 'united republic of tanzania' THEN 'emea'
    WHEN first_country = 'thailand' THEN 'apac'
    WHEN first_country = 'the netherlands' THEN 'emea'
    WHEN first_country = 'timor-leste' THEN 'apac'
    WHEN first_country = 'togo' THEN 'emea'
    WHEN first_country = 'tonga' THEN 'apac'
    WHEN first_country = 'trinidad and tobago' THEN 'amer'
    WHEN first_country = 'tunisia' THEN 'emea'
    WHEN first_country = 'turkey' THEN 'emea'
    WHEN first_country = 'turkmenistan' THEN 'emea'
    WHEN first_country = 'turks and caicos islands' THEN 'amer'
    WHEN first_country = 'u.s. virgin islands' THEN 'amer'
    WHEN first_country = 'uganda' THEN 'emea'
    WHEN first_country = 'ukraine' THEN 'emea'
    WHEN first_country = 'united arab emirates' THEN 'emea'
    WHEN first_country = 'united kingdom' THEN 'emea'
    WHEN first_country = 'uruguay' THEN 'amer'
    WHEN first_country = 'uzbekistan' THEN 'emea'
    WHEN first_country = 'vanuatu' THEN 'apac'
    WHEN first_country = 'vatican' THEN 'emea'
    WHEN first_country = 'venezuela' THEN 'amer'
    WHEN first_country = 'vietnam' THEN 'apac'
    WHEN first_country = 'western sahara' THEN 'emea'
    WHEN first_country = 'yemen' THEN 'emea'
    WHEN first_country = 'zambia' THEN 'emea'
    WHEN first_country = 'zimbabwe' THEN 'emea'
    WHEN first_country = 'united states' THEN 'amer'
    WHEN first_country = 'turks and caicos' THEN 'amer'
    WHEN first_country = 'korea, republic of' THEN 'apac'
    WHEN first_country = "lao democratic people\'s republic" THEN 'apac'
    WHEN first_country = 'macedonia, the former yugoslav republic of' THEN 'emea'
    WHEN first_country = 'moldova, republic of' THEN 'emea'
    WHEN first_country = 'russian federation' THEN 'emea'
    WHEN first_country = 'viet nam' THEN 'apac' 
  END AS geo
  
    --opportunity data
    cohort_base.opp_created_date,
    cohort_base.sales_accepted_date,
    cohort_base.close_date,
    cohort_base.is_sao,
    cohort_base.is_won,
    cohort_base.new_logo_count,
    cohort_base.net_arr,
    cohort_base.is_net_arr_closed_deal,
    cohort_base.opp_order_type,
    cohort_base.sales_qualified_source_name,
    cohort_base.deal_path_name,
    cohort_base.sales_type,
    cohort_base.crm_opp_owner_geo_stamped,
    cohort_base.crm_opp_owner_sales_segment_stamped,
    cohort_base.crm_opp_owner_region_stamped,
    cohort_base.crm_opp_owner_area_stamped,
    cohort_base.parent_crm_account_demographics_upa_country,
    cohort_base.parent_crm_account_demographics_territory,
    CASE
      WHEN rpt_sfdc_bizible_tp_opp_linear_blended.dim_crm_touchpoint_id IS NOT null THEN cohort_base.dim_crm_opportunity_id
      ELSE null
    END AS influenced_opportunity_id,
  
    --touchpoint data
    rpt_sfdc_bizible_tp_opp_linear_blended.bizible_touchpoint_date_normalized,
    rpt_sfdc_bizible_tp_opp_linear_blended.gtm_motion,
    rpt_sfdc_bizible_tp_opp_linear_blended.bizible_integrated_campaign_grouping,
    rpt_sfdc_bizible_tp_opp_linear_blended.bizible_marketing_channel_path,
    rpt_sfdc_bizible_tp_opp_linear_blended.bizible_marketing_channel,
    rpt_sfdc_bizible_tp_opp_linear_blended.bizible_ad_campaign_name,
    rpt_sfdc_bizible_tp_opp_linear_blended.bizible_form_url,
    rpt_sfdc_bizible_tp_opp_linear_blended.bizible_landing_page,
    rpt_sfdc_bizible_tp_opp_linear_blended.is_dg_influenced,
    rpt_sfdc_bizible_tp_opp_linear_blended.is_fmm_influenced,
    rpt_sfdc_bizible_tp_opp_linear_blended.mql_sum,
    rpt_sfdc_bizible_tp_opp_linear_blended.inquiry_sum,
    rpt_sfdc_bizible_tp_opp_linear_blended.accepted_sum,
    rpt_sfdc_bizible_tp_opp_linear_blended.linear_opp_created,
    rpt_sfdc_bizible_tp_opp_linear_blended.linear_net_arr,
    rpt_sfdc_bizible_tp_opp_linear_blended.linear_sao,
    rpt_sfdc_bizible_tp_opp_linear_blended.pipeline_linear_net_arr,
    rpt_sfdc_bizible_tp_opp_linear_blended.won_linear,
    rpt_sfdc_bizible_tp_opp_linear_blended.won_linear_net_arr,
    rpt_sfdc_bizible_tp_opp_linear_blended.w_shaped_sao,
    rpt_sfdc_bizible_tp_opp_linear_blended.pipeline_w_net_arr,
    rpt_sfdc_bizible_tp_opp_linear_blended.won_w,
    rpt_sfdc_bizible_tp_opp_linear_blended.won_w_net_arr,
    rpt_sfdc_bizible_tp_opp_linear_blended.u_shaped_sao,
    rpt_sfdc_bizible_tp_opp_linear_blended.pipeline_u_net_arr,
    rpt_sfdc_bizible_tp_opp_linear_blended.won_u,
    rpt_sfdc_bizible_tp_opp_linear_blended.won_u_net_arr,
    rpt_sfdc_bizible_tp_opp_linear_blended.first_sao,
    rpt_sfdc_bizible_tp_opp_linear_blended.pipeline_first_net_arr,
    rpt_sfdc_bizible_tp_opp_linear_blended.won_first,
    rpt_sfdc_bizible_tp_opp_linear_blended.won_first_net_arr,
    rpt_sfdc_bizible_tp_opp_linear_blended.custom_sao,
    rpt_sfdc_bizible_tp_opp_linear_blended.pipeline_custom_net_arr,
    rpt_sfdc_bizible_tp_opp_linear_blended.won_custom,
    rpt_sfdc_bizible_tp_opp_linear_blended.won_custom_net_arr
  FROM cohort_base
  LEFT JOIN rpt_sfdc_bizible_tp_opp_linear_blended
    ON rpt_sfdc_bizible_tp_opp_linear_blended.email_hash=cohort_base.email_hash

), final_prep AS (

    SELECT DISTINCT fo_inquiry_with_tp.*,
    COALESCE(employee_count_segment,employee_bucket_segment) AS inferred_employee_segment,
    UPPER(geo) AS inferred_geo
    FROM fo_inquiry_with_tp

), final AS (

  SELECT *
  FROM final_prep

)


{{ dbt_audit(
    cte_ref="final",
    created_by="@rkohnke",
    updated_by="@rkohnke",
    created_date="2022-07-20",
    updated_date="2022-09-28",
  ) }}
