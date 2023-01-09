{{ simple_cte([
    ('mart_crm_person','mart_crm_person'),
	('sfdc_opportunity_xf','sfdc_opportunity_xf'),
	('map_alternative_lead_demographics','map_alternative_lead_demographics')
]) }},

WITH person AS (
 
  SELECT
    mart_crm_person.dim_crm_person_id,
    mart_crm_person.email_hash,
    mart_crm_person.dim_crm_user_id,
    mart_crm_person.dim_crm_account_id,
    mart_crm_person.lead_source,
    mart_crm_person.source_buckets,
    mart_crm_person.is_first_order_person,
    mart_crm_person.is_first_order_mql,
    mart_crm_person.created_date,
    mart_crm_person.legacy_mql_date_first_pt,
    mart_crm_person.mql_date_first_pt,
	mart_crm_person.true_inquiry_date,
    map_alternative_lead_demographics.employee_count_segment_custom,
    map_alternative_lead_demographics.geo_custom
  FROM mart_crm_person
  LEFT JOIN map_alternative_lead_demographics
    ON mart_crm_person.dim_crm_person_id=map_alternative_lead_demographics.dim_crm_person_id

), opportunity AS (
  
  SELECT
    opportunity_id,
    account_id,
    owner_id,
    close_date,
    sales_accepted_date,
    created_date,
    lead_source,
    source_buckets,
    stage_name,
    order_type_stamped,
    order_type_live,
    net_arr,
    user_segment,
    user_segment_stamped,
    user_geo_stamped,
    sales_segment,
    is_closed,
    is_lost,
    is_renewal
  FROM sfdc_opportunity_xf

), combined_person_opp AS (

  SELECT DISTINCT
    --Key IDs
    person.dim_crm_person_id,
    opportunity.opportunity_id,

    --Person Data
    person.email_hash,
    person.dim_crm_user_id AS person_owner_id,
    person.dim_crm_account_id,
    person.lead_source AS person_lead_source,
    person.employee_count_segment_custom,
    person.geo_custom,
    person.is_first_order_person,
    person.is_first_order_mql,
    person.source_buckets AS person_source_buckets,

    --Person Dates
    person.created_date AS person_created_date,
    person.legacy_mql_date_first_pt,
    person.mql_date_first_pt,
	person.true_inquiry_date,

    --Opportunity Data
    opportunity.owner_id AS opp_owner_id,
    opportunity.lead_source,
    opportunity.source_buckets,
    opportunity.stage_name,
    opportunity.order_type_stamped,
    opportunity.order_type_live,
    opportunity.net_arr,
    opportunity.user_segment,
    opportunity.user_segment_stamped,
    opportunity.user_geo_stamped,
    opportunity.sales_segment,
    opportunity.is_closed,
    opportunity.is_lost,
    opportunity.is_renewal,

    --Opportunity Dates
    opportunity.created_date AS opp_created_date,
    opportunity.sales_accepted_date,
    opportunity.close_date,

	--KPI IDs
	CASE
		WHEN true_inquiry_date IS NOT NULL THEN dim_crm_person_id
		ELSE NULL
	END AS inquiry_id,
	CASE
		WHEN legacy_mql_date_first_pt IS NOT NULL THEN dim_crm_person_id
		ELSE NULL
	END AS marketo_mql_id,
	CASE
		WHEN mql_date_first_pt IS NOT NULL THEN dim_crm_person_id
		ELSE NULL
	END AS true_mql_id,
	CASE
		WHEN opp_created_date IS NOT NULL THEN opportunity_id
		ELSE NULL
	END AS opp_created_id,
	CASE
		WHEN sales_accepted_date IS NOT NULL THEN opportunity_id
		ELSE NULL
	END AS sao_id,
	CASE
		WHEN close_date IS NOT NULL THEN opportunity_id
		ELSE NULL
	END AS opp_closed_id
  FROM person
  FULL JOIN opportunity
    ON person.dim_crm_account_id=opportunity.account_id

), inquiry_final AS (

	SELECT DISTINCT
		email_hash,
		person_owner_id,
		dim_crm_account_id,
		employee_count_segment_custom,
    	geo_custom,
		lead_source,
		source_buckets,
		employee_count_segment_custom,
    	geo_custom,
		dim_date.fiscal_year                     AS date_range_year,
  		dim_date.fiscal_quarter_name_fy          AS date_range_quarter,
  		DATE_TRUNC(month, dim_date.date_actual)  AS date_range_month,
  		dim_date.first_day_of_week               AS date_range_week,
  		dim_date.date_id                         AS date_range_id,
		dim_date.date_actual,
		COUNT(DISTINCT inquiry_id) AS inquiries
	FROM combined_person_opp
	LEFT JOIN dim_date
		ON combined_person_opp.true_inquiry_date=dim_date.date_actual
	{{ dbt_utils.group_by(n=15) }}

), marketo_mql_final AS (

	SELECT DISTINCT
		email_hash,
		person_owner_id,
		dim_crm_account_id,
		employee_count_segment_custom,
    	geo_custom,
		lead_source,
		source_buckets,
		employee_count_segment_custom,
    	geo_custom,
		dim_date.fiscal_year                     AS date_range_year,
  		dim_date.fiscal_quarter_name_fy          AS date_range_quarter,
  		DATE_TRUNC(month, dim_date.date_actual)  AS date_range_month,
  		dim_date.first_day_of_week               AS date_range_week,
  		dim_date.date_id                         AS date_range_id,
		dim_date.date_actual,
		COUNT(DISTINCT marketo_mql_id) AS marketo_mqls
	FROM combined_person_opp
	LEFT JOIN dim_date
		ON combined_person_opp.legacy_mql_date_first_pt=dim_date.date_actual
	{{ dbt_utils.group_by(n=15) }}

), true_mql_final AS (

	SELECT DISTINCT
		email_hash,
		person_owner_id,
		dim_crm_account_id,
		employee_count_segment_custom,
    	geo_custom,
		lead_source,
		source_buckets,
		employee_count_segment_custom,
    	geo_custom,
		dim_date.fiscal_year                     AS date_range_year,
  		dim_date.fiscal_quarter_name_fy          AS date_range_quarter,
  		DATE_TRUNC(month, dim_date.date_actual)  AS date_range_month,
  		dim_date.first_day_of_week               AS date_range_week,
  		dim_date.date_id                         AS date_range_id,
		dim_date.date_actual,
		COUNT(DISTINCT true_mql_id) AS true_mqls
	FROM combined_person_opp
	LEFT JOIN dim_date
		ON combined_person_opp.mql_date_first_pt=dim_date.date_actual
	{{ dbt_utils.group_by(n=15) }}

), opp_created_final AS (

	SELECT DISTINCT
		opp_owner_id,
    	lead_source,
    	source_buckets,
    	stage_name,
    	order_type_stamped,
    	order_type_live,
    	user_segment,
    	user_segment_stamped,
    	user_geo_stamped,
    	sales_segment,
    	is_closed,
    	is_lost,
    	is_renewal,
		dim_date.fiscal_year                     AS date_range_year,
  		dim_date.fiscal_quarter_name_fy          AS date_range_quarter,
  		DATE_TRUNC(month, dim_date.date_actual)  AS date_range_month,
  		dim_date.first_day_of_week               AS date_range_week,
  		dim_date.date_id                         AS date_range_id,
		dim_date.date_actual,
		COUNT(DISTINCT opp_created_id) AS opps_created,
		SUM(net_arr) AS net_arr
	FROM combined_person_opp
	LEFT JOIN dim_date
		ON combined_person_opp.opp_created_date=dim_date.date_actual
	{{ dbt_utils.group_by(n=19) }}

), opp_sao_final AS (

	SELECT DISTINCT
		opp_owner_id,
    	lead_source,
    	source_buckets,
    	stage_name,
    	order_type_stamped,
    	order_type_live,
    	user_segment,
    	user_segment_stamped,
    	user_geo_stamped,
    	sales_segment,
    	is_closed,
    	is_lost,
    	is_renewal,
		dim_date.fiscal_year                     AS date_range_year,
  		dim_date.fiscal_quarter_name_fy          AS date_range_quarter,
  		DATE_TRUNC(month, dim_date.date_actual)  AS date_range_month,
  		dim_date.first_day_of_week               AS date_range_week,
  		dim_date.date_id                         AS date_range_id,
		dim_date.date_actual,
		COUNT(DISTINCT sao_id) AS saos,
		SUM(net_arr) AS net_arr
	FROM combined_person_opp
	LEFT JOIN dim_date
		ON combined_person_opp.sales_accepted_date=dim_date.date_actual
	{{ dbt_utils.group_by(n=19) }}

), opp_closed_final AS (

	SELECT DISTINCT
		opp_owner_id,
    	lead_source,
    	source_buckets,
    	stage_name,
    	order_type_stamped,
    	order_type_live,
    	user_segment,
    	user_segment_stamped,
    	user_geo_stamped,
    	sales_segment,
    	is_closed,
    	is_lost,
    	is_renewal,
		dim_date.fiscal_year                     AS date_range_year,
  		dim_date.fiscal_quarter_name_fy          AS date_range_quarter,
  		DATE_TRUNC(month, dim_date.date_actual)  AS date_range_month,
  		dim_date.first_day_of_week               AS date_range_week,
  		dim_date.date_id                         AS date_range_id,
		dim_date.date_actual,
		COUNT(DISTINCT opp_closed_id) AS opps_closed,
		SUM(net_arr) AS net_arr
	FROM combined_person_opp
	LEFT JOIN dim_date
		ON combined_person_opp.close_date=dim_date.date_actual
	{{ dbt_utils.group_by(n=19) }}

), inquiry_to_marketo_mql AS (

	SELECT DISTINCT
		person_owner_id,
		dim_crm_account_id,
		employee_count_segment_custom,
    	geo_custom,
		lead_source,
		source_buckets,
		employee_count_segment_custom,
    	geo_custom,
		dim_date.fiscal_year                     AS date_range_year,
  		dim_date.fiscal_quarter_name_fy          AS date_range_quarter,
  		DATE_TRUNC(month, dim_date.date_actual)  AS date_range_month,
  		dim_date.first_day_of_week               AS date_range_week,
  		dim_date.date_id                         AS date_range_id,
		dim_date.date_actual,
		COUNT(DISTINCT marketo_mql_id)/COUNT(DISTINCT inquiry_id) AS inquiry_to_mql
	FROM combined_person_opp
	LEFT JOIN dim_date
		ON combined_person_opp.legacy_mql_date_first_pt=dim_date.date_actual
	{{ dbt_utils.group_by(n=14) }}

), mql_to_sao AS (

), sao_to_closed_won AS (

), sao_to_closed_lost AS (

), inquiry_to_sao AS (

), inquiry_to_closed_won AS (

), inquiry_to_closed_lost AS (

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@rkohnke",
    updated_by="@rkohnke",
    created_date="2022-12-28",
    updated_date="2022-12-28"
) }}