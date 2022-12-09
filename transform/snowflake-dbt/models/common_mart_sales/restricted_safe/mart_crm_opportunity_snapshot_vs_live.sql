{{ simple_cte([
    ('mart_crm_opportunity_daily_snapshot', 'mart_crm_opportunity_daily_snapshot'),
    ('mart_crm_opportunity', 'mart_crm_opportunity')
    ])

}}

, final AS (

  SELECT 
    mart_crm_opportunity_daily_snapshot.crm_opportunity_snapshot_id,
    mart_crm_opportunity_daily_snapshot.dim_crm_opportunity_id,
    mart_crm_opportunity_daily_snapshot.snapshot_date,
    mart_crm_opportunity_daily_snapshot.snapshot_date,
    mart_crm_opportunity_daily_snapshot.snapshot_month,
    mart_crm_opportunity_daily_snapshot.snapshot_fiscal_year,
    mart_crm_opportunity_daily_snapshot.snapshot_fiscal_quarter_name,
    mart_crm_opportunity_daily_snapshot.snapshot_fiscal_quarter_date,
    mart_crm_opportunity_daily_snapshot.snapshot_day_of_fiscal_quarter_normalised,
    mart_crm_opportunity_daily_snapshot.snapshot_day_of_fiscal_year_normalised,
    mart_crm_opportunity_daily_snapshot.created_in_snapshot_quarter_net_arr,
    mart_crm_opportunity_daily_snapshot.created_in_snapshot_quarter_deal_count,
    {{ dbt_utils.star(from=ref('mart_crm_opportunity_daily_snapshot'), except=["UPDATED_DATE", "UPDATED_BY", "MODEL_CREATED_DATE", "MODEL_CREATED_BY", "DIM_CRM_OPPORUNITY_ID", "CRM_OPPORTUNITY_SNAPSHOT_ID", "SNAPSHOT_DATE", "SNAPSHOT_MONTH","SNAPSHOT_FISCAL_YEAR","SNAPSHOT_FISCAL_QUARTER_NAME","SNAPSHOT_FISCAL_QUARTER_DATE","SNAPSHOT_DAY_OF_FISCAL_QUARTER_NORMALISED","SNAPSHOT_DAY_OF_FISCAL_YEAR_NORMALISED", "CREATED_IN_SNAPSHOT_QUARTER_NET_ARR","CREATED_IN_SNAPSHOT_QUARTER_DEAL_COUNT"],relation_alias="mart_crm_opportunity_daily_snapshot", suffix="_snapshot")}},
    {{ dbt_utils.star(from=ref('mart_crm_opportunity'), except=["UPDATED_DATE", "UPDATED_BY", "MODEL_CREATED_DATE", "MODEL_CREATED_BY", "DIM_CRM_OPPORTUNITY_ID"],relation_alias="mart_crm_opportunity", suffix="_live")}}

  FROM mart_crm_opportunity_daily_snapshot
  LEFT JOIN mart_crm_opportunity
    ON mart_crm_opportunity_daily_snapshot.dim_crm_opportunity_id = mart_crm_opportunity.dim_crm_opportunity_id

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@michellecooper",
    updated_by="@michellecooper",
    created_date="2022-08-08",
    updated_date="2022-11-14"
) }}