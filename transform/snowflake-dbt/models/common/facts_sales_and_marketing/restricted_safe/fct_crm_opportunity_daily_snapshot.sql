WITH final AS (

  SELECT {{ dbt_utils.star(from=ref('prep_crm_opportunity'), except=["CREATED_BY", "UPDATED_BY", "MODEL_CREATED_DATE", "MODEL_UPDATED_DATE", "DBT_UPDATED_AT", "DBT_CREATED_AT"]) }}
  FROM {{ ref('prep_crm_opportunity') }}
  WHERE is_live = 0

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@michellecooper",
    updated_by="@michellecooper",
    created_date="2022-02-23",
    updated_date="2022-11-07"
) }}