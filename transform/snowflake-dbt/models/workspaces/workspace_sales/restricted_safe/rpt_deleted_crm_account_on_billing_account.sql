{{ simple_cte([
    ('map_merged_crm_account','map_merged_crm_account')
]) }}

, zuora_account AS (

    SELECT *
    FROM {{ref('zuora_account_source')}}
    --Exclude Batch20 which are the test accounts. This method replaces the manual dbt seed exclusion file.
    WHERE LOWER(batch) != 'batch20'
      AND is_deleted = FALSE

), final AS (

    SELECT
      zuora_account.account_id                              AS dim_billing_account_id,
      map_merged_crm_account.dim_crm_account_id             AS dim_crm_account_id_merged,
      zuora_account.crm_id                                  AS dim_crm_account_id_zuora,
      zuora_account.account_number                          AS billing_account_number
    FROM zuora_account
    LEFT JOIN map_merged_crm_account
      ON zuora_account.crm_id = map_merged_crm_account.sfdc_account_id
    WHERE dim_crm_account_id_merged != dim_crm_account_id_zuora

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@michellecooper",
    updated_by="@michellecooper",
    created_date="2022-10-07",
    updated_date="2022-10-07"
) }}