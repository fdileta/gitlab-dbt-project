{{ simple_cte([
    ('sfdc_opportunity_source','sfdc_opportunity_source'),
    ('prep_crm_person','prep_crm_person')
]) }}

 , final AS (

    SELECT DISTINCT
      prep_crm_person.dim_crm_person_id,
      sfdc_opportunity_source.opportunity_id AS dim_crm_opportunity_id

    FROM prep_crm_person
    LEFT JOIN sfdc_opportunity_source 
      ON prep_crm_person.dim_crm_account_id = sfdc_opportunity_source.account_id
    WHERE sfdc_opportunity_source.opportunity_id IS NOT NULL
)

{{ dbt_audit(
    cte_ref="final",
    created_by="@michellecooper",
    updated_by="@michellecooper",
    created_date="2022-09-30",
    updated_date="2022-09-30"
) }}
