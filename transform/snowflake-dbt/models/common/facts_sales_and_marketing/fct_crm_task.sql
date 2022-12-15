{{ simple_cte([
    ('prep_crm_person', 'prep_crm_person')
]) }}

, prep_crm_task AS (

  SELECT *
  FROM {{ ref('prep_crm_task') }}
  WHERE is_deleted = FALSE

), final AS (

  SELECT
    -- Primary key
    prep_crm_task.dim_crm_task_pk,

    -- Foreign keys
    {{ get_keyed_nulls('prep_crm_task.dim_crm_task_sk') }}        AS dim_crm_task_sk,
    {{ get_keyed_nulls('prep_crm_task.dim_crm_account_id') }}     AS dim_crm_account_id,
    {{ get_keyed_nulls('prep_crm_task.dim_crm_user_id') }}        AS dim_crm_user_id,
    {{ get_keyed_nulls('prep_crm_task.sfdc_record_type_id') }}    AS sfdc_record_type_id,
    {{ get_keyed_nulls('prep_crm_person.dim_crm_person_id') }}    AS dim_crm_person_id,
    {{ get_keyed_nulls('prep_crm_task.dim_crm_opportunity_id') }} AS dim_crm_opportunity_id,
    prep_crm_task.sfdc_record_id,

    -- Dates
    {{ get_date_id('prep_crm_task.task_date') }}                  AS task_date_id,
    prep_crm_task.task_date,
    {{ get_date_id('prep_crm_task.task_completed_date') }}        AS task_completed_date_id,
    prep_crm_task.task_completed_date,
    {{ get_date_id('prep_crm_task.reminder_date') }}              AS reminder_date_id,
    prep_crm_task.reminder_date,
    {{ get_date_id('prep_crm_task.task_recurrence_date') }}       AS task_recurrence_date_id,
    prep_crm_task.task_recurrence_date,
    {{ get_date_id('prep_crm_task.task_recurrence_start_date') }} AS task_recurrence_start_date_id,
    prep_crm_task.task_recurrence_start_date,

    -- Counts
    prep_crm_task.account_or_opportunity_count,
    prep_crm_task.lead_or_contact_count,

    -- Metadata
    prep_crm_task.task_created_by_id,
    {{ get_date_id('prep_crm_task.task_created_date') }}          AS task_created_date_id,
    prep_crm_task.task_created_date,
    prep_crm_task.last_modified_id,
    {{ get_date_id('prep_crm_task.last_modified_date') }}         AS last_modified_date_id,
    prep_crm_task.last_modified_date
  FROM prep_crm_task
  LEFT JOIN prep_crm_person
    ON prep_crm_task.sfdc_record_id = prep_crm_person.sfdc_record_id

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@michellecooper",
    updated_by="@michellecooper",
    created_date="2022-12-05",
    updated_date="2022-12-05"
) }}
