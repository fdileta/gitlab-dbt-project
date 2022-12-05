{{ simple_cte([
    ('sfdc_task_source', 'sfdc_task_source'),
    ('prep_crm_person', 'prep_crm_person')
]) }}

, task AS (

  SELECT *
  FROM sfdc_task_source
  WHERE is_deleted = FALSE

), final AS (

  SELECT
    -- Primary key
    task.task_id                                                AS dim_crm_task_pk,

    -- Surrogate key
    {{ dbt_utils.surrogate_key('task.task_id') }}               AS dim_crm_task_sk,

    -- Foreign keys
    {{ get_keyed_nulls('task.account_id') }}                    AS dim_crm_account_id,
    {{ get_keyed_nulls('task.owner_id') }}                      AS dim_crm_user_id,
    {{ get_keyed_nulls('task.record_type_id') }}                AS sfdc_record_type_id,
    {{ get_keyed_nulls('prep_crm_person.dim_crm_person_id') }}  AS dim_crm_person_id,
    {{ get_keyed_nulls('task.related_opportunity_id') }}        AS dim_crm_opportunity_id,
    COALESCE(task.related_lead_id, task.related_contact_id)     AS sfdc_record_id,

    -- Dates
    {{ get_date_id('task.task_date') }}                         AS task_date_id,
    task.task_date,
    {{ get_date_id('task.task_completed_date') }}               AS task_completed_date_id,
    task.task_completed_date,
    {{ get_date_id('task.reminder_date') }}                     AS reminder_date_id,
    task.reminder_date,
    {{ get_date_id('task.task_recurrence_date') }}              AS task_recurrence_date_id,
    task.task_recurrence_date,
    {{ get_date_id('task.task_recurrence_start_date') }}        AS task_recurrence_start_date_id,
    task.task_recurrence_start_date,

    -- Counts
    task.account_or_opportunity_count,
    task.lead_or_contact_count,

    -- Metadata
    task.task_created_by_id,
    {{ get_date_id('task.task_created_date') }}                 AS task_created_date_id,
    task.task_created_date,
    task.last_modified_id,
    {{ get_date_id('task.last_modified_date') }}                AS last_modified_date_id,
    task.last_modified_date
  FROM task
  LEFT JOIN prep_crm_person
    ON COALESCE(task.related_lead_id, task.related_contact_id) = prep_crm_person.sfdc_record_id

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@michellecooper",
    updated_by="@michellecooper",
    created_date="2022-12-05",
    updated_date="2022-12-05"
) }}