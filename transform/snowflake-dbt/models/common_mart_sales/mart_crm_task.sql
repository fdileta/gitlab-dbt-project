{{ simple_cte([
    ('dim_crm_task', 'dim_crm_task'),
    ('fct_crm_task', 'fct_crm_task')
]) }}

, final AS (


  SELECT
    -- Primary Key
    fct_crm_task.dim_crm_task_pk,

    -- Surrogate key
    dim_crm_task.dim_crm_task_sk,

    -- Natural key
    dim_crm_task.task_id,

    -- Foreign keys
    fct_crm_task.dim_crm_account_id,
    fct_crm_task.dim_crm_user_id,
    fct_crm_task.sfdc_record_type_id,
    fct_crm_task.dim_crm_person_id,
    fct_crm_task.sfdc_record_id,
    fct_crm_task.dim_crm_opportunity_id,

    -- Task infomation
    fct_crm_task.task_date_id,
    fct_crm_task.task_date,
    fct_crm_task.task_completed_date_id,
    fct_crm_task.task_completed_date,
    dim_crm_task.full_comments,
    dim_crm_task.task_subject,
    dim_crm_task.task_status,
    dim_crm_task.task_subtype,
    dim_crm_task.task_type,
    dim_crm_task.task_priority,
    dim_crm_task.close_task,
    dim_crm_task.is_closed,
    dim_crm_task.is_deleted,
    dim_crm_task.is_archived,
    dim_crm_task.is_high_priority,
    dim_crm_task.persona_functions,
    dim_crm_task.persona_levels,
    dim_crm_task.outreach_meeting_type,
    dim_crm_task.customer_interaction_sentiment,
    dim_crm_task.task_owner_role,

    -- Activity infromation
    dim_crm_task.activity_disposition,
    dim_crm_task.activity_source,
    dim_crm_task.activity,
    dim_crm_task.csm_activity_type,
    dim_crm_task.sa_activity_type,
    dim_crm_task.gs_activity_type,
    dim_crm_task.gs_sentiment,
    dim_crm_task.gs_meeting_type,
    dim_crm_task.is_gs_exec_sponsor_present,
    dim_crm_task.is_meeting_cancelled,

    -- Call information
    dim_crm_task.call_type,
    dim_crm_task.call_purpose,
    dim_crm_task.call_disposition,
    dim_crm_task.call_duration_in_seconds,
    dim_crm_task.call_recording,
    dim_crm_task.is_answered,
    dim_crm_task.is_bad_number,
    dim_crm_task.is_busy,
    dim_crm_task.is_correct_contact,
    dim_crm_task.is_not_answered,
    dim_crm_task.is_left_message,

    -- Reminder information
    dim_crm_task.is_reminder_set,
    fct_crm_task.reminder_date_id,
    fct_crm_task.reminder_date,

    -- Recurrence information
    dim_crm_task.is_recurrence,
    fct_crm_task.task_recurrence_date_id,
    fct_crm_task.task_recurrence_date,
    fct_crm_task.task_recurrence_start_date_id,
    fct_crm_task.task_recurrence_start_date,
    dim_crm_task.task_recurrence_interval,
    dim_crm_task.task_recurrence_instance,
    dim_crm_task.task_recurrence_type,
    dim_crm_task.task_recurrence_activity_id,
    dim_crm_task.task_recurrence_day_of_week,
    dim_crm_task.task_recurrence_timezone,
    dim_crm_task.task_recurrence_day_of_month,
    dim_crm_task.task_recurrence_month,

    -- Sequence information
    dim_crm_task.active_sequence_name,
    dim_crm_task.sequence_step_number,

    -- Docs/Video Conferencing
    dim_crm_task.google_doc_link,
    dim_crm_task.zoom_app_ics_sequence,
    dim_crm_task.zoom_app_use_personal_zoom_meeting_id,
    dim_crm_task.zoom_app_join_before_host,
    dim_crm_task.zoom_app_make_it_zoom_meeting,
    dim_crm_task.chorus_call_id,

    -- Counts
    fct_crm_task.account_or_opportunity_count,
    fct_crm_task.lead_or_contact_count,

    -- Metadata
    fct_crm_task.task_created_by_id,
    fct_crm_task.task_created_date_id,
    fct_crm_task.task_created_date,
    fct_crm_task.last_modified_id,
    fct_crm_task.last_modified_date_id,
    fct_crm_task.last_modified_date

    FROM fct_crm_task 
    LEFT JOIN dim_crm_task 
      ON fct_crm_task.dim_crm_task_sk = dim_crm_task.dim_crm_task_sk


)

{{ dbt_audit(
    cte_ref="final",
    created_by="@michellecooper",
    updated_by="@michellecooper",
    created_date="2022-12-05",
    updated_date="2022-12-05"
) }}
