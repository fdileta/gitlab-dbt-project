WITH source AS (

  SELECT *
  FROM {{ ref('sfdc_task_source') }}
  WHERE is_deleted = FALSE

), final AS (


  SELECT

    -- Surrogate key
    {{ dbt_utils.surrogate_key(['task_id']) }}  AS dim_crm_task_sk,

    -- Natural key
    task_id,

    -- Task infomation
    full_comments,
    task_subject,
    task_status,
    task_subtype,
    task_type,
    task_priority,
    close_task,
    is_closed,
    is_deleted,
    is_archived,
    is_high_priority,
    persona_functions,
    persona_levels,
    outreach_meeting_type,
    customer_interaction_sentiment,
    task_owner_role,

    -- Activity infromation
    activity_disposition,
    activity_source,
    activity,
    csm_activity_type,
    sa_activity_type,
    gs_activity_type,
    gs_sentiment,
    gs_meeting_type,
    is_gs_exec_sponsor_present,
    is_meeting_cancelled,

    -- Call information
    call_type,
    call_purpose,
    call_disposition,
    call_duration_in_seconds,
    call_recording,
    is_answered,
    is_bad_number,
    is_busy,
    is_correct_contact,
    is_not_answered,
    is_left_message,

    -- Reminder information
    is_reminder_set,

    -- Recurrence information
    is_recurrence,
    task_recurrence_interval,
    task_recurrence_instance,
    task_recurrence_type,
    task_recurrence_activity_id,
    task_recurrence_day_of_week,
    task_recurrence_timezone,
    task_recurrence_day_of_month,
    task_recurrence_month,

    -- Sequence information
    active_sequence_name,
    sequence_step_number,

    -- Docs/Video Conferencing
    google_doc_link,
    zoom_app_ics_sequence,
    zoom_app_use_personal_zoom_meeting_id,
    zoom_app_join_before_host,
    zoom_app_make_it_zoom_meeting,
    chorus_call_id

    FROM source


)

{{ dbt_audit(
    cte_ref="final",
    created_by="@michellecooper",
    updated_by="@michellecooper",
    created_date="2022-12-05",
    updated_date="2022-12-05"
) }}
