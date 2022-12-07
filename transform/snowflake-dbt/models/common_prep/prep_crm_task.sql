WITH source AS (

    SELECT *
    FROM {{ ref('sfdc_task_source') }}

), renamed AS(

    SELECT
      task_id,

      --keys
      {{ dbt_utils.surrogate_key(['source.task_id']) }}             AS dim_crm_task_sk,
      source.task_id                                                AS dim_crm_task_pk,
      source.account_id                                             AS dim_crm_account_id,
      source.owner_id                                               AS dim_crm_user_id,
      source.assigned_employee_number,
      source.lead_or_contact_id,
      source.account_or_opportunity_id,
      source.record_type_id                                         AS sfdc_record_type_id,
      source.related_to_account_name,
      source.pf_order_id,
      source.related_lead_id,
      source.related_contact_id,
      source.related_opportunity_id                                 AS dim_crm_opportunity_id,
      source.related_account_id,
      source.related_to_id,
      COALESCE(source.related_lead_id, source.related_contact_id)   AS sfdc_record_id,

      -- Task infomation
      source.comments,
      source.full_comments,
      source.task_subject,
      source.task_date,
      source.task_created_date,
      source.task_created_by_id,
      source.task_status,
      source.task_subtype,
      source.task_type,
      source.task_priority,
      source.close_task,
      source.task_completed_date,
      source.is_closed,
      source.is_deleted,
      source.is_archived,
      source.is_high_priority,
      source.persona_functions,
      source.persona_levels,
      source.outreach_meeting_type,
      source.customer_interaction_sentiment,
      source.task_owner_role,

      -- Activity infromation
      source.activity_disposition,
      source.activity_source,
      source.activity,
      source.csm_activity_type,
      source.sa_activity_type,
      source.gs_activity_type,
      source.gs_sentiment,
      source.gs_meeting_type,
      source.is_gs_exec_sponsor_present,
      source.is_meeting_cancelled,

      -- Call information
      source.call_type,
      source.call_purpose,
      source.call_disposition,
      source.call_duration_in_seconds,
      source.call_recording,
      source.is_answered,
      source.is_bad_number,
      source.is_busy,
      source.is_correct_contact,
      source.is_not_answered,
      source.is_left_message,

      -- Reminder information
      source.is_reminder_set,
      source.reminder_date,

      -- Recurrence information
      source.is_recurrence,
      source.task_recurrence_interval,
      source.task_recurrence_instance,
      source.task_recurrence_type,
      source.task_recurrence_activity_id,
      source.task_recurrence_date,
      source.task_recurrence_day_of_week,
      source.task_recurrence_timezone,
      source.task_recurrence_start_date,
      source.task_recurrence_day_of_month,
      source.task_recurrence_month,

      -- Sequence information
      source.active_sequence_name,
      source.sequence_step_number,

      -- Docs/Video Conferencing
      source.google_doc_link,
      source.zoom_app_ics_sequence,
      source.zoom_app_use_personal_zoom_meeting_id,
      source.zoom_app_join_before_host,
      source.zoom_app_make_it_zoom_meeting,
      source.chorus_call_id,

      -- Counts
      source.account_or_opportunity_count,
      source.lead_or_contact_count,

      -- metadata
      source.last_modified_id,
      source.last_modified_date,
      source.systemmodstamp

    FROM source
)

SELECT *
FROM renamed
