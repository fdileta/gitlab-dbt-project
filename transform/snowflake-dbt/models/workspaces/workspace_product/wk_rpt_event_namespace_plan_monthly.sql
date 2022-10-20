{{ config(
    materialized='table',
    tags=["mnpi_exception", "product"]
) }}

{{ simple_cte([
    ('mart_event_valid', 'mart_event_valid')
    ])
}},

/*
Look for a namespace's last event within the reporting period (i.e. the last 28 days of the month).
The plan on the final event will be used to attribute the namespace's usage during the month.
This is the same logic used in rpt_event_xmau_metric_monthly and rpt_event_plan_monthly.
*/

plan_id_by_month AS (

  SELECT
    dim_ultimate_parent_namespace_id,
    event_calendar_month,
    plan_id_at_event_date,
    plan_name_at_event_date,
    plan_was_paid_at_event_date,
    namespace_is_internal,
    ultimate_parent_namespace_type,
    namespace_creator_is_blocked,
    event_created_at,
    event_pk
  FROM mart_event_valid
  WHERE event_date BETWEEN DATEADD('day', -27, LAST_DAY(event_date)) AND LAST_DAY(event_date) --last 28 days of the month
    AND event_date < DATE_TRUNC('month', CURRENT_DATE) --exclude current month
  QUALIFY ROW_NUMBER() OVER (PARTITION BY dim_ultimate_parent_namespace_id, event_calendar_month
      ORDER BY event_created_at DESC) = 1

),

final AS (

  SELECT
    event_calendar_month                                  AS event_calendar_month,
    dim_ultimate_parent_namespace_id                      AS dim_ultimate_parent_namespace_id,
    plan_id_at_event_date                                 AS plan_id_at_event_month,
    plan_name_at_event_date                               AS plan_name_at_event_month,
    plan_was_paid_at_event_date                           AS plan_was_paid_at_event_month,
    namespace_is_internal                                 AS namespace_is_internal,
    ultimate_parent_namespace_type                        AS ultimate_parent_namespace_type,
    namespace_creator_is_blocked                          AS namespace_creator_is_blocked,
    DATEADD('day', -27, LAST_DAY(event_calendar_month))   AS first_day_of_reporting_period,
    LAST_DAY(event_calendar_month)                        AS last_day_of_reporting_period
  FROM plan_id_by_month

)

SELECT *
FROM final
