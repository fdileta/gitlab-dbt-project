{{ config(
    materialized='table',
    tags=["mnpi_exception", "product"]
) }}

{{ simple_cte([
    ('dim_date','dim_date'),
    ('mart_event_valid', 'mart_event_valid')
    ])
}},

mart_with_date_range AS (

  SELECT
    mart_event_valid.dim_ultimate_parent_namespace_id,
    mart_event_valid.event_calendar_month,
    mart_event_valid.event_calendar_quarter,
    mart_event_valid.event_calendar_year,
    mart_event_valid.event_name,
    mart_event_valid.stage_name,
    mart_event_valid.section_name,
    mart_event_valid.group_name,
    mart_event_valid.is_smau,
    mart_event_valid.is_gmau,
    mart_event_valid.is_umau,
    mart_event_valid.dim_user_id,
    dim_date.last_day_of_month AS last_day_of_month,
    dim_date.last_day_of_quarter AS last_day_of_quarter,
    dim_date.last_day_of_fiscal_year AS last_day_of_fiscal_year
  FROM mart_event_valid
  LEFT JOIN dim_date
    ON mart_event_valid.event_date = dim_date.date_actual
  WHERE mart_event_valid.event_date BETWEEN DATEADD('day', -27, last_day_of_month) AND last_day_of_month

),

distinct_plan AS (

  SELECT
     DISTINCT
     dim_ultimate_parent_namespace_id,
     event_created_at,
     plan_id_at_event_date
  FROM
    mart_event_valid

),

max_created_at AS (

  SELECT 
    dim_ultimate_parent_namespace_id,
    event_calendar_month,
    MAX(event_created_at) max_event_created_at
  FROM mart_event_valid
  GROUP BY
    dim_ultimate_parent_namespace_id,
    event_calendar_month

),

month_plan AS (

  SELECT 
    max_created_at.dim_ultimate_parent_namespace_id,
    max_created_at.event_calendar_month,
    distinct_plan.plan_id_at_event_date
  FROM max_created_at 
  LEFT JOIN distinct_plan
    ON max_created_at.dim_ultimate_parent_namespace_id IS NOT DISTINCT FROM distinct_plan.dim_ultimate_parent_namespace_id       -- Treats Nulls as values to Join
    AND max_created_at.max_event_created_at = distinct_plan.event_created_at

),

mart_usage_event_plan_monthly AS (

  SELECT
    {{ dbt_utils.surrogate_key(['mart_with_date_range.event_calendar_month', 'month_plan.plan_id_at_event_date', 'mart_with_date_range.event_name']) }} AS event_plan_monthly_id,
    mart_with_date_range.event_calendar_month,
    mart_with_date_range.event_calendar_quarter,
    mart_with_date_range.event_calendar_year,
    month_plan.plan_id_at_event_date latest_plan_id,
    mart_with_date_range.event_name,
    mart_with_date_range.stage_name,
    mart_with_date_range.section_name,
    mart_with_date_range.group_name,
    mart_with_date_range.is_smau,
    mart_with_date_range.is_gmau,
    mart_with_date_range.is_umau,
    COUNT(*) AS event_count,
    COUNT(DISTINCT(month_plan.dim_ultimate_parent_namespace_id)) AS ultimate_parent_namespace_count,
    COUNT(DISTINCT( mart_with_date_range.dim_user_id)) AS user_count
  FROM mart_with_date_range
  LEFT JOIN month_plan
    ON mart_with_date_range.dim_ultimate_parent_namespace_id IS NOT DISTINCT FROM month_plan.dim_ultimate_parent_namespace_id           -- Treats Nulls as values to Join
    AND mart_with_date_range.event_calendar_month = month_plan.event_calendar_month
  WHERE mart_with_date_range.event_calendar_month < DATE_TRUNC('month', CURRENT_DATE)
  {{ dbt_utils.group_by(n=12) }}
  ORDER BY mart_with_date_range.event_calendar_month DESC, month_plan.plan_id_at_event_date DESC

)

{{ dbt_audit(
    cte_ref="mart_usage_event_plan_monthly",
    created_by="@dihle",
    updated_by="@tpoole",
    created_date="2022-02-22",
    updated_date="2022-09-15"
) }}
