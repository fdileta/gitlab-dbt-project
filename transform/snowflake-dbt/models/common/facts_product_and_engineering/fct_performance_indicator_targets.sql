{{ config(
    materialized='table',
    tags=["mnpi_exception", "product"]
) }}

{{ simple_cte([
    ('pi_targets', 'prep_performance_indicators_yaml'),
    ('dim_date', 'dim_date'),
    ])

}},

first_day_of_month AS (

  SELECT DISTINCT first_day_of_month AS reporting_month
  FROM dim_date

),

/*
Grab the most recent record of each pi_metric_name that does not have a NULL estimated target
*/

most_recent_yml_record AS (

  SELECT *
  FROM pi_targets
  WHERE pi_monthly_estimated_targets IS NOT NULL
  QUALIFY ROW_NUMBER() OVER (PARTITION BY pi_metric_name ORDER BY snapshot_date DESC) = 1

),

/*
Flatten the json record from the yml file to get the target value and end month for each key:value pair
*/

flattened_monthly_targets AS (

  SELECT
    pi_metric_name,
    d.value,
    PARSE_JSON(d.path)[0]::TIMESTAMP AS target_end_month
  FROM most_recent_yml_record,
    LATERAL FLATTEN(INPUT => PARSE_JSON(pi_monthly_estimated_targets), OUTER => TRUE) AS d

),

/*
Calculate the reporting intervals for the pi_metric_name. Each row will have a start and end date
Determine start month by checking if the previous record has a target_end_date.
- If yes, then target_start_month = target_end_date from previous record
- If no, then target_start_month = 2017-01-01 (earliest available product data, an arbitrary date in the past)
*/

monthly_targets_with_intervals AS (

  SELECT
    *,
    COALESCE(
      LAG(target_end_month) OVER (PARTITION BY pi_metric_name ORDER BY target_end_month),
      '2017-01-01' --year of GitLab initial release date
    ) AS target_start_month
  FROM flattened_monthly_targets

),

/*
  Join each pi_metric_name and value to the reporting_month it corresponds with
  Join on reporting_month greater than metric start_month and reporting_month less than or equal to the target end month/CURRENT_DATE
*/

final_targets AS (

  SELECT
    {{ dbt_utils.surrogate_key(['reporting_month', 'pi_metric_name']) }} AS performance_indicator_targets_pk,
    reporting_month                                                      AS reporting_month,
    pi_metric_name                                                       AS pi_metric_name,
    value                                                                AS target_value
  FROM first_day_of_month
  INNER JOIN monthly_targets_with_intervals
  WHERE reporting_month > target_start_month
    AND reporting_month <= COALESCE(target_end_month, CURRENT_DATE)

),

results AS (

  SELECT *
  FROM final_targets

)

{{ dbt_audit(
    cte_ref="results",
    created_by="@dihle",
    updated_by="@cbraza",
    created_date="2022-04-20",
    updated_date="2022-12-23"
) }}
