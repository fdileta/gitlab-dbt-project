WITH source AS (

  SELECT *
  FROM {{ ref ('blended_employee_mapping_source') }}

),

stage AS (

  SELECT
    employee_number,
    employee_id,
    first_name,
    last_name,
    hire_date,
    termination_date,
    first_inactive_date,
    job_role,
    job_grade,
    cost_center,
    CASE
      WHEN jobtitle_speciality_multi_select IS NULL
        AND jobtitle_speciality_single_select IS NULL
        THEN NULL
      WHEN jobtitle_speciality_single_select IS NULL
        THEN jobtitle_speciality_multi_select
      WHEN jobtitle_speciality_multi_select IS NULL
        THEN jobtitle_speciality_single_select
      ELSE jobtitle_speciality_single_select || ',' || jobtitle_speciality_multi_select
    END AS jobtitle_speciality,
    gitlab_username,
    pay_frequency,
    sales_geo_differential,
    region,
    DATE_TRUNC('day', uploaded_at) AS effective_date,
    {{ dbt_utils.surrogate_key(['employee_id', 'job_role', 'job_grade', 
                                'cost_center', 'jobtitle_speciality', 
                                'gitlab_username', 'pay_frequency', 
                                'sales_geo_differential']) }} AS unique_key,
    LEAD(unique_key) OVER (PARTITION BY employee_id 
      ORDER BY effective_date DESC) as prior_unique_key
  FROM source
  WHERE
    CASE
      WHEN source_system = 'workday'
        AND uploaded_at::date >= '2022-06-16'
        THEN 1
      WHEN source_system = 'bamboohr'
        AND uploaded_at::date <= '2022-06-16'
        THEN 1
      ELSE 0
      END = 1

),

intermediate AS (

  SELECT *
  FROM stage
  WHERE 
    CASE
      WHEN unique_key != prior_unique_key
        THEN 1
      WHEN prior_unique_key IS NULL
        THEN 1
      ELSE 0
      END = 1

),

final AS (

  SELECT
    *,
    LEAD(DATEADD('day', -1, DATE_TRUNC('day', effective_date))) OVER (PARTITION BY employee_number
      ORDER BY effective_date) AS next_effective_date
  FROM intermediate
  WHERE effective_date >= '2020-02-27'  --1st day we started capturing job role

)

SELECT *
FROM final