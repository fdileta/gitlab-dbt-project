WITH source AS (

  SELECT *
  FROM {{ ref ('blended_employee_mapping_source') }}

),

intermediate AS (

  SELECT
    employee_number,
    employee_id,
    first_name,
    last_name,
    hire_date,
    termination_date,
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
                                'sales_geo_differential']) }} AS unique_key
  FROM source
  QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_key
    ORDER BY DATE_TRUNC('day', effective_date) ASC, DATE_TRUNC('hour', effective_date) DESC) = 1
  /*
  This type of filtering does not account for a change back to a previous value
  and will return incorrect ranges for effective values.  This can be solved with a
  gaps and islands solution
  */

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
