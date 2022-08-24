WITH source AS (

  SELECT 
    created_date::DATE AS created_date,
    version::VARCHAR AS version,
    period_id::VARCHAR AS period_id,
    period_name::VARCHAR AS period_name,
    assignment_id::VARCHAR AS assignment_id,
    assignment_name::VARCHAR AS assignment_name,
    indicator::VARCHAR AS indicator,
    role::VARCHAR AS role,
    name::VARCHAR AS name,
    quota_amnt::VARCHAR AS quota_amnt,
    units::VARCHAR AS units,
    quota_type::VARCHAR AS quota_type,
    bhr_id::VARCHAR AS bhr_id,
    employee_id::VARCHAR AS employee_id,
    hc_type::VARCHAR AS hc_type,
    region::VARCHAR AS region,
    role_type::VARCHAR AS role_type,
    start_date::DATE AS start_date,
    end_date::DATE AS VARCHAR
  FROM {{ source('sheetload','fy23_quota') }}
  
        )
SELECT * 
FROM source
