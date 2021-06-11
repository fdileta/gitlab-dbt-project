WITH zuora_revenue_revenue_contract_schedule_deleted AS (

    SELECT *
    FROM {{source('zuora_revenue','zuora_revenue_revenue_contract_schedule_deleted')}}

), renamed AS (

    SELECT 
    
      schd_id::VARCHAR              AS revenue_contract_schedule_id,
      client_id::VARCHAR            AS client_id,
      deleted_time::DATETIME        AS revenue_contract_schedule_deleted_at,
      crtd_by::VARCHAR              AS revenue_contract_schedule_created_by,
      crtd_dt::DATE                 AS revenue_contract_schedule_created_date,
      updt_by::DATE                 AS revenue_contract_schedule_updated_by,
      updt_dt::DATE                 AS revenue_contract_schedule_updated_date,
      incr_updt_dt::DATE            AS incremental_update_date

    FROM zuora_revenue_revenue_contract_schedule_deleted

)

SELECT *
FROM renamed 