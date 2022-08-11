WITH source AS (
  SELECT *
  FROM {{ ref('date_details_source') }}
)

{{ dbt_audit(
    cte_ref="source",
    created_by="@pempey",
    updated_by="@pempey",
    created_date="2022-08-11",
    updated_date="2022-08-11"
) }}
