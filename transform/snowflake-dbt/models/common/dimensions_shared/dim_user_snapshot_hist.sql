{{ config({
        "materialized": "table",
        "unique_key": "dim_user_snapshot_hist_id",
        "tags": ["edm_snapshot", "user_snapshots"]
    })
}}

WITH dim_user_snapshot AS (
 
    SELECT
     {{ dbt_utils.surrogate_key(['dbt_updated_at', 'dim_user_sk']) }} AS dim_user_snapshot_hist_id,
     *
    FROM {{ source('snapshots','dim_user_snapshot') }}

)    

{{ dbt_audit(
    cte_ref = "dim_user",
    created_by="@tpoole",
    updated_by="@tpoole",
    created_date="2022-12-14",
    updated_date="2022-12-14"
) }}