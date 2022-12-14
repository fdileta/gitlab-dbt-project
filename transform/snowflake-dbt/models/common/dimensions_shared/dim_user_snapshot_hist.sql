{{ config({
        "materialized": "table",
        "unique_key": "dim_user_snapshot_hist_id",
        "tags": ["edm_snapshot", "user_snapshot"]
    })
}}

    SELECT
     {{ dbt_utils.surrogate_key(['dbt_updated_at', 'dim_user_sk']) }} AS dim_user_snapshot_hist_id,
     *
    FROM {{ source('snapshot','dim_user_snapshot') }}

{{ dbt_audit(
    created_by="@tpoole",
    updated_by="@tpoole",
    created_date="2022-12-14",
    updated_date="2022-12-14"
) }}