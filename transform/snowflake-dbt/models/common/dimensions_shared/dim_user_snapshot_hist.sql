{{ config({
        "materialized": "table",
        "unique_key": "dim_user_snapshot_hist_id",
        "tags": ["edm_snapshot", "user_snapshots"]
    })
}}

WITH dim_user_snapshot AS (

  SELECT
    {{ dbt_utils.surrogate_key(['dbt_updated_at', 'dim_user_sk']) }} AS dim_user_snapshot_hist_id,
    dim_user_sk,
	user_id,
	dim_user_id,
	remember_created_at,
	sign_in_count,
	current_sign_in_at,
	last_sign_in_at,
	created_at,
	updated_at,
	is_admin,
	is_blocked_user,
	notification_email_domain,
	notification_email_domain_classification,
	email_domain,
	email_domain_classification,
	is_valuable_signup,
	public_email_domain,
	public_email_domain_classification,
	commit_email_domain,
	commit_email_domain_classification,
	identity_provider,
	role,
	last_activity_date,
	last_sign_in_date,
	setup_for_company,
	jobs_to_be_done,
	for_business_use,
	employee_count,
	country,
	state,
	dbt_scd_id,
	dbt_valid_from,
	dbt_valid_to
  FROM {{ source('snapshots','dim_user_snapshot') }}
  
)

{{ dbt_audit(
    cte_ref = "dim_user_snapshot",
    created_by="@tpoole",
    updated_by="@tpoole",
    created_date="2022-12-21",
    updated_date="2022-12-21"
) }}
