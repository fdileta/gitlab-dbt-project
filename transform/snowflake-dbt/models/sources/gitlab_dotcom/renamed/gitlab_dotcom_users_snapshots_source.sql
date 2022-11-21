WITH source AS (

    SELECT *
    FROM {{ source('snapshots','gitlab_dotcom_users_snapshots') }}

), renamed AS (

    SELECT
      id::NUMBER AS user_id,
      CASE 
          WHEN state in ('blocked', 'banned') THEN TRUE
          ELSE FALSE 
      END::VARCHAR AS is_blocked_user,      
      SPLIT_PART(COALESCE(notification_email, email), '@', 2)::VARCHAR AS notification_email_domain,
      SPLIT_PART(email, '@', 2)::VARCHAR AS email_domain,      
      SPLIT_PART(public_email, '@', 2)::VARCHAR AS public_email_domain,
      IFF(SPLIT_PART(commit_email, '@', 2) = '', NULL, SPLIT_PART(commit_email, '@', 2))::VARCHAR AS commit_email_domain,
      {{ user_role_mapping(user_role='role') }}::VARCHAR AS role_description,
      COALESCE(TO_DATE(last_activity_on)::VARCHAR,'Unknown') AS last_activity_date,              
      COALESCE(TO_DATE(last_sign_in_at)::VARCHAR,'Unknown')  AS last_sign_in_date, 
      admin::BOOLEAN AS is_admin,              
      *
    FROM source
    
)

SELECT  *
FROM renamed