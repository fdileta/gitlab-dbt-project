{{ config(
    tags=["product"]
) }}

{{ config({
    "materialized": "table",
    "unique_key": "dim_user_id"
    })
}}

{{ simple_cte([
    ('dim_date', 'dim_date'),
    ('source', 'gitlab_dotcom_users_source'),
    ('email_classification', 'driveload_email_domain_classification_source'),
    ('identity','gitlab_dotcom_identities_source'),
    ('gitlab_dotcom_user_preferences_source','gitlab_dotcom_user_preferences_source'),
    ('gitlab_dotcom_user_details_source','gitlab_dotcom_user_details_source'),
    ('customers_db_leads_source','customers_db_leads_source')

]) }}, 

email_classification_dedup AS (

  SELECT 
    *
  FROM email_classification
  QUALIFY ROW_NUMBER() OVER(PARTITION BY domain ORDER BY domain DESC) = 1

), 

closest_provider AS (

  SELECT
    source.user_id AS user_id,
    identity.identity_provider AS identity_provider
  FROM source                                                       
  LEFT JOIN identity 
    ON source.user_id = identity.user_id
  WHERE 
    identity.user_id IS NOT NULL
  QUALIFY ROW_NUMBER() OVER(PARTITION BY source.user_id 
      ORDER BY TIMEDIFF(MILLISECONDS,source.created_at,COALESCE(identity.created_at,{{var('infinity_future')}})) ASC) = 1

), 

user_preferences AS (

  SELECT 
    user_id AS user_id,
    COALESCE(setup_for_company::VARCHAR,'Unknown') AS setup_for_company
  FROM gitlab_dotcom_user_preferences_source

),

user_details AS (

  SELECT 
    user_id AS user_id,
    CASE COALESCE(registration_objective,-1)
      WHEN 0 THEN 'basics' 
      WHEN 1 THEN 'move_repository' 
      WHEN 2 THEN 'code_storage' 
      WHEN 3 THEN 'exploring' 
      WHEN 4 THEN 'ci' 
      WHEN 5 THEN 'other' 
      WHEN 6 THEN 'joining_team'
      WHEN -1 THEN 'Unknown'
    END AS jobs_to_be_done
  FROM gitlab_dotcom_user_details_source

),

customer_leads AS (

  SELECT 
    user_id AS user_id,
    COALESCE(MAX(is_for_business_use)::VARCHAR,'Unknown') AS for_business_use,
    COALESCE(MAX(employees_bucket)::VARCHAR,'Unknown') AS employee_count,
    COALESCE(MAX(country)::VARCHAR,'Unknown') AS country,
    COALESCE(MAX(state)::VARCHAR,'Unknown') AS state
  FROM customers_db_leads_source
  GROUP BY
    user_id

),


renamed AS (

  SELECT
    --surrogate_key
    {{ dbt_utils.surrogate_key(['source.user_id']) }}  AS dim_user_sk,
    
    --natural_key
    source.user_id,
    
    --legacy natural_key to be deprecated during change management plan
    source.user_id AS dim_user_id,
    
    --Other attributes
    source.remember_created_at AS remember_created_at,
    source.sign_in_count AS sign_in_count,
    source.current_sign_in_at AS current_sign_in_at,
    source.last_sign_in_at AS last_sign_in_at,
    source.created_at AS created_at,
    dim_date.date_id AS created_date_id,
    source.updated_at AS updated_at,
    source.is_admin AS is_admin,
    source.state AS user_state,
    CASE 
      WHEN source.state in ('blocked', 'banned') THEN TRUE
      ELSE FALSE 
    END AS is_blocked_user,
    source.notification_email_domain AS notification_email_domain,
    notification_email_domain.classification AS notification_email_domain_classification,
    source.email_domain AS email_domain,
    email_domain.classification AS email_domain_classification,
    source.public_email_domain AS public_email_domain,
    public_email_domain.classification AS public_email_domain_classification,
    source.commit_email_domain AS commit_email_domain,
    commit_email_domain.classification AS commit_email_domain_classification,
    closest_provider.identity_provider AS identity_provider,

    -- Expanded Attributes  (Not Found = Joined Row Not found for the Attribute)
    COALESCE(source.role,'Unknown') AS role,
    COALESCE(TO_DATE(source.last_activity_on)::VARCHAR,'Unknown') AS last_activity_date,              
    COALESCE(TO_DATE(source.last_sign_in_at)::VARCHAR,'Unknown')  AS last_sign_in_date,               
    COALESCE(user_preferences.setup_for_company,'Not Found') AS setup_for_company,               
    COALESCE(user_details.jobs_to_be_done,'Not Found') AS jobs_to_be_done,
    COALESCE(customer_leads.for_business_use,'Not Found') AS for_business_use,                 
    COALESCE(customer_leads.employee_count,'Not Found') AS employee_count,
    COALESCE(customer_leads.country,'Not Found') AS country,
    COALESCE(customer_leads.state,'Not Found') AS state

  FROM source
  LEFT JOIN dim_date
    ON TO_DATE(source.created_at) = dim_date.date_day
  LEFT JOIN email_classification_dedup AS notification_email_domain
    ON notification_email_domain.domain = source.notification_email_domain
  LEFT JOIN email_classification_dedup AS email_domain
    ON email_domain.domain = source.email_domain
  LEFT JOIN email_classification_dedup AS public_email_domain
    ON public_email_domain.domain = source.public_email_domain
  LEFT JOIN email_classification_dedup AS commit_email_domain
    ON commit_email_domain.domain = source.commit_email_domain
  LEFT JOIN closest_provider AS closest_provider
    ON source.user_id = closest_provider.user_id  
  LEFT JOIN user_preferences  AS user_preferences
    ON source.user_id = user_preferences.user_id
  LEFT JOIN user_details AS user_details
    ON source.user_id = user_details.user_id
  LEFT JOIN customer_leads AS customer_leads
    ON source.user_id = customer_leads.user_id

)

{{ dbt_audit(
    cte_ref="renamed",
    created_by="@mpeychet",
    updated_by="@tpoole",
    created_date="2021-05-31",
    updated_date="2022-08-25"
) }}
