{{ config(
    tags=["product"]
) }}

{{ config({
    "materialized": "table",
    "unique_key": "user_snapshot_id"
    })
}}

{{ simple_cte([
    ('dim_date', 'dim_date'),
    ('users_snapshots', 'gitlab_dotcom_users_snapshots_source'),
    ('email_classification', 'driveload_email_domain_classification_source'),
    ('identities_snapshots','gitlab_dotcom_identities_snapshots_source'),
    ('preferences_snapshots','gitlab_dotcom_user_preferences_snapshots_source'),
    ('details_snapshots','gitlab_dotcom_user_details_snapshots_source'),
    ('leads_snapshots','customers_db_leads_snapshots_source')

]) }} 

,snapshot_dates AS (

    SELECT *
    FROM dim_date
    --WHERE date_actual >= '2020-03-01' and date_actual <= CURRENT_DATE
    WHERE date_actual >= '2022-10-05' and date_actual <= CURRENT_DATE

), user_spined AS (

    SELECT
        snapshot_dates.date_id AS snapshot_date_id,
        users_snapshots.*
    FROM users_snapshots
    INNER JOIN snapshot_dates
        ON snapshot_dates.date_actual >= users_snapshots.dbt_valid_from
        AND snapshot_dates.date_actual < {{ coalesce_to_infinity('users_snapshots.dbt_valid_to') }}

), email_classification AS (

    SELECT 
    *
    FROM email_classification

-- ), closest_provider  ????????? AS (

--     SELECT
--         user_snapshots.user_id AS user_id,
--         identity.identity_provider AS identity_provider
--     FROM user_snapshots                                                       
--     LEFT JOIN identity
--     ON user_snapshots.user_id = identity.user_id
--     WHERE 
--         identity.user_id IS NOT NULL
--     QUALIFY ROW_NUMBER() OVER(PARTITION BY user_snapshots.user_id 
--         ORDER BY TIMEDIFF(MILLISECONDS,users_source.created_at,COALESCE(identity.created_at,{{var('infinity_future')}})) ASC) = 1

),identity_snapshot_spined AS (

    SELECT
        snapshot_dates.date_id AS snapshot_date_id,
        identities_snapshots.*
    FROM 
        identities_snapshots
        INNER JOIN snapshot_dates
        ON snapshot_dates.date_actual >= identities_snapshots.dbt_valid_from
        AND snapshot_dates.date_actual < {{ coalesce_to_infinity('identities_snapshots.dbt_valid_to') }}

),closest_provider_spined AS (

    SELECT
        user_spined.user_id,
        user_spined.snapshot_date_id,
        identity_snapshot_spined.provider AS identity_provider
    FROM 
        user_spined
        LEFT JOIN identity_snapshot_spined
        ON user_spined.user_id = identity_snapshot_spined.user_id
        AND user_spined.snapshot_date_id = identity_snapshot_spined.snapshot_date_id

    WHERE 
        identity_snapshot_spined.user_id IS NOT NULL
    QUALIFY ROW_NUMBER() OVER(PARTITION BY user_spined.id, user_spined.snapshot_date_id 
            ORDER BY TIMEDIFF(MILLISECONDS,user_spined.created_at,COALESCE(identity_snapshot_spined.created_at,{{var('infinity_future')}})) ASC) = 1       

), preferences_spined AS (

    SELECT 
        preferences_snapshots.user_id AS user_id,
        snapshot_dates.date_id AS snapshot_date_id,
        COALESCE(preferences_snapshots.setup_for_company::VARCHAR,'Unknown') AS setup_for_company
    FROM preferences_snapshots
    INNER JOIN snapshot_dates
        ON snapshot_dates.date_actual >= preferences_snapshots.dbt_valid_from
        AND snapshot_dates.date_actual < {{ coalesce_to_infinity('preferences_snapshots.dbt_valid_to') }}

), details_snapshots_spined AS (

    SELECT 
        details_snapshots.user_id AS user_id,
        snapshot_dates.date_id AS snapshot_date_id,
        CASE COALESCE(details_snapshots.registration_objective,-1)
            WHEN 0 THEN 'basics' 
            WHEN 1 THEN 'move_repository' 
            WHEN 2 THEN 'code_storage' 
            WHEN 3 THEN 'exploring' 
            WHEN 4 THEN 'ci' 
            WHEN 5 THEN 'other' 
            WHEN 6 THEN 'joining_team'
            WHEN -1 THEN 'Unknown'
        END AS jobs_to_be_done
    FROM details_snapshots
    INNER JOIN snapshot_dates
        ON snapshot_dates.date_actual >= details_snapshots.dbt_valid_from
        AND snapshot_dates.date_actual < {{ coalesce_to_infinity('details_snapshots.dbt_valid_to') }}

), max_leads_snapshots AS (

    SELECT 
        user_id,
        dbt_valid_from,
        {{ coalesce_to_infinity('dbt_valid_to') }} AS dbt_valid_to,
        MAX(is_for_business_use) AS is_for_business_use,
        MAX(employees_bucket) AS employees_bucket,
        MAX(country) AS country,
        MAX(state) AS state
    FROM 
        leads_snapshots 
    GROUP BY
        user_id,
        dbt_valid_from,
        dbt_valid_to
    
), leads_spined AS ( 

    SELECT 
        max_leads_snapshots.user_id,
        snapshot_dates.date_id AS snapshot_date_id,
        COALESCE(MAX(max_leads_snapshots.is_for_business_use)::VARCHAR,'Unknown') AS for_business_use,
        COALESCE(MAX(max_leads_snapshots.employees_bucket)::VARCHAR,'Unknown') AS employee_count,
        COALESCE(MAX(max_leads_snapshots.country)::VARCHAR,'Unknown') AS country,
        COALESCE(MAX(max_leads_snapshots.state)::VARCHAR,'Unknown') AS state
    FROM max_leads_snapshots
    INNER JOIN snapshot_dates
        ON snapshot_dates.date_actual >= max_leads_snapshots.dbt_valid_from
        AND snapshot_dates.date_actual < max_leads_snapshots.dbt_valid_to
    GROUP BY
        max_leads_snapshots.user_id,
        snapshot_dates.date_id

 ), renamed AS (

    SELECT  
        --surrogate_key
        {{ dbt_utils.surrogate_key(['user_spined.user_id','user_spined.snapshot_date_id']) }}  AS user_snapshot_id,
        user_spined.snapshot_date_id AS spined_date_id,
        {{ dbt_utils.surrogate_key(['user_spined.user_id']) }}  AS dim_user_sk,
                
        --natural_key
        user_spined.user_id AS user_id,
        
        --legacy natural_key to be deprecated during change management plan
        user_spined.user_id AS dim_user_id,
        
        --Other attributes
        user_spined.remember_created_at AS remember_created_at,
        user_spined.sign_in_count AS sign_in_count,
        user_spined.current_sign_in_at AS current_sign_in_at,
        user_spined.last_sign_in_at AS last_sign_in_at,
        user_spined.created_at AS created_at,
        --snapshot_dates.date_id AS created_date_id,  (not in Dim_User)
        user_spined.updated_at AS updated_at,
        user_spined.admin AS is_admin,
        --user_spined.state AS user_state,  (not in Dim_User)
        CASE 
            WHEN user_spined.state in ('blocked', 'banned') THEN TRUE
            ELSE FALSE 
        END AS is_blocked_user,
        
        SPLIT_PART(COALESCE(user_spined.notification_email, email), '@', 2)     AS notification_email_domain,
        notification_email_domain.classification                                AS notification_email_domain_classification,
        SPLIT_PART(user_spined.email, '@', 2)                                   AS email_domain,
        email_domain.classification                                             AS email_domain_classification,
        
        SPLIT_PART(user_spined.public_email, '@', 2)                            AS public_email_domain,
        public_email_domain.classification                                      AS public_email_domain_classification,
        IFF(SPLIT_PART(user_spined.commit_email, '@', 2) = '', NULL, SPLIT_PART(user_spined.commit_email, '@', 2)) AS commit_email_domain,
        commit_email_domain.classification                                      AS commit_email_domain_classification,
        closest_provider.identity_provider                                      AS identity_provider,

        -- Expanded Attributes  (Not Found = Joined Row Not found for the Attribute)
        {{ user_role_mapping(user_role='role') }}::VARCHAR AS role,
        COALESCE(TO_DATE(user_spined.last_activity_on)::VARCHAR,'Unknown') AS last_activity_date,              
        COALESCE(TO_DATE(user_spined.last_sign_in_at)::VARCHAR,'Unknown')  AS last_sign_in_date,               
        COALESCE(preferences_spined.setup_for_company,'Not Found') AS setup_for_company,               
        COALESCE(details_spined.jobs_to_be_done,'Not Found') AS jobs_to_be_done,
        COALESCE(leads_spined.for_business_use,'Not Found') AS for_business_use,                 
        COALESCE(leads_spined.employee_count,'Not Found') AS employee_count,
        COALESCE(leads_spined.country,'Not Found') AS country,
        COALESCE(leads_spined.state,'Not Found') AS state

    FROM user_spined
    LEFT JOIN email_classification AS notification_email_domain
        ON SPLIT_PART(COALESCE(user_spined.notification_email, email), '@', 2) = notification_email_domain.domain 
    LEFT JOIN email_classification AS email_domain
        ON SPLIT_PART(user_spined.email, '@', 2) = email_domain.domain 
    LEFT JOIN email_classification AS public_email_domain
        ON SPLIT_PART(user_spined.public_email, '@', 2) = public_email_domain.domain 
    LEFT JOIN email_classification AS commit_email_domain
        ON IFF(SPLIT_PART(user_spined.commit_email, '@', 2) = '', NULL, SPLIT_PART(user_spined.commit_email, '@', 2)) = commit_email_domain.domain    
    LEFT JOIN closest_provider_spined AS closest_provider
        ON user_spined.id = closest_provider.user_id 
        AND user_spined.snapshot_date_id = closest_provider.snapshot_date_id
    LEFT JOIN preferences_spined  AS preferences_spined
        ON user_spined.id = preferences_spined.user_id
        AND user_spined.snapshot_date_id = preferences_spined.snapshot_date_id
    LEFT JOIN details_spined AS details_spined
        ON user_spined.id = details_spined.user_id
        AND user_spined.snapshot_date_id = details_spined.snapshot_date_id
    LEFT JOIN leads_spined AS leads_spined
        ON user_spined.id = leads_spined.user_id
        AND user_spined.snapshot_date_id = leads_spined.snapshot_date_id
    LEFT JOIN snapshot_dates
        ON TO_DATE(user_spined.created_at) = snapshot_dates.date_day

)

{{ dbt_audit(
    cte_ref="renamed",
    created_by="tpoole",
    updated_by="@tpoole",
    created_date="2022-11-16",
    updated_date="2022-11-16"
) }}
