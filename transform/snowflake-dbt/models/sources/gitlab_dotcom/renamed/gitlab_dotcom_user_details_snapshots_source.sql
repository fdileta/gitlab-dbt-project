WITH source AS (

    SELECT *
    FROM {{ source('snapshots','gitlab_dotcom_user_details_snapshots') }}

), renamed AS (

    SELECT
      CASE COALESCE(registration_objective,-1)
          WHEN 0 THEN 'basics' 
          WHEN 1 THEN 'move_repository' 
          WHEN 2 THEN 'code_storage' 
          WHEN 3 THEN 'exploring' 
          WHEN 4 THEN 'ci' 
          WHEN 5 THEN 'other' 
          WHEN 6 THEN 'joining_team'
          WHEN -1 THEN 'Unknown'
      END AS jobs_to_be_done,
      *
    FROM source
    
)

SELECT  *
FROM renamed