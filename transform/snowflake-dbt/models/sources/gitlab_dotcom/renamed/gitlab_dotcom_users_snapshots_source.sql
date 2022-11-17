WITH source AS (

    SELECT *
    FROM {{ source('snapshots','gitlab_dotcom_users_snapshots') }}

), renamed AS (

    SELECT
      id::NUMBER                     AS user_id,
      *
    FROM source
    
)

SELECT  *
FROM renamed