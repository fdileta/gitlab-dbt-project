WITH source AS (

    SELECT *
    FROM {{ source('snapshots','gitlab_dotcom_user_preferences_snapshots') }}

), renamed AS (

    SELECT
      *
    FROM source
    
)

SELECT  *
FROM renamed