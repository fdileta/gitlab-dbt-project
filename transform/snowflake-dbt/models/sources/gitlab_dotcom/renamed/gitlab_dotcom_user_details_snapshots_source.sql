WITH source AS (

    SELECT *
    FROM {{ source('snapshots','gitlab_dotcom_user_details_snapshots') }}

), renamed AS (

    SELECT
      *
    FROM source
    
)

SELECT  *
FROM renamed