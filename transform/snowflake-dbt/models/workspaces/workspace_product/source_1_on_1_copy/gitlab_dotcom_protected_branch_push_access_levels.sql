WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_protected_branch_push_access_levels_source') }}

)

SELECT *
FROM source
