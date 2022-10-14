{{ config(
    tags=["mnpi_exception"]
) }}

WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_protected_branch_merge_access_levels') }}

)

SELECT *
FROM source
