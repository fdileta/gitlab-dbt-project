WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_project_features_source') }}

)

SELECT *
FROM source
