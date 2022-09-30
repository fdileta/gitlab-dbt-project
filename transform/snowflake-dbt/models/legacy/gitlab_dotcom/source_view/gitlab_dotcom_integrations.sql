WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_integrations_source') }}

)

SELECT *
FROM source
