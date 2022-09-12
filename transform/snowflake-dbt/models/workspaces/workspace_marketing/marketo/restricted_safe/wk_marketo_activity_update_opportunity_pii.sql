WITH source AS (

    SELECT *
    FROM {{ ref('marketo_activity_update_opportunity_source_pii') }}

)

SELECT *
FROM source