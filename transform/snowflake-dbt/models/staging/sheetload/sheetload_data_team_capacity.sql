
WITH source AS (

    SELECT *
    FROM {{ ref('sheetload_data_team_capacity') }}

)

SELECT *
FROM source
