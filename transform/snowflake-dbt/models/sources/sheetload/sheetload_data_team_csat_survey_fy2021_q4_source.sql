WITH source AS (
  
    SELECT * 
    FROM {{ source('sheetload','data_team_csat_survey_fy2021_q4') }}

), final AS (
    
    SELECT *			               		
    FROM source
      
) 

SELECT * 
FROM final

