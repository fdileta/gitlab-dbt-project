WITH source AS (

	SELECT *
	FROM {{ source('google_analytics_360', 'session_hit_custom_dimension') }}

)

SELECT *
FROM source