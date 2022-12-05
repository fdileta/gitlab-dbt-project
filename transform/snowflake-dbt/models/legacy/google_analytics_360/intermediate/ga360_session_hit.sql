WITH source AS (

	SELECT *
	FROM {{ ref('ga360_session_hit_source') }}

), custom_dimensions AS (

	SELECT *
	FROM {{ ref('ga360_session_custom_dimension_xf') }}
	
), joined AS (

	SELECT 
		source.*,
		dims.dimension_index,
		dims.dimension_name
	FROM source
	LEFT JOIN custom_dimensions AS dims
	ON dims.visit_id = source.visit_id 
		AND dims.visitor_id = source.visitor_id 
		AND dims.visit_start_time = source.visit_start_time
	WHERE dims.dimension_scope = 'Hit'

)

SELECT *
FROM joined
