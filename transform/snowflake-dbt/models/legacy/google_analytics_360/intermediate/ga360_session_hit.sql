WITH source AS (

	SELECT *
	FROM {{ ref('ga360_session_hit_source') }}

), custom_dimensions AS (

	SELECT *
	FROM {{ ref('ga360_session_hit_custom_dimension_xf') }}
	
), joined AS (

	SELECT 
		source.*,
		dims.dimension_index,
		dims.dimension_name,
		dim.dimension_value
	FROM source
	LEFT JOIN custom_dimensions AS dims
	ON dims.visit_id = source.visit_id 
		AND dims.visitor_id = source.visitor_id 
		    AND dims.hit_number = source.hit_number

)


SELECT *
FROM joined
