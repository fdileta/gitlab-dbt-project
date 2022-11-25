{{ config({
       "materialized": "table"
       })
}}


WITH
{{ distinct_source(source=source('gitlab_dotcom', 'fork_network_members'))}}

, renamed AS (

   SELECT


    id::NUMBER 				                AS id,
    fork_network_id::NUMBER 		        AS fork_network_id,
    project_id::NUMBER 		                AS project_id,
    forked_from_project_id::NUMBER 	        AS forked_from_project_id

   FROM distinct_source

)

/* Note: the primary key used is namespace_id, not subscription id.
  This matches our business use case better. */
{{ scd_type_2(
   primary_key_renamed='id',
   primary_key_raw='id'
) }}
