WITH base AS (

   SELECT *
   FROM {{ source('gitlab_dotcom', 'fork_network_members') }}

)

{{ scd_latest_state(source='base', max_column='_task_instance') }}