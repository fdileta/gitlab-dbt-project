{{ config(
    materialized='table'
) }}

{{ simple_cte([
    ('projects','gitlab_dotcom_projects_source')
]) }},

project_namespace_map AS (

  SELECT DISTINCT
    {{ dbt_utils.surrogate_key(['project_id']) }} AS dim_project_sk,
    {{ dbt_utils.surrogate_key(['project_namespace_id']) }} AS dim_namespace_sk,
    project_id,
    project_namespace_id AS namespace_id
  FROM projects

)

SELECT *
FROM project_namespace_map
