{{ config(
    materialized='ephemeral'
) }}

{{ simple_cte([
    ('projects','gitlab_dotcom_projects_snapshots_base')
]) }},

-- In the event a project is deleted we still want to know the historic map to namespace
list AS (
  SELECT DISTINCT
    project_id,
    project_namespace_id
  FROM projects
  WHERE project_namespace_id IS NOT NULL

)

SELECT *
FROM list