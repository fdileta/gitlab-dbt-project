{{ config(
    materialized='table'
) }}

{{ simple_cte([
    ('members','gitlab_dotcom_members_source'),
    ('projects_map','prep_map_project_namespace'),
    ('access_levels','gitlab_dotcom_access_levels_source')
]) }},

remap_projects AS (

  SELECT
    members.*,
    access_levels.access_level_name,
    -- A project could be deleted
    COALESCE(IFF(members.member_source_type = 'Project',
      projects_map.project_namespace_id,
      members.source_id), -1) AS member_namespace_id
  FROM members
  LEFT JOIN projects_map
    ON members.source_id = projects_map.project_id
      AND members.member_source_type = 'Project'
  LEFT JOIN access_levels
    ON members.access_level = access_levels.access_level_id
),

fct AS (

  SELECT
    -- Primary Key
    {{ dbt_utils.surrogate_key(['remap_projects.member_id']) }} AS direct_member_pk,

    -- Foreign Keys
    {{ get_keyed_nulls(dbt_utils.surrogate_key(['remap_projects.user_id'])) }} AS dim_user_sk,
    {{ get_keyed_nulls(dbt_utils.surrogate_key(['remap_projects.member_namespace_id'])) }} AS dim_namespace_sk,

    -- Legacy Keys
    remap_projects.member_id,
    remap_projects.user_id,
    remap_projects.member_namespace_id AS namespace_id,

    -- Degenerate Dimensions
    remap_projects.access_level,
    remap_projects.access_level_name,
    remap_projects.member_type,
    remap_projects.created_at AS membership_created_at, -- change to created_at
    remap_projects.invite_accepted_at,
    remap_projects.requested_at,
    remap_projects.expires_at,
    IFF(remap_projects.invite_token IS NOT NULL, TRUE, FALSE) AS is_invite,
    IFF(remap_projects.invite_accepted_at IS NOT NULL, TRUE, FALSE) AS is_past_invite,
    IFF(remap_projects.requested_at IS NOT NULL, TRUE, FALSE) AS is_request,
    remap_projects.valid_from,
    remap_projects.valid_to,
    remap_projects.is_currently_valid

    -- Facts

  FROM remap_projects

)

SELECT *
FROM fct
