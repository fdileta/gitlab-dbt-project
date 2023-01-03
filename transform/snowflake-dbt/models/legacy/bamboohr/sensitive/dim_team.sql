
WITH supervisory_orgs AS (

    SELECT *
    FROM {{ ref('workday_supervisory_organizations_source') }}
), 

recursive_hierarchy AS (
    SELECT
        root.team_id, 
        root.team_name, 
        root.team_manager_name, 
        root.team_hierarchy_level, 
        root.cost_center,
        root.team_superior_team_id,
        TO_ARRAY(root.valid_to)                   AS valid_to_list,
        TO_ARRAY(root.valid_from)                 AS valid_from_list,
        TO_ARRAY(root.team_superior_team_id)      AS upstream_organizations
    FROM supervisory_orgs AS root
    WHERE team_superior_team_id IS NULL

    UNION ALL 

    SELECT
        iter.team_id, 
        iter.team_name, 
        iter.team_manager_name, 
        iter.team_hierarchy_level, 
        iter.cost_center,
        iter.team_superior_team_id,
        ARRAY_APPEND(anchor.valid_to_list, iter.valid_to)           AS valid_to_list,
        ARRAY_APPEND(anchor.valid_from_list, iter.valid_from)       AS valid_from_list,
        ARRAY_APPEND(anchor.upstream_organizations, iter.team_id)   AS upstream_organizations
    FROM recursive_hierarchy AS anchor
    INNER JOIN supervisory_orgs AS iter
        ON iter.team_superior_team_id = anchor.team_id
            AND NOT ARRAY_CONTAINS(iter.team_id::VARIANT, anchor.upstream_organizations)
      
  ),