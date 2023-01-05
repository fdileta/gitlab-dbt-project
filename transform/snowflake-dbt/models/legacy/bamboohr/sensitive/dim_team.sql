
WITH supervisory_orgs AS (

    SELECT *
    FROM {{ ref('workday_supervisory_organizations_source') }}
), 

recursive_hierarchy AS (
    SELECT
        root.team_id, 
        root.team_hierarchy_level,
        root.team_members_count,
        root.team_manager_inherited,
        root.team_inactivated,
        root.team_name, 
        root.team_manager_name, 
        root.team_manager_name_id,
        root.team_superior_team_id,
        root.team_inactivated_date,
        TO_ARRAY(root.valid_to)                                                          AS valid_to_list,
        TO_ARRAY(root.valid_from)                                                        AS valid_from_list,
        TO_ARRAY(root.team_id)                                                           AS upstream_organizations
    FROM supervisory_orgs AS root
    WHERE team_superior_team_id IS NULL

    UNION ALL 

    SELECT
        iter.team_id, 
        iter.team_hierarchy_level,
        iter.team_members_count,
        iter.team_manager_inherited,
        iter.team_inactivated,
        iter.team_name, 
        iter.team_manager_name, 
        iter.team_manager_name_id,
        iter.team_superior_team_id,
        iter.team_inactivated_date,
        ARRAY_APPEND(anchor.valid_to_list, iter.valid_to)                                AS valid_to_list,
        ARRAY_APPEND(anchor.valid_from_list, iter.valid_from)                            AS valid_from_list,
        ARRAY_APPEND(anchor.upstream_organizations, iter.team_id)                        AS upstream_organizations
    FROM recursive_hierarchy AS anchor
    INNER JOIN supervisory_orgs AS iter
        ON iter.team_superior_team_id = anchor.team_id
            AND NOT ARRAY_CONTAINS(iter.team_id::VARIANT, anchor.upstream_organizations)
      
),

final AS (

    SELECT 
        {{ dbt_utils.surrogate_key(['team_id', 'team_hierarchy_level', 'team_members_count','team_manager_inherited','team_inactivated','team_name','team_manager_name', 'team_manager_name_id', 'team_superior_team_id', 'team_inactivated_date']) }} 
                                                                                         AS dim_team_sk,
        recursive_hierarchy.team_id, 
        recursive_hierarchy.team_hierarchy_level,
        recursive_hierarchy.team_members_count,
        recursive_hierarchy.team_manager_inherited,
        recursive_hierarchy.team_inactivated,
        recursive_hierarchy.team_name, 
        recursive_hierarchy.team_manager_name, 
        recursive_hierarchy.team_manager_name_id,
        recursive_hierarchy.team_superior_team_id,
        recursive_hierarchy.team_inactivated_date,
        IFF(recursive_hierarchy.team_inactivated IS NULL,
          TRUE, FALSE)                                                                   AS is_currently_valid,
        NULLIF(recursive_hierarchy.upstream_organizations[0],'')::VARCHAR                AS hierarchy_level_1,
        NULLIF(recursive_hierarchy.upstream_organizations[1],'')::VARCHAR                AS hierarchy_level_2,
        NULLIF(recursive_hierarchy.upstream_organizations[2],'')::VARCHAR                AS hierarchy_level_3,
        NULLIF(recursive_hierarchy.upstream_organizations[3],'')::VARCHAR                AS hierarchy_level_4,
        NULLIF(recursive_hierarchy.upstream_organizations[4],'')::VARCHAR                AS hierarchy_level_5,
        NULLIF(recursive_hierarchy.upstream_organizations[5],'')::VARCHAR                AS hierarchy_level_6,
        NULLIF(recursive_hierarchy.upstream_organizations[6],'')::VARCHAR                AS hierarchy_level_7,
        NULLIF(recursive_hierarchy.upstream_organizations[7],'')::VARCHAR                AS hierarchy_level_8,
        NULLIF(recursive_hierarchy.upstream_organizations[8],'')::VARCHAR                AS hierarchy_level_9
    FROM recursive_hierarchy

)

SELECT * 
FROM final



