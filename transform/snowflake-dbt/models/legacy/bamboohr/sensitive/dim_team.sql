
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
        root.valid_from,
        root.valid_to,
        TO_ARRAY(root.valid_to)                                                                                                               AS valid_to_list,
        TO_ARRAY(root.valid_from)                                                                                                             AS valid_from_list,
        TO_ARRAY(root.team_id)                                                                                                                AS upstream_organizations
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
        iter.valid_from,
        iter.valid_to,
        ARRAY_APPEND(anchor.valid_to_list, iter.valid_to)                                                                                     AS valid_to_list,
        ARRAY_APPEND(anchor.valid_from_list, iter.valid_from)                                                                                 AS valid_from_list,
        ARRAY_APPEND(anchor.upstream_organizations, iter.team_id)                                                                             AS upstream_organizations
    FROM recursive_hierarchy AS anchor
    INNER JOIN supervisory_orgs AS iter
        ON iter.team_superior_team_id = anchor.team_id
            AND NOT ARRAY_CONTAINS(iter.team_id::VARIANT, anchor.upstream_organizations)
      
),

final AS (

    SELECT 
        {{ dbt_utils.surrogate_key(['team_id', 'valid_to', 'valid_from']) }}                                                                  AS dim_team_sk,
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
        IFF(recursive_hierarchy.upstream_organizations[0] IS NULL, '--', recursive_hierarchy.upstream_organizations[0])::VARCHAR              AS hierarchy_level_1,
        IFF(recursive_hierarchy.upstream_organizations[1] IS NULL, '--', recursive_hierarchy.upstream_organizations[1])::VARCHAR              AS hierarchy_level_2,
        IFF(recursive_hierarchy.upstream_organizations[2] IS NULL, '--', recursive_hierarchy.upstream_organizations[2])::VARCHAR              AS hierarchy_level_3,
        IFF(recursive_hierarchy.upstream_organizations[3] IS NULL, '--', recursive_hierarchy.upstream_organizations[3])::VARCHAR              AS hierarchy_level_4,
        IFF(recursive_hierarchy.upstream_organizations[4] IS NULL, '--', recursive_hierarchy.upstream_organizations[4])::VARCHAR              AS hierarchy_level_5,
        IFF(recursive_hierarchy.upstream_organizations[5] IS NULL, '--', recursive_hierarchy.upstream_organizations[5])::VARCHAR              AS hierarchy_level_6,
        IFF(recursive_hierarchy.upstream_organizations[6] IS NULL, '--', recursive_hierarchy.upstream_organizations[6])::VARCHAR              AS hierarchy_level_7,
        IFF(recursive_hierarchy.upstream_organizations[7] IS NULL, '--', recursive_hierarchy.upstream_organizations[7])::VARCHAR              AS hierarchy_level_8,
        IFF(recursive_hierarchy.upstream_organizations[8] IS NULL, '--', recursive_hierarchy.upstream_organizations[8])::VARCHAR              AS hierarchy_level_9,
        recursive_hierarchy.upstream_organizations                                                                                            AS hierarchy_levels_array,
        recursive_hierarchy.valid_from_list[ARRAY_SIZE(recursive_hierarchy.valid_from_list) - 1]::TIMESTAMP                                   AS valid_from,
        recursive_hierarchy.valid_to_list[ARRAY_SIZE(recursive_hierarchy.valid_to_list) - 1]::TIMESTAMP                                       AS valid_to,

        IFF(recursive_hierarchy.team_inactivated IS NULL,
          TRUE, FALSE)                                                                                                                        AS is_currently_valid
    FROM recursive_hierarchy

)

SELECT * 
FROM final



