
WITH source AS (

    SELECT *
    FROM {{ ref('workday_supervisory_organizations_source') }}

), team_superior_groups AS (

    SELECT
      team_id,
      team_superior_team_id,
      IFNULL(team_superior_team_id, '-1')                                          AS no_null_superior,
      LAG(no_null_superior, 1, '-1')
          OVER (PARTITION BY team_id ORDER BY valid_from)                          AS lag_superior_id,
      CONDITIONAL_TRUE_EVENT(no_null_superior != lag_superior_id)
                             OVER ( PARTITION BY team_id ORDER BY valid_from)      AS superior_id_group,
      valid_from, 
      IFNULL(valid_to, CURRENT_DATE())                                             AS valid_to     
    FROM source

), team_superior_change AS (

    SELECT
      team_id,
      team_superior_team_id,
      superior_id_group,
      MIN(valid_from)  AS valid_from,
      MAX(valid_to)    AS valid_to
    FROM team_superior_groups
    GROUP BY 1,2,3

), recursive_hierarchy AS (

    SELECT
        root.team_id                                                                                                                          AS team_id, 
        root.team_hierarchy_level                                                                                                             AS team_hierarchy_level,
        root.team_members_count                                                                                                               AS team_members_count,
        root.team_manager_inherited                                                                                                           AS team_manager_inherited,
        root.team_inactivated                                                                                                                 AS team_inactivated,
        root.team_name                                                                                                                        AS team_name,
        root.team_manager_name                                                                                                                AS team_manager_name,
        root.team_manager_name_id                                                                                                             AS team_manager_name_id,
        root.team_superior_team_id                                                                                                            AS team_superior_team_id,
        root.team_inactivated_date                                                                                                            AS team_inactivated_date,
        root.valid_from                                                                                                                       AS valid_from,
        root.valid_to                                                                                                                         AS valid_to,
        TO_ARRAY(root.valid_to)                                                                                                               AS valid_to_list,
        TO_ARRAY(root.valid_from)                                                                                                             AS valid_from_list,
        TO_ARRAY(root.team_id)                                                                                                                AS upstream_organizations
    FROM team_superior_change AS root
    WHERE team_superior_team_id IS NULL

    UNION ALL 

    SELECT
        iter.team_id                                                                                                                          AS team_id, 
        iter.team_hierarchy_level                                                                                                             AS team_hierarchy_level,
        iter.team_members_count                                                                                                               AS team_members_count,
        iter.team_manager_inherited                                                                                                           AS team_manager_inherited,
        iter.team_inactivated                                                                                                                 AS team_inactivated,
        iter.team_name                                                                                                                        AS team_name,
        iter.team_manager_name                                                                                                                AS team_manager_name,
        iter.team_manager_name_id                                                                                                             AS team_manager_name_id,
        iter.team_superior_team_id                                                                                                            AS team_superior_team_id,
        iter.team_inactivated_date                                                                                                            AS team_inactivated_date,
        iter.valid_from                                                                                                                       AS valid_from,
        iter.valid_to                                                                                                                         AS valid_to,
        ARRAY_APPEND(anchor.valid_from_list, iter.valid_from)                                                                                 AS valid_from_list,
        ARRAY_APPEND(anchor.valid_to_list, iter.valid_to)                                                                                     AS valid_to_list,
        ARRAY_APPEND(anchor.upstream_organizations, iter.team_id)                                                                             AS upstream_organizations
    FROM recursive_hierarchy AS anchor
    INNER JOIN team_superior_change AS iter
        ON iter.team_superior_team_id = anchor.team_id
            AND NOT ARRAY_CONTAINS(iter.team_id::VARIANT, anchor.upstream_organizations)
            AND (
                CASE
                    WHEN iter.valid_from BETWEEN anchor.valid_from AND anchor.valid_to THEN TRUE
                    WHEN iter.valid_to BETWEEN anchor.valid_from AND anchor.valid_to THEN TRUE
                    WHEN anchor.valid_from BETWEEN iter.valid_from AND iter.valid_to THEN TRUE
                    ELSE FALSE
                END) = TRUE
            
), team_hierarchy AS (

    SELECT 
        recursive_hierarchy.team_id                                                                                                           AS team_id, 
        recursive_hierarchy.team_hierarchy_level                                                                                              AS team_hierarchy_level,
        recursive_hierarchy.team_members_count                                                                                                AS team_members_count,
        recursive_hierarchy.team_manager_inherited                                                                                            AS team_manager_inherited,
        recursive_hierarchy.team_inactivated                                                                                                  AS team_inactivated,
        recursive_hierarchy.team_name                                                                                                         AS team_name, 
        recursive_hierarchy.team_manager_name                                                                                                 AS team_manager_name, 
        recursive_hierarchy.team_manager_name_id                                                                                              AS team_manager_name_id,
        recursive_hierarchy.team_superior_team_id                                                                                             AS team_superior_team_id,
        recursive_hierarchy.team_inactivated_date                                                                                             AS team_inactivated_date,
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
        recursive_hierarchy.valid_from_list                                                                                                   AS valid_from_list, 
        recursive_hierarchy.valid_to_list                                                                                                     AS valid_to_list,
        MAX(from_list.value::TIMESTAMP)                                                                                                       AS hierarchy_valid_from,
        MIN(to_list.value::TIMESTAMP)                                                                                                         AS hierarchy_valid_to
    FROM recursive_hierarchy
    INNER JOIN LATERAL FLATTEN(INPUT =>valid_from_list) from_list
    INNER JOIN LATERAL FLATTEN(INPUT =>valid_to_list) to_list 
    {{ dbt_utils.group_by(n=23)}}
    HAVING hierarchy_valid_to > hierarchy_valid_from
    
), final AS (

    SELECT 
        {{ dbt_utils.surrogate_key(['team_id', 'hierarchy_valid_from']) }} AS dim_team_sk,
        team_id, 
        team_hierarchy_level,
        team_members_count,
        team_manager_inherited,
        team_inactivated,
        team_name, 
        team_manager_name, 
        team_manager_name_id,
        team_superior_team_id,
        team_inactivated_date,
        hierarchy_level_1,
        hierarchy_level_2,
        hierarchy_level_3,
        hierarchy_level_4,
        hierarchy_level_5,
        hierarchy_level_6,
        hierarchy_level_7,
        hierarchy_level_8,
        hierarchy_level_9,
        hierarchy_levels_array,
        hierarchy_valid_from,
        hierarchy_valid_to
    FROM team_hierarchy
)

SELECT * 
FROM final
