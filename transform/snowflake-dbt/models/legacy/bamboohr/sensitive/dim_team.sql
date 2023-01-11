WITH source AS (

    SELECT *
    FROM {{ ref('workday_supervisory_organizations_source') }}

), team_data AS (

    SELECT  
        team_id,
        team_superior_team_id,
        team_hierarchy_level,
        team_members_count,
        team_manager_inherited,
        team_inactivated,
        team_name, 
        team_manager_name, 
        team_manager_name_id,
        team_inactivated_date,
        source.valid_from,
        IFNULL(source.valid_to, CURRENT_DATE()) AS valid_to
    FROM source
    {{ dbt_utils.group_by(n=12)}}

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
        root.team_superior_team_id                                                                                                            AS team_superior_team_id,
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
        iter.team_superior_team_id                                                                                                            AS team_superior_team_id,
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
        recursive_hierarchy.team_superior_team_id                                                                                             AS team_superior_team_id,
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
        recursive_hierarchy.valid_from,   
        recursive_hierarchy.valid_to
    FROM recursive_hierarchy
    {{ dbt_utils.group_by(n=14)}}
    
), final AS (

SELECT 
    team_hierarchy.team_id, 
    team_hierarchy.team_superior_team_id,
    team_hierarchy.hierarchy_level_1,
    team_hierarchy.hierarchy_level_2,
    team_hierarchy.hierarchy_level_3,
    team_hierarchy.hierarchy_level_4,
    team_hierarchy.hierarchy_level_5,
    team_hierarchy.hierarchy_level_6,
    team_hierarchy.hierarchy_level_7,
    team_hierarchy.hierarchy_level_8,
    team_hierarchy.hierarchy_level_9,
    team_hierarchy.hierarchy_levels_array,
    ARRAY_SIZE(team_hierarchy.hierarchy_levels_array) AS team_hierarchy_level,
    team_data.team_manager_inherited,
    team_data.team_inactivated,
    team_data.team_name, 
    team_data.team_manager_name, 
    team_data.team_manager_name_id,
    team_data.team_inactivated_date,
    team_hierarchy.valid_from,
    team_hierarchy.valid_to
FROM team_hierarchy
LEFT JOIN team_data
ON team_hierarchy.team_superior_team_id = team_data.team_superior_team_id AND team_hierarchy.team_id = team_data.team_id
{{ dbt_utils.group_by(n=21)}}

)

SELECT *
FROM FINAL