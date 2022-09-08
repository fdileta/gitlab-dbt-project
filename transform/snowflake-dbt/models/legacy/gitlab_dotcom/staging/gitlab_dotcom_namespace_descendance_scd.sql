{{ config(
    enabled=false
) }}
WITH source AS (
  SELECT
    *    
  FROM {{ ref('gitlab_dotcom_namespaces_snapshots_base') }} 
),
/*
This CTE finds groups of snapshoted changes that changed the parent id. This is a typical 'gaps and islands' problem. 
*/
  parent_groups AS (
    SELECT
      namespace_id,
      parent_id,
      IFNULL(parent_id, -1)                                                             AS no_null_parent_id,
      LAG(no_null_parent_id, 1, -1)
          OVER (PARTITION BY namespace_id ORDER BY valid_from)                          AS lag_parent_id,
      CONDITIONAL_TRUE_EVENT(no_null_parent_id != lag_parent_id)
                             OVER ( PARTITION BY namespace_id ORDER BY valid_from)      AS parent_id_group,
      valid_from, 
      IFNULL(valid_to, CURRENT_DATE())                                                  AS valid_to     
    FROM source
),
  parent_change AS (
    SELECT
      namespace_id,
      parent_id,
      parent_id_group,
      MIN(valid_from)  AS valid_from,
      MAX(valid_to)    AS valid_to
    FROM parent_groups
    GROUP BY 1,2,3
  ),

  recursive_namespace_lineage(
    root_namespace,
    namespace_id,
    parent_id,
    valid_to,
    valid_from,
    upstream_lineage) AS (

    SELECT
      root.namespace_id AS root_namespace,
      root.namespace_id,
      root.parent_id,
      root.valid_to,
      root.valid_from,
      TO_ARRAY(root.namespace_id) AS upstream_lineage
    FROM parent_change AS root
    WHERE namespace_id = 4347861

    UNION ALL

    SELECT
      anchor.root_namespace,
      iter.namespace_id,
      iter.parent_id,
      iter.valid_to,
      iter.valid_from,
      ARRAY_APPEND(anchor.upstream_lineage, iter.namespace_id) AS upstream_lineage
    FROM recursive_namespace_lineage AS anchor
    INNER JOIN parent_change AS iter
      ON iter.parent_id = anchor.namespace_id
      AND NOT ARRAY_CONTAINS(iter.namespace_id::VARIANT, anchor.upstream_lineage)
      AND (CASE
             WHEN iter.valid_from BETWEEN anchor.valid_from AND anchor.valid_to THEN TRUE
             WHEN iter.valid_to BETWEEN anchor.valid_from AND anchor.valid_to THEN TRUE
             WHEN anchor.valid_from BETWEEN iter.valid_from AND iter.valid_to THEN TRUE
             ELSE FALSE
           END) = TRUE
  ),

    simple_descendants AS (
    SELECT
      root_namespace,
      ARRAY_AGG(namespace_id) WITHIN GROUP ( ORDER BY valid_from ) AS descendants_list,
      ARRAY_AGG(valid_from) WITHIN GROUP ( ORDER BY valid_from ) AS descendants_valid_from_list,
      ARRAY_AGG(valid_to) WITHIN GROUP ( ORDER BY valid_from ) AS descendants_valid_to_list
    FROM recursive_namespace_lineage
    GROUP BY 1
  ),

  namespace_descendants_scd as (
  SELECT
    root_namespace,    
    GET(simple_descendants.descendants_valid_from_list,
        descendants_valid_from_list.index)::TIMESTAMP AS descendants_valid_from, 
    
    ARRAY_AGG(descendants_list.value::INTEGER) WITHIN GROUP ( ORDER BY GET(simple_descendants.descendants_valid_from_list,
                                                                          descendants_list.index)::TIMESTAMP ) AS full_list, 
    ARRAY_SLICE(full_list, 1, ARRAY_SIZE(full_list)) AS descendant_list,
    ARRAY_SIZE(descendant_list) AS descendant_list_size,
    LEAD(descendants_valid_from, 1, CURRENT_DATE())
         OVER (PARTITION BY root_namespace ORDER BY descendants_valid_from) AS descendants_valid_to
  FROM simple_descendants
  INNER JOIN LATERAL FLATTEN(INPUT =>descendants_list) descendants_list
  INNER JOIN LATERAL FLATTEN(INPUT =>descendants_valid_from_list) descendants_valid_from_list
  WHERE TRUE

    AND GET(simple_descendants.descendants_valid_from_list, descendants_list.index)::TIMESTAMP <=
        descendants_valid_from 
    AND GET(simple_descendants.descendants_valid_to_list, descendants_list.index)::TIMESTAMP >=
        descendants_valid_from 
    
  GROUP BY 1, 2, descendants_valid_from_list.index
  ORDER BY 1, 2
),

final AS (
select
  root_namespace,
  descendant_list,
  descendant_list_size,
  descendants_valid_from,
  descendants_valid_to
from namespace_descendants_scd
where descendants_valid_from != descendants_valid_to
)



{{ dbt_audit(
    cte_ref="final",
    created_by="@pempey",
    updated_by="@pempey",
    created_date="2022-09-01",
    updated_date="2022-09-01"
) }}


