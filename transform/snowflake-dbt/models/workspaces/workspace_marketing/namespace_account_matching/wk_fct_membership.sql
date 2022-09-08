{{ config(
    materialized='table',
    snowflake_warehouse = generate_warehouse_name('XL'),
    enabled=false
) }}

{{ simple_cte([
    ('direct_membership','wk_fct_direct_membership'),
    ('group_share','wk_fct_group_share'),
    ('lineage','gitlab_dotcom_namespace_lineage_scd'),
    ('access_levels','gitlab_dotcom_access_levels_source')
]) }},

direct_membership_test AS (
  SELECT *
  FROM direct_membership
  WHERE user_id IS NOT NULL
    AND user_id in (9475165)
)

shared_groups AS (

  SELECT DISTINCT 
    received_namespace_id
  FROM group_share

),

all_membership AS (

  SELECT 
    share_id::INT AS share_id,
    shared_namespace_id,
    received_namespace_id AS shared_with_id,
    LOWER(share_source) AS shared_with_type,
    access_level,
    created_at,
    valid_from,
    COALESCE(valid_to,CURRENT_DATE()) AS valid_to
  FROM group_share

  UNION 

  SELECT 
    member_id::INT AS share_id,
    namespace_id AS shared_namespace_id,
    user_id AS shared_with_id,
    'user' AS shared_with_type,
    access_level,
    membership_created_at AS created_at,
    valid_from,
    COALESCE(valid_to,CURRENT_DATE()) AS valid_to
  FROM direct_membership_test

),

descendants AS (
  
  SELECT DISTINCT
    descendants.value::INT AS related_namespace,
    lineage.namespace_id AS namespace_id,
    ARRAY_POSITION(descendants.value, lineage.upstream_lineage) AS lineage_position,
    ARRAY_SLICE(lineage.upstream_lineage, lineage_position, lineage.lineage_depth) AS full_lineage,
    ARRAY_SLICE(full_lineage, 1, ARRAY_SIZE(full_lineage)) AS descendant_lineage,
    ARRAY_PREPEND(
      STRTOK_TO_ARRAY(
        REGEXP_REPLACE(
          ARRAY_TO_STRING(descendant_lineage, ',')
          , '[0-9]+', 'child')
        , ',')
      , 'direct') AS relationship_lineage,
    lineage_valid_from AS valid_from,
    lineage_valid_to AS valid_to
  FROM lineage
  INNER JOIN LATERAL FLATTEN(INPUT => lineage.upstream_lineage) descendants

),

relationships AS ( -- SCD
  
  SELECT
    namespace_id,
    related_namespace,
    full_lineage AS lineage,
    relationship_lineage,
    valid_from,
    valid_to
  FROM descendants

),


inherited_membership AS (
 SELECT
    all_membership.share_id,
    all_membership.shared_namespace_id,
    relationships.namespace_id AS related_namespace_id,
    all_membership.shared_with_id,
    all_membership.shared_with_type,
    all_membership.access_level,
    all_membership.created_at,
    relationships.lineage,
    --relationships.relationship_lineage,
    CASE
      WHEN all_membership.shared_with_type != 'user' THEN
        ARRAY_PREPEND(ARRAY_SLICE(relationships.relationship_lineage,1,ARRAY_SIZE(relationships.relationship_lineage)),all_membership.shared_with_type)
      ELSE relationships.relationship_lineage
    END AS relationship_lineage,
    GREATEST(relationships.valid_from,all_membership.valid_from) AS valid_from,
    LEAST(relationships.valid_to,all_membership.valid_to) AS valid_to
  FROM all_membership
  INNER JOIN relationships
    ON all_membership.shared_namespace_id = relationships.related_namespace
    AND (CASE
             WHEN relationships.valid_from BETWEEN all_membership.valid_from AND all_membership.valid_to THEN TRUE
             WHEN relationships.valid_to BETWEEN all_membership.valid_from AND all_membership.valid_to THEN TRUE
             WHEN all_membership.valid_from BETWEEN relationships.valid_from AND relationships.valid_to THEN TRUE
             ELSE FALSE
           END) = TRUE
),

shared_inheritance AS (
  SELECT
    inherited_membership.share_id,
    inherited_membership.shared_namespace_id,
    inherited_membership.related_namespace_id,
    share.shared_with_id,
    inherited_membership.shared_with_type,
    LEAST(inherited_membership.access_level, share.access_level) AS access_level,
    GREATEST(inherited_membership.created_at, share.created_at) AS created_at,
    ARRAY_CAT(share.lineage, inherited_membership.lineage) AS lineage,
    ARRAY_CAT(share.relationship_lineage, inherited_membership.relationship_lineage) AS relationship_lineage,
    GREATEST(inherited_membership.created_at, inherited_membership.valid_from, share.valid_from) AS valid_from,
    LEAST(inherited_membership.valid_to, share.valid_to) AS valid_to
  FROM inherited_membership
  INNER JOIN shared_groups
    ON inherited_membership.shared_with_id = shared_groups.received_namespace_id
  INNER JOIN LATERAL ( SELECT * FROM inherited_membership _s WHERE inherited_membership.shared_with_id = _s.related_namespace_id AND _s.shared_with_type = 'user' ) AS share
  --INNER JOIN inherited_membership share
    --ON inherited_membership.shared_with_id = share.related_namespace_id
    --AND share.shared_with_type = 'user'
  WHERE TRUE
  AND inherited_membership.shared_with_type != 'user'    
  AND (CASE
          WHEN share.valid_from BETWEEN inherited_membership.valid_from AND inherited_membership.valid_to THEN TRUE
          WHEN share.valid_to BETWEEN inherited_membership.valid_from AND inherited_membership.valid_to THEN TRUE
          WHEN inherited_membership.valid_from BETWEEN share.valid_from AND share.valid_to THEN TRUE
          ELSE FALSE
        END) = TRUE

),

all_inheritance AS (

  SELECT *
  FROM inherited_membership
  WHERE inherited_membership.shared_with_type = 'user'
  
  UNION ALL
  
  SELECT *
  FROM shared_inheritance
),

fct AS (

  SELECT
    -- Primary Key
    {{ dbt_utils.surrogate_key(['all_inheritance.share_id']) }} AS member_pk,

    -- Foreign Keys
    {{ get_keyed_nulls(dbt_utils.surrogate_key(['all_inheritance.shared_with_id'])) }} AS dim_user_sk,
    {{ get_keyed_nulls(dbt_utils.surrogate_key(['all_inheritance.related_namespace_id'])) }} AS dim_namespace_sk,

    -- Legacy Keys
    all_inheritance.share_id,
    all_inheritance.shared_with_id AS user_id,
    all_inheritance.related_namespace_id AS namespace_id,

    -- Degenerate Dimensions
    all_inheritance.shared_namespace_id,
    all_inheritance.access_level,
    access_levels.access_level_name,
    CASE GET(all_inheritance.relationship_lineage,ARRAY_POSITION(all_inheritance.related_namespace_id, all_inheritance.lineage))
      WHEN 'direct' THEN 'Direct'
      WHEN 'child' THEN 'Inherited'
      WHEN 'group' THEN 'Shared Group'
      WHEN 'project' THEN 'Shared Project'
    END AS membership_type,
    all_inheritance.shared_with_type,
    all_inheritance.created_at AS membership_created_at, -- change to created_at
    all_inheritance.lineage,
    all_inheritance.relationship_lineage,
    all_inheritance.valid_from,
    all_inheritance.valid_to,
    IFF(valid_to = CURRENT_DATE(), TRUE, FALSE) AS is_current

    -- Facts

  FROM all_inheritance
  LEFT JOIN access_levels
    ON all_inheritance.access_level = access_levels.access_level_id
)

SELECT *
FROM fct
