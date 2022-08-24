{{ config({
    "database": env_var('SNOWFLAKE_PROD_DATABASE'),
    "schema": "legacy"
    })
}}
WITH RECURSIVE namespaces AS (

    SELECT *
    FROM {{ref('gitlab_dotcom_namespaces')}}

), gitlab_subscriptions AS (

    SELECT *
    FROM {{ref('gitlab_dotcom_gitlab_subscriptions')}}
    WHERE is_currently_valid = TRUE

), plans AS (

    SELECT *
    FROM {{ref('gitlab_dotcom_plans')}}

), recursive_namespaces(namespace_id, parent_id, upstream_lineage) AS (

  -- Select all namespaces without parents
  SELECT
    namespace_id,
    namespaces.parent_id,
    TO_ARRAY(namespace_id)                                      AS upstream_lineage -- Initiate lineage array
  FROM namespaces
  WHERE namespaces.parent_id IS NULL

  UNION ALL

  -- Recursively iterate through each of the children namespaces
  SELECT
    iter.namespace_id,
    iter.parent_id,
    ARRAY_INSERT(anchor.upstream_lineage, 0, iter.namespace_id)  AS upstream_lineage -- Copy the lineage array of parent, inserting self at start
  FROM recursive_namespaces AS anchor -- Parent namespace
    INNER JOIN namespaces AS iter -- Child namespace
      ON anchor.namespace_id = iter.parent_id

), extracted AS (

  SELECT
    *,
    GET(upstream_lineage, ARRAY_SIZE(upstream_lineage)-1) AS ultimate_parent_id -- Last item is the ultimate parent.
  FROM recursive_namespaces

  UNION ALL
  /* Union all children with deleted ancestors. These are missed by the top-down recursive CTE.
     This is quite rare (n=82 on 2020-01-06) but need to be included in this model for full coverage. */
  SELECT
    namespaces.namespace_id, 
    namespaces.parent_id,
    ARRAY_CONSTRUCT() AS upstream_lineage, -- Empty Array.
    0                 AS ultimate_parent_id
  FROM namespaces
  WHERE parent_id NOT IN (SELECT DISTINCT namespace_id FROM namespaces)
    OR namespace_id IN (
                          26631729,
                          30257602,
                          30257599,
                          35511259,
                          26631727,
                          35504596,
                          26631725,
                          35504660,
                          35511305,
                          35510918,
                          35504841,
                          35504640,
                          30257600,
                          35725380,
                          35510952,
                          35511301,
                          26631730,
                          35504714,
                          35504621,
                          35511308,
                          35504835,
                          35504873,
                          35504813,
                          35510919,
                          35504818,
                          25068282,
                          35511277,
                          35937608,
                          35504834,
                          30257601,
                          31405078,
                          35504721,
                          35504652,
                          35511297,
                          35504636,
                          30257598,
                          35504775,
                          35504795,
                          35504734,
                          11967197,
                          11967195,
                          11967194,
                          11967196,
                          12014338,
                          12014366,
                          6713278, 
                          6142621, 
                          4159925, 
                          8370670, 
                          8370671,
                          8437164,
                          8437147,
                          8437148,
                          8437172,
                          8437156,
                          8437159,
                          8437146,
                          8437176,
                          8437165,
                          8437179,
                          8427708,
                          8437167,
                          8437110,
                          8437178,
                          8437175,
                          8427717,
                          8437153,
                          8437161,
                          8437169,
                          8437177,
                          8437160,
                          8437157,
                          8437154,
                          8437162,
                          8437150,
                          8437149,
                          8427716,
                          8437142,
                          8437145,
                          8437151,
                          8437171,
                          8437155,
                          8437173,
                          8437170
                        ) -- Grandparent or older is deleted.

), with_plans AS (

  SELECT
    extracted.*,
    COALESCE((ultimate_parent_id IN {{ get_internal_parent_namespaces() }}), FALSE)   AS namespace_is_internal,
    namespace_plans.plan_id                                                           AS namespace_plan_id,
    namespace_plans.plan_title                                                        AS namespace_plan_title,
    namespace_plans.plan_is_paid                                                      AS namespace_plan_is_paid,
    COALESCE(ultimate_parent_plans.plan_id, 34)                                       AS ultimate_parent_plan_id,
    CASE
    WHEN ultimate_parent_gitlab_subscriptions.is_trial AND COALESCE(ultimate_parent_gitlab_subscriptions.plan_id, 34) <> 34
      THEN 'Trial: Ultimate'
      ELSE COALESCE(ultimate_parent_plans.plan_title, 'Free')
    END                                                                               AS ultimate_parent_plan_title,
    CASE
    WHEN ultimate_parent_gitlab_subscriptions.is_trial
      THEN FALSE
      ELSE COALESCE(ultimate_parent_plans.plan_is_paid, FALSE) 
    END                                                                               AS ultimate_parent_plan_is_paid
  FROM extracted
    -- Get plan information for the namespace.
    LEFT JOIN gitlab_subscriptions AS namespace_gitlab_subscriptions
      ON extracted.namespace_id = namespace_gitlab_subscriptions.namespace_id
    LEFT JOIN plans AS namespace_plans
      ON COALESCE(namespace_gitlab_subscriptions.plan_id, 34) = namespace_plans.plan_id
    -- Get plan information for the ultimate parent namespace.
    LEFT JOIN gitlab_subscriptions AS ultimate_parent_gitlab_subscriptions
      ON extracted.ultimate_parent_id = ultimate_parent_gitlab_subscriptions.namespace_id
    LEFT JOIN plans AS ultimate_parent_plans
      ON COALESCE(ultimate_parent_gitlab_subscriptions.plan_id, 34) = ultimate_parent_plans.plan_id

)

SELECT *
FROM with_plans
