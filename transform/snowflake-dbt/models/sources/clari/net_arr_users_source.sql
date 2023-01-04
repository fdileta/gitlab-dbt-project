{{ config({
    "materialized": "incremental",
    "unique_key": "user_id"
    })
}}

WITH
  source AS (
    SELECT
      value,
      uploaded_at
    FROM
      {{ source('clari', 'net_arr') }},
      LATERAL FLATTEN(input => jsontext:data:users)

      {% if is_incremental() %}
        WHERE uploaded_at > (SELECT MAX(uploaded_at) FROM {{this}})
      {% endif %}
  ),
  parsed AS (
    SELECT
      -- primary key
      value:userId::varchar user_id,

      -- logical info
      value:crmId::varchar crm_user_id,
      value:email::varchar email,
      value:parentHierarchyId::varchar parent_role_id,
      value:parentHierarchyName::varchar parent_role,
      value:hierarchyId::varchar sales_team_role_id,
      value:hierarchyName::varchar sales_team_role,
      value:name::varchar user_full_name,
      value:scopeId::variant scope_id,

      uploaded_at
    FROM
      source
    -- remove dups in case of overlapping data from daily/quarter loads
    QUALIFY
      ROW_NUMBER() over (
        PARTITION BY
          user_id
        ORDER BY
          uploaded_at desc
      ) = 1
  )
SELECT
  *
FROM
  parsed
