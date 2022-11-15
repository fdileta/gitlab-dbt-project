{{ config({
    "materialized": "table",
    "unique_key": "id"
    })
}}

SELECT id::VARCHAR                           AS id                          -- integer NOT NULL,
      ,target_branch::VARCHAR                AS target_branch               -- character varying NOT NULL,
      ,source_branch::VARCHAR                AS source_branch               -- character varying NOT NULL,
      ,source_project_id::VARCHAR            AS source_project_id           -- integer,
      ,author_id::VARCHAR                    AS author_id                   -- integer,
      ,assignee_id::VARCHAR                  AS assignee_id                 -- integer,
      ,title::VARCHAR                        AS title                       -- character varying,
      ,created_at                            AS created_at                  -- timestamp without time zone,
      ,updated_at                            AS updated_at                  -- timestamp without time zone,
      ,milestone_id::VARCHAR                 AS milestone_id                -- integer,
      ,merge_status::VARCHAR                 AS merge_status                -- character varying DEFAULT 'unchecked'::character varying NOT NULL,
      ,target_project_id::VARCHAR            AS target_project_id           -- integer NOT NULL,
      ,iid::VARCHAR                          AS iid                         -- integer,
      ,description::VARCHAR                  AS description                 -- text,
      ,updated_by_id::VARCHAR                AS updated_by_id               -- integer,
      ,merge_error::VARCHAR                  AS merge_error                 -- text,
      ,merge_params::VARCHAR                 AS merge_params                -- text,
FROM {{ source('gitlab_dotcom', 'merge_requests') }}
{% if is_incremental() %}

WHERE updated_at >= (SELECT MAX(updated_at) FROM {{this}})

{% endif %}
QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1
