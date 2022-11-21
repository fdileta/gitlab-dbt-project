{{ config({
    "materialized": "table",
    "unique_key": "id"
    })
}}

SELECT id::VARCHAR                           AS id                           -- integer NOT NULL,
      ,target_branch::VARCHAR                AS target_branch                -- character varying NOT NULL,
      ,source_branch::VARCHAR                AS source_branch                -- character varying NOT NULL,
      ,source_project_id::VARCHAR            AS source_project_id            -- integer,
      ,author_id::VARCHAR                    AS author_id                    -- integer,
      ,assignee_id::VARCHAR                  AS assignee_id                  -- integer,
      ,title::VARCHAR                        AS title                        -- character varying,
      ,created_at::TIMESTAMP                 AS created_at                   -- timestamp without time zone,
      ,updated_at::TIMESTAMP                 AS updated_at                   -- timestamp without time zone,
      ,milestone_id::VARCHAR                 AS milestone_id                 -- integer,
      ,merge_status::VARCHAR                 AS merge_status                 -- character varying DEFAULT 'unchecked'::character varying NOT NULL,
      ,target_project_id::VARCHAR            AS target_project_id            -- integer NOT NULL,
      ,iid::VARCHAR                          AS iid                          -- integer,
      ,description::VARCHAR                  AS description                  -- text,
      ,updated_by_id::VARCHAR                AS updated_by_id                -- integer,
      ,merge_error::VARCHAR                  AS merge_error                  -- text,
      ,merge_params::VARCHAR                 AS merge_params                 -- text,
      ,merge_when_pipeline_succeeds::BOOLEAN AS merge_when_pipeline_succeeds -- boolean DEFAULT false NOT NULL,
      ,merge_user_id::VARCHAR                AS merge_user_id                -- integer,
--       ,merge_commit_sha::VARCHAR             AS merge_commit_sha             -- character varying,
      ,approvals_before_merge::NUMBER        AS approvals_before_merge       -- integer,
--       ,rebase_commit_sha::VARCHAR            AS rebase_commit_sha            -- character varying,
--       ,in_progress_merge_commit_sha::VARCHAR AS in_progress_merge_commit_sha -- character varying,
      ,lock_version::NUMBER                  AS lock_version                 -- integer DEFAULT 0,
--       ,title_html::VARCHAR                   AS title_html                   -- text,
      --,description_html::VARCHAR             AS description_html             -- text,
      ,time_estimate::NUMBER                 AS time_estimate                -- integer,
      ,squash::BOOLEAN                       AS squash                       -- boolean DEFAULT false NOT NULL,
      --,cached_markdown_version::NUMBER       AS cached_markdown_version      -- integer,
      ,last_edited_at::TIMESTAMP             AS last_edited_at               -- timestamp without time zone,
      ,last_edited_by_id::VARCHAR            AS last_edited_by_id            -- integer,
      ,head_pipeline_id::NUMBER              AS head_pipeline_id             -- integer,
      --,merge_jid::VARCHAR                    AS merge_jid                    -- character varying,
      ,discussion_locked::BOOLEAN            AS discussion_locked            -- boolean,
      ,latest_merge_request_diff_id::VARCHAR AS latest_merge_request_diff_id -- integer,
      ,allow_maintainer_to_push::BOOLEAN     AS allow_maintainer_to_push     -- boolean DEFAULT true,
      ,state_id::NUMBER                      AS state_id                     -- smallint DEFAULT 1 NOT NULL,
      --,rebase_jid::VARCHAR                   AS rebase_jid                   -- character varying,
      --,squash_commit_sha::VARCHAR            AS squash_commit_sha            -- bytea,
      --,sprint_id::VARCHAR                    AS sprint_id                    -- bigint,
      --,merge_ref_sha::VARCHAR                AS merge_ref_sha                -- bytea,
      --,draft::BOOLEAN                        AS draft                        -- boolean DEFAULT false NOT NULL,
FROM {{ source('gitlab_dotcom', 'merge_requests') }}
{% if is_incremental() %}

WHERE updated_at >= (SELECT MAX(updated_at) FROM {{this}})

{% endif %}
QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1
