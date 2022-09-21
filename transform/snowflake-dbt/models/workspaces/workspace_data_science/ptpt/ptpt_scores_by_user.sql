{{ config(
     materialized = "table"
)}}

{{ simple_cte([
    ('ptpt_scores', 'ptpt_scores'),
    ('dim_namespace', 'dim_namespace'),
    ('gitlab_dotcom_users_source', 'gitlab_dotcom_users_source')
    ])
}}

, score_dates AS (
    
    SELECT DISTINCT score_date
    FROM ptpt_scores
  
), last_dates AS (
  
    SELECT
      FIRST_VALUE(score_date) OVER(ORDER BY score_date DESC)  AS last_score_date,
      NTH_VALUE(score_date, 2) OVER(ORDER BY score_date DESC) AS after_last_score_date
    FROM score_dates
    LIMIT 1

), ptpt_scores AS (

    SELECT *
    FROM ptpt_scores
    WHERE score_date IN (SELECT last_score_date FROM last_dates)
  
), ptpt_scores_last_2 AS (
  
    SELECT *
    FROM ptpt_scores
    WHERE score_date IN (SELECT after_last_score_date FROM last_dates)

), namespace_creator_ptpt_score AS (

    SELECT
      COALESCE(users.notification_email, users.email) AS email_address,
      {{ hash_of_column('email_address') }}
      ptpt_scores.namespace_id,
      ptpt_scores.score,
      ptpt_scores.score_group,
      ptpt_scores.insights,
      ptpt_scores.score_date::DATE                    AS score_date
    FROM dim_namespace
    INNER JOIN gitlab_dotcom_users_source users
      ON dim_namespace.creator_id = users.user_id
    INNER JOIN ptpt_scores
      ON dim_namespace.dim_namespace_id = ptpt_scores.namespace_id
    QUALIFY ROW_NUMBER() OVER(PARTITION BY email_address ORDER BY score DESC) = 1

), namespace_creator_ptpt_score_last_2 AS (

    SELECT
      COALESCE(users.notification_email, users.email) AS email_address,
      {{ hash_of_column('email_address') }}
      ptpt_scores_last_2.score_group,
      ptpt_scores_last_2.score_date
    FROM dim_namespace
    INNER JOIN gitlab_dotcom_users_source users
      ON dim_namespace.creator_id = users.user_id
    INNER JOIN ptpt_scores_last_2
      ON dim_namespace.dim_namespace_id = ptpt_scores_last_2.namespace_id
    QUALIFY ROW_NUMBER() OVER(PARTITION BY email_address ORDER BY score DESC) = 1

)

SELECT
  namespace_creator_ptpt_score.email_address_hash,
  namespace_creator_ptpt_score.namespace_id,
  namespace_creator_ptpt_score.score,
  namespace_creator_ptpt_score.score_group,
  namespace_creator_ptpt_score.insights,
  namespace_creator_ptpt_score.score_date,
  namespace_creator_ptpt_score_last_2.score_group       AS past_score_group,
  namespace_creator_ptpt_score_last_2.score_date::DATE  AS past_score_date
FROM namespace_creator_ptpt_score
LEFT JOIN namespace_creator_ptpt_score_last_2
  ON namespace_creator_ptpt_score.email_address = namespace_creator_ptpt_score_last_2.email_address
QUALIFY ROW_NUMBER() OVER(PARTITION BY namespace_creator_ptpt_score.email_address_hash ORDER BY namespace_creator_ptpt_score.score DESC) = 1 -- Covers the case where the hash is not unique
-- due to the fact that some email addresses have different cases and the hash produced is the same between emails.
-- in case this happens, just only take the record with the highest score
