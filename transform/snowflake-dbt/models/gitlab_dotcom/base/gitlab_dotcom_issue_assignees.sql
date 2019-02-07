WITH source AS (

	SELECT DISTINCT user_id, issue_id
	FROM raw.gitlab_dotcom.issue_assignees

), renamed AS (

    SELECT
      md5(user_id || issue_id) as user_issue_relation_id,
      user_id :: integer       as user_id,
      issue_id :: integer      as issue_id

    FROM source


)

SELECT *
FROM renamed