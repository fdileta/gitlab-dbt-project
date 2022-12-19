{{ config({
    "materialized": "incremental",
    "unique_key": "id"
    })
}}

SELECT 
  id::NUMBER,
  tag::VARCHAR,
  description::VARCHAR,
  project_id::INT,
  created_at::TIMESTAMP,
  updated_at::TIMESTAMP,
  author_id::INT,
  sha::VARCHAR,
  _uploaded_at::FLOAT
FROM {{ source('gitlab_dotcom', 'releases') }}
{% if is_incremental() %}

WHERE updated_at >= (SELECT MAX(updated_at) FROM {{this}})

{% endif %}
QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1
