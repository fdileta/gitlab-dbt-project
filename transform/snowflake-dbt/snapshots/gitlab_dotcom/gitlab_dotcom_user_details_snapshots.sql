{% snapshot gitlab_dotcom_user_details_snapshots %}

    {{
        config(
          unique_key='user_id',
          strategy='timestamp',
          updated_at='_uploaded_at',
          invalidate_hard_deletes=True
        )
    }}

    SELECT *
    FROM {{ source('gitlab_dotcom', 'user_details') }}
    QUALIFY (ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY _uploaded_at DESC) = 1)

{% endsnapshot %}
