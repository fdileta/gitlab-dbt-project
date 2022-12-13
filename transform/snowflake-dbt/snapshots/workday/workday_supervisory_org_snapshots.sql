{% snapshot workday_supervisory_org_snapshots %}

    {{
        config(
          unique_key='team_id',
          strategy='timestamp',
          updated_at='_fivetran_synced',
        )
    }}
    
    SELECT * 
    FROM {{ source('workday','supervisory_organization') }}
    
{% endsnapshot %}
