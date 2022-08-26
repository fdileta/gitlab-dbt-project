{% snapshot bizible_touchpoint_snapshots %}

    {{
        config(
          unique_key='id',
          strategy='timestamp',
          updated_at='systemmodstamp',
        )
    }}
    
    SELECT * 
    FROM {{ source('bizible', 'biz_touchpoints') }}
    
{% endsnapshot %}
