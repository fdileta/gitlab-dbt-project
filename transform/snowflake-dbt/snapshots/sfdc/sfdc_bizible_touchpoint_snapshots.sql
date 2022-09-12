{% snapshot sfdc_bizible_touchpoint_snapshots %}

    {{
        config(
          unique_key='id',
          strategy='timestamp',
          updated_at='systemmodstamp',
        )
    }}
    
    SELECT * 
    FROM {{ source('salesforce', 'bizible_touchpoint') }}
    
{% endsnapshot %}
