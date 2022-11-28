{% snapshot sfdc_bizible_attribution_touchpoint_snapshots %}

    {{
        config(
          unique_key='id',
          strategy='timestamp',
          updated_at='systemmodstamp',
        )
    }}
    
    SELECT * 
    FROM {{ source('salesforce', 'bizible_attribution_touchpoint') }}
    
{% endsnapshot %}
