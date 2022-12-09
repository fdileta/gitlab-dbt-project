{% snapshot zuora_revenue_revenue_contract_schedule_snapshots %}

     {{
        config(
          strategy='timestamp',
          unique_key='revenue_snapshot_id',
          updated_at='incr_updt_dt',
        )
    }}

    SELECT
       schd_id || '-' || acctg_type AS revenue_snapshot_id,
        *
    FROM {{ source('zuora_revenue','zuora_revenue_revenue_contract_schedule') }}
    QUALIFY RANK() OVER (PARTITION BY schd_id, acctg_type ORDER BY incr_updt_dt DESC) = 1

{% endsnapshot %}
