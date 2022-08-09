{%- macro usage_ping_over_ping_difference(all_time_event_metric_column, partition_by_columns = ['dim_subscription_id' ,'uuid', 'hostname'], order_by_column = 'snapshot_month') -%} 

    {%- set ping_over_ping_alias =  all_time_event_metric_column ~ '_since_last_ping' -%}

    {{ all_time_event_metric_column }},
    {{ all_time_event_metric_column }} - LAG({{ all_time_event_metric_column }})
      IGNORE NULLS OVER (
        PARTITION BY
          {%- for column in partition_by_columns %}
          {{ column }}
          {%- if not loop.last -%},{% endif %}
          {%- endfor %}
        ORDER BY {{ order_by_column }}
      )                                                                         AS {{ ping_over_ping_alias }}

{%- endmacro -%}