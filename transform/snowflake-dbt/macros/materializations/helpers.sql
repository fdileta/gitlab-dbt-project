{% macro check_for_period_filter(model_unique_id, sql) %}
    {{ return(adapter.dispatch('check_for_period_filter')(model_unique_id, sql)) }}
{% endmacro %}

{% macro default__check_for_period_filter(model_unique_id, sql) %}
    {%- if sql.find('__PERIOD_FILTER__') == -1 -%}
        {%- set error_message -%}
        Model '{{ model_unique_id }}' does not include the required string '__PERIOD_FILTER__' in its sql
        {%- endset -%}
        {{ exceptions.raise_compiler_error(error_message) }}
    {%- endif -%}
{% endmacro %}

{% macro get_period_boundaries(target_database, target_schema, target_table, timestamp_field, start_date, stop_date, period) -%}
    {{ return(adapter.dispatch('get_period_boundaries')(target_database, target_schema, target_table, timestamp_field, start_date, stop_date, period)) }}
{% endmacro %}

{% macro default__get_period_boundaries(target_database, target_schema, target_table, timestamp_field, start_date, stop_date, period) -%}

  {% call statement('period_boundaries', fetch_result=True) -%}
    {%- set model_name = model['name'] -%}
    
    with data as (
      select
          coalesce(max("{{timestamp_field}}"), '{{start_date}}')::timestamp as start_timestamp,
          coalesce(
            {{dbt_utils.dateadd('millisecond',
                                -1,
                                "nullif('" ~ stop_date ~ "','')::timestamp")}},
            {{dbt_utils.current_timestamp()}}
          ) as stop_timestamp
      from "{{target_database}}"."{{target_schema}}"."{{model_name}}"
    )

    select
      start_timestamp,
      stop_timestamp,
      {{dbt_utils.datediff('start_timestamp',
                           'stop_timestamp',
                           period)}}  + 1 as num_periods
    from data
  {%- endcall %}

{%- endmacro %}

{% macro snowflake__get_period_boundaries(target_database, target_schema, target_table, timestamp_field, start_date, stop_date, period) -%}

  {% call statement('period_boundaries', fetch_result=True) -%}
    {%- set model_name = model['name'] -%}
    
    with data as (
      select
          coalesce(max({{timestamp_field}}), '{{start_date}}')::timestamp as start_timestamp,
          coalesce(
            {{dbt_utils.dateadd('millisecond',
                                -1,
                                "nullif('" ~ stop_date ~ "','')::timestamp")}},
            {{dbt_utils.current_timestamp()}}
          ) as stop_timestamp
      from {{target_database}}.{{target_schema}}.{{model_name}}
    )

    select
      start_timestamp,
      stop_timestamp,
      {{dbt_utils.datediff('start_timestamp',
                           'stop_timestamp',
                           period)}}  + 1 as num_periods
    from data
  {%- endcall %}

{%- endmacro %}


{% macro get_period_sql(target_cols_csv, sql, timestamp_field, period, start_timestamp, stop_timestamp, offset) -%}
    {{ return(adapter.dispatch('get_period_sql')(target_cols_csv, sql, timestamp_field, period, start_timestamp, stop_timestamp, offset)) }}
{% endmacro %}

{% macro default__get_period_sql(target_cols_csv, sql, timestamp_field, period, start_timestamp, stop_timestamp, offset) -%}

  {%- set period_filter -%}
    ("{{timestamp_field}}" >  '{{start_timestamp}}'::timestamp + interval '{{offset}} {{period}}' and
     "{{timestamp_field}}" <= '{{start_timestamp}}'::timestamp + interval '{{offset}} {{period}}' + interval '1 {{period}}' and
     "{{timestamp_field}}" <  '{{stop_timestamp}}'::timestamp)
  {%- endset -%}

  {%- set filtered_sql = sql | replace("__PERIOD_FILTER__", period_filter) -%}

  select
    {{target_cols_csv}}
  from (
    {{filtered_sql}}
  ) as t -- has to have an alias

{%- endmacro %}

{% macro snowflake__get_period_sql(target_cols_csv, sql, timestamp_field, period, start_timestamp, stop_timestamp, offset) -%}

  {%- set period_filter -%}
    ({{timestamp_field}} >  '{{start_timestamp}}'::timestamp + interval '{{offset}} {{period}}' and
     {{timestamp_field}} <= '{{start_timestamp}}'::timestamp + interval '{{offset}} {{period}}' + interval '1 {{period}}' and
     {{timestamp_field}} <  '{{stop_timestamp}}'::timestamp)
  {%- endset -%}

  {%- set filtered_sql = sql | replace("__PERIOD_FILTER__", period_filter) -%}

  select
    {{target_cols_csv}}
  from (
    {{filtered_sql}}
  )

{%- endmacro %}


{% macro get_insert_sql(target, source, dest_columns) -%}
  {{ adapter.dispatch('get_insert_sql')(target, source, dest_columns) }}
{%- endmacro %}

{% macro default__get_insert_sql(target, source, dest_columns) -%}

    {%- set dest_cols_csv = get_quoted_csv(dest_columns | map(attribute="name")) -%}

    insert into {{ target }} ({{ dest_cols_csv }})
    (
        select {{ dest_cols_csv }}
        from {{ source }}
    )

{%- endmacro %}


{% macro get_insert_period_sql(target,  sql, dest_columns, timestamp_field, period, start_timestamp, stop_timestamp, offset) -%}

    {%- set dest_cols_csv = get_quoted_csv(dest_columns | map(attribute="name")) -%}
    {%- set insert_sql = get_period_sql(dest_cols_csv, sql, timestamp_field, period, start_timestamp, stop_timestamp, offset) -%}

    insert into {{ target }} ({{ dest_cols_csv }})
    (
      {{ insert_sql }}
    )

{%- endmacro %}


{% macro get_rolling_delete_insert_merge_sql(target, source, unique_key, dest_columns, date_field, period, window) -%}
  {{ adapter.dispatch('get_delete_insert_merge_sql', 'dbt')(target, source, unique_key, dest_columns, date_field, period, window) }}
{%- endmacro %}

{% macro default__get_rolling_delete_insert_merge_sql(target, source, unique_key, dest_columns, date_field, period, window) -%}

    {%- set dest_cols_csv = get_quoted_csv(dest_columns | map(attribute="name")) -%}

    {% if unique_key %}
        {% if unique_key is sequence and unique_key is not string %}
            delete from {{target }}
            using {{ source }}
            where (
                {% for key in unique_key %}
                    {{ source }}.{{ key }} = {{ target }}.{{ key }}
                    {{ "and " if not loop.last }}
                {% endfor %}
                and {{ date_field }} < DATEADD({{ period }}, -{{ window }}, CURRENT_DATE())
            );
        {% else %}
            delete from {{ target }}
            where (
                {{ unique_key }}) in (
                select ({{ unique_key }})
                from {{ source }}
            );

        {% endif %}
        {% endif %}

    delete from {{ target }}
      where (
          {{ date_field }} < DATEADD({{ period }}, -{{ window }}, CURRENT_DATE())
      );

    insert into {{ target }} ({{ dest_cols_csv }})
    (
        select {{ dest_cols_csv }}
        from {{ source }}
    )

{%- endmacro %}

{% macro snowflake__get_rolling_delete_insert_merge_sql(target, source, unique_key, dest_columns, date_field, period, window) %}
    {% set dml = default__get_rolling_delete_insert_merge_sql(target, source, unique_key, dest_columns, date_field, period, window) %}
    {% do return(snowflake_dml_explicit_transaction(dml)) %}
{% endmacro %}


{% macro get_period_filter(insert=False) -%}
  {% if is_incremental() -%}
  TRUE
  {%- else %}
   __PERIOD_FILTER__
  {%- endif %}
{%- endmacro %}

{% macro get_period_filter_sql(timestamp_field, period, start_timestamp, stop_timestamp, offset) -%}

  {%- set period_filter -%}
    ({{timestamp_field}} >  '{{start_timestamp}}'::timestamp + interval '{{offset}} {{period}}' and
     {{timestamp_field}} <= '{{start_timestamp}}'::timestamp + interval '{{offset}} {{period}}' + interval '1 {{period}}' and
     {{timestamp_field}} <  '{{stop_timestamp}}'::timestamp)
  {%- endset -%}
  {{ return(period_filter) }}

{%- endmacro %}

{% macro get_insert_period_boundaries(start_date, stop_date, period) -%}

  {% call statement('period_boundaries', fetch_result=True) -%}
    select
      '{{ start_date }}'::timestamp as start_timestamp,
      '{{ stop_date }}'::timestamp as stop_timestamp,
      {{dbt_utils.datediff('start_timestamp','stop_timestamp', period)}}  + 1 as num_periods
  {%- endcall %}

  {%- set number_of_periods = load_result('period_boundaries')['data'][0][2] | int -%}

  {{ return(number_of_periods) }}

{%- endmacro %}



/* {# New logic for period based insert for large tables #} */
{% macro get_create_table_as_insert_sql(temporary, relation, sql, timestamp_field, period, start_timestamp, stop_timestamp, empty, dest_columns) -%}
  {{ adapter.dispatch('get_create_table_as_insert_sql', 'dbt')(temporary, relation, sql, timestamp_field, period, start_timestamp, stop_timestamp, empty, dest_columns) }}
{%- endmacro %}

{% macro default__get_create_table_as_insert_sql(temporary, relation, sql, timestamp_field, period, start_timestamp, stop_timestamp, empty, dest_columns) -%} 
  {%- set sql_header = config.get('sql_header', none) -%}
  {%- set empty_sql = sql | replace("__PERIOD_FILTER__", 'FALSE') -%}
  {%- set number_of_periods = get_insert_period_boundaries(start_timestamp,stop_timestamp, period) -%}
  {#%- set dest_columns = adapter.get_columns_in_relation(relation) -%#}
  {%- set dest_cols_csv = get_quoted_csv(dest_columns | map(attribute="name")) -%}

  {{ sql_header if sql_header is not none }}

  {% if empty %}
    create {% if temporary: -%}temporary{%- endif %} table
      {{ relation.include(database=(not temporary), schema=(not temporary)) }}
    as (
      {{ empty_sql }}
    )

  {% else %}
    create {% if temporary: -%}temporary{%- endif %} table
      {{ relation.include(database=(not temporary), schema=(not temporary)) }}
    as (
      {{ empty_sql }}
    );

    {% for inset_period in range(number_of_periods) -%}
      {%- set period_filter = get_period_filter_sql(timestamp_field, period, start_timestamp, stop_timestamp, inset_period) -%}
      {%- set filtered_sql = sql | replace("__PERIOD_FILTER__", period_filter) -%}
      
      
      insert into {{ relation }} ({{ dest_cols_csv }})
      (
        {{ filtered_sql }}
      ){{ ';' if not loop.last }}


    {%- endfor %}
  {% endif %} 

{%- endmacro %}

{% macro snowflake__get_create_table_as_insert_sql(temporary, relation, sql, timestamp_field, period, start_timestamp, stop_timestamp, empty, dest_columns) -%}
  {% set dml = snowflake__create_table_as_insert(temporary, relation, sql, timestamp_field, period, start_timestamp, stop_timestamp, empty, dest_columns) %}
  {% do return(snowflake_dml_explicit_transaction(dml)) %}
{%- endmacro %}


{% macro snowflake__create_table_as_insert(temporary, relation, sql, timestamp_field, period, start_timestamp, stop_timestamp, empty, dest_column) -%}
  {%- set transient = config.get('transient', default=true) -%}
  {%- set cluster_by_keys = config.get('cluster_by', default=none) -%}
  {%- set enable_automatic_clustering = config.get('automatic_clustering', default=false) -%}
  {%- set copy_grants = config.get('copy_grants', default=false) -%}
  {%- set empty_sql = sql | replace("__PERIOD_FILTER__", 'FALSE') -%}

  {%- if cluster_by_keys is not none and cluster_by_keys is string -%}
    {%- set cluster_by_keys = [cluster_by_keys] -%}
  {%- endif -%}
  {%- if cluster_by_keys is not none -%}
    {%- set cluster_by_string = cluster_by_keys|join(", ")-%}
  {% else %}
    {%- set cluster_by_string = none -%}
  {%- endif -%}
  {%- set sql_header = config.get('sql_header', none) -%}

  {{ sql_header if sql_header is not none }}

    {% if empty %}
      create or replace {% if temporary -%}
        temporary
      {%- elif transient -%}
        transient
      {%- endif %} table {{ relation }} {% if copy_grants and not temporary -%} copy grants {%- endif %} as
      (
        {%- if cluster_by_string is not none -%}
          select * from(
            {{ empty_sql }}
            ) order by ({{ cluster_by_string }})
        {%- else -%}
          {{ empty_sql }}
        {%- endif %}
      );

    {% else %}

      create or replace {% if temporary -%}
        temporary
      {%- elif transient -%}
        transient
      {%- endif %} table {{ relation }} {% if copy_grants and not temporary -%} copy grants {%- endif %} as
      (
        {%- if cluster_by_string is not none -%}
          select * from(
            {{ empty_sql }}
            ) order by ({{ cluster_by_string }})
        {%- else -%}
          {{ empty_sql }}
        {%- endif %}
      );

      {% for inset_period in range(number_of_periods) -%}
        {%- set period_filter = get_period_filter_sql(timestamp_field, period, start_timestamp, stop_timestamp, inset_period) -%}
        {%- set filtered_sql = sql | replace("__PERIOD_FILTER__", period_filter) -%}
        
        
        insert into {{ relation }} ({{ dest_cols_csv }})
        (
          {{ filtered_sql }}
        ){{ ';' if not loop.last }}


      {%- endfor %}
    {% endif %} 
    {% if cluster_by_string is not none and not temporary -%}
      alter table {{relation}} cluster by ({{cluster_by_string}});
    {%- endif -%}
    {% if enable_automatic_clustering and cluster_by_string is not none and not temporary  -%}
      alter table {{relation}} resume recluster;
    {%- endif -%}
{% endmacro %}