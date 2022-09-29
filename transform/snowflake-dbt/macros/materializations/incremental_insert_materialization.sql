{% materialization incremental_insert_, adapter='snowflake' -%}

  {% set original_query_tag = set_query_tag() %}

  {%- set unique_key = config.get('unique_key') -%}
  {%- set timestamp_field = config.require('timestamp_field') -%}
  {%- set start_date = config.require('start_date')  -%}
  {%- set stop_date = config.get('stop_date') or modules.datetime.datetime.now() -%}
  {%- set period = config.get('period') or 'week' -%}
  

  {% set target_relation = this %}
  {% set existing_relation = load_relation(this) %}
  {% set tmp_relation = make_temp_relation(this) %}
  {% set empty_relation = make_temp_relation(this) %}
  {%- set full_refresh_mode = (should_full_refresh()) -%}

  {#-- Validate early so we don't run SQL if the strategy is invalid --#}
  {% set strategy = dbt_snowflake_validate_get_incremental_strategy(config) -%}
  {% set on_schema_change = incremental_validate_on_schema_change(config.get('on_schema_change'), default='ignore') %}

  {% set to_drop = [] %}

  {{ run_hooks(pre_hooks) }}

  {#-- Create an empty temp table to get columns for later inserts --#}
  {% set build_empty_sql = get_create_table_as_insert_sql(False, empty_relation, sql, timestamp_field, period, start_date, stop_date, True,[]) %}

  {%- call statement('pre') -%}
    {{ build_empty_sql }}
  {%- endcall -%}

  {%- set dest_columns = adapter.get_columns_in_relation(empty_relation) -%}

  {% do to_drop.append(empty_relation) %}

  {% if existing_relation is none %}
    {#% set build_sql = create_table_as(False, target_relation, sql) %#}
    {% set build_sql = get_create_table_as_insert_sql(False, target_relation, sql, timestamp_field, period, start_date, stop_date, False, dest_columns) %}

  {% elif existing_relation.is_view %}
    {#-- Can't overwrite a view with a table - we must drop --#}
    {{ log("Dropping relation " ~ target_relation ~ " because it is a view and this model is a table.") }}
    {% do adapter.drop_relation(existing_relation) %}

    {#% set build_sql = create_table_as(False, target_relation, sql) %#}
    {% set build_sql = get_create_table_as_insert_sql(False, target_relation, sql, timestamp_field, period, start_date, stop_date, False, dest_columns) %}

  {% elif full_refresh_mode %}

    {#% set build_sql = create_table_as(False, target_relation, sql) %#}
    {% set build_sql = get_create_table_as_insert_sql(False, target_relation, sql, timestamp_field, period, start_date, stop_date, False, dest_columns) %}

  {% else %}
    {% do run_query(create_table_as(True, tmp_relation, sql)) %}
    {% do adapter.expand_target_column_types(
           from_relation=tmp_relation,
           to_relation=target_relation) %}
    {#-- Process schema changes. Returns dict of changes if successful. Use source columns for upserting/merging --#}
    {% set dest_columns = process_schema_changes(on_schema_change, tmp_relation, existing_relation) %}
    {% if not dest_columns %}
      {% set dest_columns = adapter.get_columns_in_relation(existing_relation) %}
    {% endif %}
    {% set build_sql = dbt_snowflake_get_incremental_sql(strategy, tmp_relation, target_relation, unique_key, dest_columns) %}

  {% endif %}

  {%- call statement('main') -%}
    {{ build_sql }}
  {%- endcall -%}

  {{ run_hooks(post_hooks) }}

  {% for rel in to_drop %}
    {% do adapter.drop_relation(rel) %}
  {% endfor %}

  {% set target_relation = target_relation.incorporate(type='table') %}
  {% do persist_docs(target_relation, model) %}

  {% do unset_query_tag(original_query_tag) %}

  {{ return({'relations': [target_relation]}) }}

{%- endmaterialization %}