
{% materialization incremental_only, default -%}

  -- If there is no __PERIOD_FILTER__ specified, raise error.
  {{ check_for_period_filter(model.unique_id, sql) }}

  -- relations
  {%- set existing_relation = load_cached_relation(this) -%}
  {%- set target_relation = this.incorporate(type='table') -%}
  {#%- set temp_relation = make_temp_relation(target_relation)-%#}
  {%- set intermediate_relation = make_intermediate_relation(target_relation)-%}
  {%- set backup_relation_type = 'table' if existing_relation is none else existing_relation.type -%}
  {%- set backup_relation = make_backup_relation(target_relation, backup_relation_type) -%}

  -- configs
  {%- set unique_key = config.get('unique_key') -%}
  {%- set full_refresh_mode = (should_full_refresh()  or existing_relation.is_view) -%}
  {%- set on_schema_change = incremental_validate_on_schema_change(config.get('on_schema_change'), default='ignore') -%}
  {%- set timestamp_field = config.require('timestamp_field') -%}
  {%- set start_date = config.require('start_date') -%}
  {%- set stop_date = config.get('stop_date') or '' -%}
  {%- set period = config.get('period') or 'week' -%}

  -- the temp_ and backup_ relations should not already exist in the database; get_relation
  -- will return None in that case. Otherwise, we get a relation that we can drop
  -- later, before we try to use this name for the current operation. This has to happen before
  -- BEGIN, in a separate transaction
  {%- set preexisting_intermediate_relation = load_cached_relation(intermediate_relation)-%}
  {%- set preexisting_backup_relation = load_cached_relation(backup_relation) -%}
   -- grab current tables grants config for comparision later on
  {% set grant_config = config.get('grants') %}
  {{ drop_relation_if_exists(preexisting_intermediate_relation) }}
  {{ drop_relation_if_exists(preexisting_backup_relation) }}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  -- `BEGIN` happens here:
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {% set to_drop = [] %}

  {%- set empty_sql = sql | replace("__PERIOD_FILTER__", 'false') -%}

  {% if existing_relation is none %}
      {% set build_sql = get_create_table_as_sql(False, target_relation, empty_sql) %}
      {% set used_relation = target_relation%}
  {% elif full_refresh_mode %}
      {% set build_sql = get_create_table_as_sql(False, intermediate_relation, empty_sql) %}
      {% set used_relation = intermediate_relation%}
      {% set need_swap = true %}

  {% endif %}

  {% call statement("main") %}
      {{ build_sql }}
  {% endcall %}

  {% set _ = get_period_boundaries(database, schema, identifier, timestamp_field, start_date, stop_date, period) %}
  {%- set start_timestamp = load_result('period_boundaries')['data'][0][0] | string -%}
  {%- set stop_timestamp = load_result('period_boundaries')['data'][0][1] | string -%}
  {%- set num_periods = load_result('period_boundaries')['data'][0][2] | int -%}

  {% for insert_period in range(num_periods) -%} 
    {%- set msg = "Running for " ~ period ~ " " ~ (insert_period + 1) ~ " of " ~ num_periods -%}
    {{ dbt_utils.log_info(msg) }}

    {%- set tmp_identifier_suffix = '__dbt_incremental_period_' ~ insert_period ~ '_tmp' -%}
    {% set temp_relation = make_temp_relation(used_relation, tmp_identifier_suffix) %}

    {% set tmp_table_sql = get_period_sql(target_cols_csv, sql, timestamp_field, period, start_timestamp, stop_timestamp, insert_period) %}

    {% do run_query(get_create_table_as_sql(True, temp_relation, tmp_table_sql)) %}
    {% do adapter.expand_target_column_types(
              from_relation=temp_relation,
              to_relation=target_relation) %}
    {#-- Process schema changes. Returns dict of changes if successful. Use source columns for upserting/merging --#}
    {% set dest_columns = process_schema_changes(on_schema_change, temp_relation, existing_relation) %}
    {% if not dest_columns %}
      {% set dest_columns = adapter.get_columns_in_relation(existing_relation) %}
    {% endif %}

    {#% do run_query(create_table_as(True, tmp_relation, tmp_table_sql)) %#}

    {#% do adapter.expand_target_column_types(
             from_relation=tmp_relation,
             to_relation=target_relation) %#}

    {%- set name = 'main-' ~ insert_period -%}
    {# set build_sql = incremental_upsert(tmp_relation, target_relation, unique_key=unique_key) #}
    
    {#% set build_sql = get_delete_insert_merge_sql(used_relation, temp_relation, unique_key, target_columns) %#}

    {% set build_sql = get_insert_period_sql(used_relation, target_columns, sql, timestamp_field, period, start_timestamp, stop_timestamp, insert_period) %}
    
    {% call statement(name, fetch_result=True) -%}
      {{ build_sql }}
    {%- endcall %}

    {% set result = load_result('main-' ~ insert_period) %}
    {% if 'response' in result.keys() %} {# added in v0.19.0 #}
        {% set rows_inserted = result['response']['rows_affected'] %}
    {% else %} {# older versions #}
        {% set rows_inserted = result['status'].split(" ")[2] | int %}
    {% endif %}

    {%- set sum_rows_inserted = loop_vars['sum_rows_inserted'] + rows_inserted -%}
    {%- if loop_vars.update({'sum_rows_inserted': sum_rows_inserted}) %} {% endif -%}

    {%- set msg = "Ran completed for " ~ period ~ " " ~ ( insert_period + 1 ) ~ " of " ~ num_periods ~ "; " ~ rows_inserted ~ " records inserted" -%}
    {{ dbt_utils.log_info(msg) }}
  {%- endfor %}
 



  {% if need_swap %}
      {% do adapter.rename_relation(target_relation, backup_relation) %}
      {% do adapter.rename_relation(intermediate_relation, target_relation) %}
      {% do to_drop.append(backup_relation) %}
  {% endif %}

  {% set should_revoke = should_revoke(existing_relation, full_refresh_mode) %}
  {% do apply_grants(target_relation, grant_config, should_revoke=should_revoke) %}

  {% do persist_docs(target_relation, model) %}

  {% if existing_relation is none or existing_relation.is_view or should_full_refresh() %}
    {% do create_indexes(target_relation) %}
  {% endif %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  -- `COMMIT` happens here
  {% do adapter.commit() %}

  {% for rel in to_drop %}
      {% do adapter.drop_relation(rel) %}
  {% endfor %}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}

{%- endmaterialization %}