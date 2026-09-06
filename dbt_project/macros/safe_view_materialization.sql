{#
  Keep partial dbt refreshes from deleting dependent staging views.

  dbt-postgres' stock view materialization swaps an intermediate view into
  place and drops the old relation with CASCADE.  That is safe for a complete
  graph build, but a targeted run can then remove unselected child views until
  a later full build recreates them.  Existing views are stable contracts in
  this project, so replace them in place and preserve their object identity.

  A relation that is changing materialization (for example, a table left by a
  prior configuration) is still dropped explicitly; that is an intentional,
  one-time migration rather than the normal refresh path.
#}
{% materialization view, default %}

  {% set existing_relation = load_cached_relation(this) %}
  {% set target_relation = this.incorporate(type='view') %}
  {% set grant_config = config.get('grants') %}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  {% if existing_relation is not none and existing_relation.type != 'view' %}
    {% do adapter.drop_relation(existing_relation) %}
    {% set existing_relation = none %}
  {% endif %}

  -- BEGIN is opened by the statement wrapper, as in dbt's default
  -- materialization. CREATE OR REPLACE preserves dependent-view references.
  {% call statement('main') -%}
    {{ get_replace_view_sql(target_relation, sql) }}
  {%- endcall %}

  {% set should_revoke = should_revoke(existing_relation, full_refresh_mode=True) %}
  {% do apply_grants(target_relation, grant_config, should_revoke=should_revoke) %}
  {% do persist_docs(target_relation, model) %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  {{ adapter.commit() }}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}

{% macro postgres__get_replace_view_sql(relation, sql) -%}
  {%- set sql_header = config.get('sql_header', none) -%}
  {{ sql_header if sql_header is not none }}
  create or replace view {{ relation }}
    {% set contract_config = config.get('contract') %}
    {% if contract_config.enforced %}
      {{ get_assert_columns_equivalent(sql) }}
    {%- endif %}
  as (
    {{ sql }}
  );
{%- endmacro %}
