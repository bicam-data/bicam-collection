{% macro slug_ident(name) %}
  {%- set s = (name | string | lower) -%}
  {%- set s = s | regex_replace('[^a-z0-9_]+', '_') -%}
  {%- if s|length == 0 -%}
    {%- set s = 'field' -%}
  {%- endif -%}
  {%- if s[0] in '0123456789' -%}
    {%- set s = 'f_' ~ s -%}
  {%- endif -%}
  {{ return(s) }}
{% endmacro %}

{% macro distinct_payload_keys(source_name, table_name, limit_keys=5000) %}
  {%- if execute -%}
    {%- set src = source(source_name, table_name) -%}
    {%- set sql -%}
      select distinct k as key
      from (
        select jsonb_object_keys(payload) as k from {{ src }} limit {{ limit_keys }}
      ) x
      where k is not null and k <> ''
      order by 1
    {%- endset -%}
    {%- set res = run_query(sql) -%}
    {%- set keys = [] -%}
    {%- if res is not none and res.rows is not none -%}
      {%- for r in res.rows -%}
        {%- do keys.append(r[0]) -%}
      {%- endfor -%}
    {%- endif -%}
    {{ return(keys) }}
  {%- else -%}
    {{ return([]) }}
  {%- endif -%}
{% endmacro %}

{% macro select_flatten_payload(source_name, table_name) %}
  {%- set keys = distinct_payload_keys(source_name, table_name) -%}
  {%- set src = source(source_name, table_name) -%}
  select
    id_uuid        as raw_id,
    source_doc_id  as source_doc_id,
    scraped_at     as scraped_at,
    payload        as payload
    {%- for k in keys %}
      {%- set col = slug_ident(k) -%}
      , payload ->> '{{ k }}' as {{ col }}
    {%- endfor %}
  from {{ src }}
{% endmacro %}
