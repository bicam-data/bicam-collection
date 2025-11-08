{% macro columns_in(relation) %}
  {# Returns a list of column names in the relation (lowercased) #}
  {% set cols = adapter.get_columns_in_relation(relation) %}
  {% set names = [] %}
  {% for c in cols %}
    {% do names.append(c.name | lower) %}
  {% endfor %}
  {{ return(names) }}
{% endmacro %}

{% macro if_has_col(cols, name, sql) %}
  {# Conditionally render SQL if column exists in cols #}
  {% if name | lower in cols %}
    {{ sql }}
  {% endif %}
{% endmacro %}

