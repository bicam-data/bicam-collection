{% macro safe_int(expr, default_val=none) %}
    {# Replicates BaseCleanerLogic.safe_int() - safely convert to integer, handling strings with non-numeric chars #}
    (
      case
        when {{ expr }} is null or {{ expr }}::text = '' then {% if default_val is not none %}{{ default_val }}{% else %}null{% endif %}
        else (
          case
            when regexp_replace({{ expr }}::text, '[^0-9\-]', '', 'g') in ('', '-') then {% if default_val is not none %}{{ default_val }}{% else %}null{% endif %}
            else regexp_replace({{ expr }}::text, '[^0-9\-]', '', 'g')::int
          end
        )
      end
    )
{% endmacro %}

{% macro safe_float(expr, default_val=none) %}
    {# Replicates BaseCleanerLogic.safe_float() - safely convert to float #}
    (
      case
        when {{ expr }} is null or {{ expr }}::text = '' then {% if default_val is not none %}{{ default_val }}{% else %}null{% endif %}
        else (
          case
            when regexp_replace({{ expr }}::text, '[^0-9\.-]', '', 'g') in ('', '-', '.') then {% if default_val is not none %}{{ default_val }}{% else %}null{% endif %}
            else regexp_replace({{ expr }}::text, '[^0-9\.-]', '', 'g')::float8
          end
        )
      end
    )
{% endmacro %}

{% macro standardize_chamber(expr) %}
    (
      case lower(coalesce({{ expr }}::text, ''))
        when 'h' then 'house'
        when 'house' then 'house'
        when 'house of representatives' then 'house'
        when 's' then 'senate'
        when 'senate' then 'senate'
        when 'joint' then 'joint'
        when 'both' then 'joint'
        when 'nochamber' then 'nochamber'
        when 'no chamber' then 'nochamber'
        else nullif({{ expr }}::text, '')
      end
    )
{% endmacro %}

{% macro safe_timestamp(expr) %}
    (
      case
        when {{ expr }} is null then null
        when {{ expr }}::text ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' then ({{ expr }}::date)::timestamptz
        when {{ expr }}::text ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}[ T][0-9]{2}:[0-9]{2}(:[0-9]{2})?(Z|[+-][0-9]{2}:[0-9]{2})?$' then {{ expr }}::timestamptz
        else null
      end
    )
{% endmacro %}

{% macro standardize_date(expr) %}
    {# Replicates BaseCleanerLogic.standardize_date() - parse and standardize dates to timestamptz #}
    {# Handles various date formats and ensures timezone-aware timestamps (assumes UTC if timezone-naive) #}
    (
      case
        when {{ expr }} is null or {{ expr }}::text in ('', 'null', 'None') then null
        when {{ expr }}::text ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' then ({{ expr }}::date)::timestamptz
        when {{ expr }}::text ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}[ T][0-9]{2}:[0-9]{2}(:[0-9]{2})?(\.?[0-9]+)?(Z|[+-][0-9]{2}:?[0-9]{2})?$' then 
          case
            when {{ expr }}::text ~ '[Z+-]' then {{ expr }}::timestamptz
            else ({{ expr }}::timestamp)::timestamptz
          end
        else null
      end
    )
{% endmacro %}

{% macro clean_long_text(expr) %}
    {# Replicates BaseCleanerLogic.clean_long_text() - HTML removal, whitespace normalization, artifact removal #}
    (
      case when {{ expr }} is null then null else
        trim(
          regexp_replace(
            regexp_replace(
              regexp_replace(
                regexp_replace(
                  regexp_replace(
                    regexp_replace(
                      regexp_replace(
                        regexp_replace(
                          regexp_replace(
                            regexp_replace(
                              regexp_replace(
                                regexp_replace(
                                  regexp_replace(
                                    regexp_replace(
                                      regexp_replace(
                                        regexp_replace(
                                          {{ expr }}::text,
                                          '<!DOCTYPE[^>]*>', '', 'gi'
                                        ),
                                        '<\\?xml[^>]*\\?>', '', 'gi'
                                      ),
                                      '</p>', E'\n\n', 'gi'
                                    ),
                                    '<br\\s*/?>', E'\n', 'gi'
                                  ),
                                  '</div>', E'\n', 'gi'
                                ),
                                '</section>', E'\n\n', 'gi'
                              ),
                              '<[^>]+>', '', 'g'
                            ),
                            '&nbsp;', ' ', 'g'
                          ),
                          '&lt;', '<', 'g'
                        ),
                        '&gt;', '>', 'g'
                      ),
                      '&amp;', '&', 'g'
                    ),
                    E'\\x00', '', 'g'
                  ),
                  E'\\uFFFD', '', 'g'
                ),
                E'\\u00AD', '', 'g'
              ),
              E'\\r\\n', E'\n', 'g'
            ),
            E'\\r', E'\n', 'g'
          )
        )
      end
    )
{% endmacro %}

{# GovInfo set/part helpers for IDs like 'srpt36-1-115' (parted) or 'srpt36-115' (no parts) #}
{% macro govinfo_part_number(id_expr) %}
    (
      case
        when {{ id_expr }} is null then 0
        else (
          case
            when array_length(string_to_array({{ id_expr }}::text, '-'), 1) >= 3 then (
              nullif(regexp_replace((string_to_array({{ id_expr }}::text, '-'))[2], '[^0-9\-]', '', 'g'), '')::int
            )
            else 0
          end
        )
      end
    )
{% endmacro %}

{% macro govinfo_set_id(id_expr) %}
    (
      case
        when {{ id_expr }} is null then null
        else (
          case
            when array_length(string_to_array({{ id_expr }}::text, '-'), 1) >= 3
              then (
                (string_to_array({{ id_expr }}::text, '-'))[1]
                || '-' ||
                (string_to_array({{ id_expr }}::text, '-'))[3]
              )
            else {{ id_expr }}::text
          end
        )
      end
    )
{% endmacro %}
