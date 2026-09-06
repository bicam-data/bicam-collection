{% macro normalize_congress_bill_number_text(text_expr) -%}
  case
    -- Avoid an integer cast here: PostgreSQL may pre-evaluate a casted CASE
    -- arm while planning a view, and historical source values can contain
    -- Unicode fractions.  ltrim preserves the text contract safely.
    when {{ text_expr }} ~ '^[0-9]+$' then coalesce(nullif(ltrim({{ text_expr }}, '0'), ''), '0')
    when {{ text_expr }} ~ '^[0-9]+½$' then regexp_replace({{ text_expr }}, '½$', '.5')
    when {{ text_expr }} ~ '^[0-9]+¼$' then regexp_replace({{ text_expr }}, '¼$', '.25')
    when {{ text_expr }} ~ '^[0-9]+¾$' then regexp_replace({{ text_expr }}, '¾$', '.75')
    when {{ text_expr }} ~ '^[IVXLCDM]+$' then (
      select sum(
        case
          when value < coalesce(next_value, 0) then -value
          else value
        end
      )::integer::text
      from (
        select
          case character
            when 'I' then 1
            when 'V' then 5
            when 'X' then 10
            when 'L' then 50
            when 'C' then 100
            when 'D' then 500
            when 'M' then 1000
          end as value,
          lead(
            case character
              when 'I' then 1
              when 'V' then 5
              when 'X' then 10
              when 'L' then 50
              when 'C' then 100
              when 'D' then 500
              when 'M' then 1000
            end
          ) over (order by ordinal) as next_value
        from regexp_split_to_table({{ text_expr }}, '') with ordinality as chars(character, ordinal)
      ) as roman_values
    )
    else {{ text_expr }}
  end
{%- endmacro %}

{% macro congress_bill_number(json_expr) -%}
  {{ normalize_congress_bill_number_text("nullif(" ~ json_expr ~ " #>> '{number}', '')") }}
{%- endmacro %}
