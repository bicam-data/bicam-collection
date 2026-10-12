{# Shared semantic-code rules for package IDs and rendition URL suffixes.
   v1rh -> RH (variant 1); rh2 -> RH (variant 2); eh1s -> EH (star print 1).
   A star print is not the legacy numeric variant. Retain the full suffix.
   This does not equate CRS summary version numbers with BILLS version codes. #}
{% macro govinfo_bill_version_code(suffix_expr) -%}
upper(coalesce(
  (regexp_match(lower({{ suffix_expr }}), '^([a-z]+)[1-9][0-9]*s$'))[1],
  (regexp_match(lower({{ suffix_expr }}), '^v[0-9]+([a-z]+)$'))[1],
  (regexp_match(lower({{ suffix_expr }}), '^([a-z]+)[0-9]*$'))[1]
))
{%- endmacro %}

{% macro govinfo_bill_version_variant(suffix_expr) -%}
coalesce(
  (regexp_match(lower({{ suffix_expr }}), '^v([0-9]+)[a-z]+$'))[1],
  (regexp_match(lower({{ suffix_expr }}), '^[a-z]+([0-9]+)$'))[1]
)::integer
{%- endmacro %}

{% macro govinfo_bill_package_suffix(package_expr) -%}
(regexp_match(lower({{ package_expr }}), '^bills-[0-9]+[a-z]+[0-9]+(v[0-9]+[a-z]+|[a-z]+(?:[0-9]+|[1-9][0-9]*s)?)$'))[1]
{%- endmacro %}

{% macro govinfo_bill_starprint_number(suffix_expr) -%}
(regexp_match(lower({{ suffix_expr }}), '^[a-z]+([1-9][0-9]*)s$'))[1]::integer
{%- endmacro %}

{% macro govinfo_bill_url_suffix(url_expr) -%}
(regexp_match(split_part(split_part(lower({{ url_expr }}), '?', 1), '#', 1), '/bills-[0-9]+[a-z]+[0-9]+(v[0-9]+[a-z]+|[a-z]+(?:[0-9]+|[1-9][0-9]*s)?)\.(?:html?|xml|pdf)$'))[1]
{%- endmacro %}

{% macro govinfo_bill_url_version_code(url_expr) -%}
{{ govinfo_bill_version_code(govinfo_bill_url_suffix(url_expr)) }}
{%- endmacro %}
