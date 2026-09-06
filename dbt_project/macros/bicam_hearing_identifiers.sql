{% macro bicam_govinfo_hearing_id(package_id_expression) -%}
case
    when {{ package_id_expression }} ~* '^CHRG-[0-9]+[hsj]hrg[0-9]+'
    then
        lower((regexp_match(
            {{ package_id_expression }},
            '^CHRG-([0-9]+)([hsj]hrg)([0-9]+)(.*)$',
            'i'
        ))[2])
        || coalesce(nullif(ltrim((regexp_match(
            {{ package_id_expression }},
            '^CHRG-([0-9]+)([hsj]hrg)([0-9]+)(.*)$',
            'i'
        ))[3], '0'), ''), '0')
        /* Preserve the source suffix and its case. P1/Op1/O/v1 variants are
           separate published package identities, not duplicate base rows. */
        || (regexp_match(
            {{ package_id_expression }},
            '^CHRG-([0-9]+)([hsj]hrg)([0-9]+)(.*)$',
            'i'
        ))[4]
        || '-'
        || (regexp_match(
            {{ package_id_expression }},
            '^CHRG-([0-9]+)([hsj]hrg)([0-9]+)(.*)$',
            'i'
        ))[1]
end
{%- endmacro %}
