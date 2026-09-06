{{ config(tags=["treaties", "staging"]) }}

with detail_responses as (
    select
        response
    from {{ ref('int_raw_responses__current') }}
    where source = 'congress'
      and category = 'treaties'
      and node_type = 'treaty_detail'
),
treaties as (
    select
        treaty.value as treaty_json
    from detail_responses
    cross join lateral {{ unnest_array('response', 'treaty', 'treaty') }}
),
unnested as (
    select
        'td' || {{ json_int('treaty_json', 'congressReceived') }}::text || '-' || {{ json_text('treaty_json', 'number') }} as treaty_id,
        'td' || {{ json_int('treaty_json', 'congressReceived') }}::text || '-' || {{ json_text('treaty_json', 'number') }} || coalesce({{ json_text('treaty_json', 'suffix') }}, '') as treaty_part_id,
        related_doc.value as related_doc_json
    from treaties
    cross join lateral {{ unnest_array('treaty_json', 'relatedDocs', 'related_doc') }}
)

select
    treaty_id,
    treaty_part_id,
    lower(substring({{ json_text('related_doc_json', 'url') }} from '/committee-report/[0-9]+/([A-Za-z]+)/')) ||
        substring({{ json_text('related_doc_json', 'url') }} from '/committee-report/[0-9]+/[A-Za-z]+/([0-9]+)') ||
        '-' ||
        substring({{ json_text('related_doc_json', 'url') }} from '/committee-report/([0-9]+)/') as report_id,
    lower(substring({{ json_text('related_doc_json', 'url') }} from '/committee-report/[0-9]+/([A-Za-z]+)/')) as report_type,
    substring({{ json_text('related_doc_json', 'url') }} from '/committee-report/[0-9]+/[A-Za-z]+/([0-9]+)')::integer as report_number,
    substring({{ json_text('related_doc_json', 'url') }} from '/committee-report/([0-9]+)/')::integer as congress,
    {{ json_text('related_doc_json', 'citation') }} as citation
from unnested
where {{ json_text('related_doc_json', 'url') }} like '%/committee-report/%'
