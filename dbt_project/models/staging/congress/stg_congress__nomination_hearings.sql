{{ config(tags=["nominations", "staging"]) }}

with hearing_responses as (
    {{ resolve_child('nomination_detail', 'nomination_hearings', 'nominations') }}
),
unnested as (
    select
        substring(parent_endpoint_url from '/nomination/([0-9]+)/')::integer as nomination_congress,
        substring(parent_endpoint_url from '/nomination/[0-9]+/([0-9]+)') as nomination_number,
        coalesce(
            substring(parent_endpoint_url from '/nomination/[0-9]+/[0-9]+/([^/?]+)'),
            '00'
        ) as nomination_part,
        hearing.value as hearing_json
    from hearing_responses
    cross join lateral {{ unnest_array('response', 'hearings', 'hearing') }}
),
parsed as (
    select
        'pn' || nomination_number || '-' || nomination_congress::text as nomination_id,
        'pn' || nomination_number || '-' || nomination_part || '-' || nomination_congress::text as nomination_part_id,
        lower({{ json_text('hearing_json', 'chamber') }}) as chamber,
        coalesce(
            nullif(substring({{ json_text('hearing_json', 'url') }} from '/hearing/([0-9]+)/'), ''),
            nomination_congress::text
        )::integer as congress,
        coalesce({{ json_text('hearing_json', 'partNumber') }}, '0') as hearing_part,
        coalesce(
            {{ json_bigint('hearing_json', 'jacketNumber') }}::text,
            'MISSING'
        ) as hearing_jacketnumber,
        hearing_json
    from unnested
)

select
    nomination_id,
    nomination_part_id,
    lower(left(chamber, 1))
    || 'hrg'
    || hearing_jacketnumber
    || '-'
    || congress::text as hearing_id,
    lower(left(chamber, 1))
    || 'hrg'
    || hearing_jacketnumber
    || '-'
    || hearing_part
    || '-'
    || congress::text as hearing_part_id,
    chamber,
    hearing_jacketnumber,
    congress,
    {{ json_text('hearing_json', 'number') }} as hearing_number,
    hearing_part,
    {{ json_date('hearing_json', 'date') }} as hearing_date,
    {{ json_text('hearing_json', 'errataNumber') }} as "errataNumber",
    {{ json_text('hearing_json', 'citation') }} as citation
from parsed
