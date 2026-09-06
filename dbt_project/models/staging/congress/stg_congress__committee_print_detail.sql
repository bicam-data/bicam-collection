{{ config(tags=["committee_prints", "staging"]) }}

with detail_responses as (
    select
        raw_id,
        endpoint_url,
        raw_response_version_id,
        response
    from {{ ref('int_raw_responses__current') }}
    where source = 'congress'
      and category = 'committee-prints'
      and node_type = 'committee_print_detail'
),
unnested as (
    select
        raw_id,
        endpoint_url,
        raw_response_version_id,
        print.value as print_json
    from detail_responses
    cross join lateral {{ unnest_array('response', 'committeePrint', 'print') }}
),
typed as (
    select
        raw_id,
        endpoint_url,
        raw_response_version_id,
        lower(left({{ json_text('print_json', 'chamber') }}, 1))
        || 'prt'
        || {{ json_int('print_json', 'jacketNumber') }}::text
        || '-'
        || {{ json_int('print_json', 'congress') }}::text as print_id,
        lower(left({{ json_text('print_json', 'chamber') }}, 1)) || 'prt' as print_type,
        {{ json_int('print_json', 'jacketNumber') }} as print_jacketnumber,
        {{ json_int('print_json', 'congress') }} as congress,
        lower({{ json_text('print_json', 'chamber') }}) as chamber,
        {{ json_text('print_json', 'title') }} as title,
        {{ json_int('print_json', 'jacketNumber') }} as print_number,
        {{ json_text('print_json', 'citation') }} as citation,
        {{ json_int('print_json', 'text,count', 0) }} as texts_count,
        {{ json_timestamptz('print_json', 'updateDate') }} as updated_at
    from unnested
),
deduped as (
    select
        *,
        row_number() over (
            partition by print_id
            order by updated_at desc nulls last, texts_count desc nulls last, title desc nulls last
        ) as _rn
    from typed
)

select
    raw_id,
    endpoint_url,
    raw_response_version_id,
    print_id,
    print_type,
    print_jacketnumber,
    congress,
    chamber,
    title,
    print_number,
    citation,
    texts_count,
    updated_at
from deduped
where _rn = 1
