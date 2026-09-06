{{ config(tags=["congresses", "staging"]) }}

with detail as (
    select
        response -> 'congress' as congress_json
    from {{ ref('int_raw_responses__current') }}
    where source = 'congress'
      and category = 'congresses'
      and node_type = 'congress_detail'
),
unnested as (
    select
        nullif(regexp_replace({{ json_text('congress_json', 'name') }}, '\D', '', 'g'), '')::integer as congress_number,
        session.value as session_json
    from detail
    cross join lateral {{ unnest_array('congress_json', 'sessions', 'session') }}
)

select
    congress_number,
    {{ json_int('session_json', 'number') }} as session,
    case {{ json_text('session_json', 'chamber') }}
        when 'House of Representatives' then 'house'
        when 'Senate' then 'senate'
        else null
    end as chamber,
    {{ json_text('session_json', 'type') }} as type,
    {{ json_date('session_json', 'startDate') }} as start_date,
    {{ json_date('session_json', 'endDate') }} as end_date
from unnested
