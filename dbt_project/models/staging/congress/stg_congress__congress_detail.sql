{{ config(tags=["congresses", "staging"]) }}

with detail as (
    select
        response -> 'congress' as congress_json
    from {{ source('raw', 'raw_responses') }}
    where source = 'congress'
      and category = 'congresses'
      and node_type = 'congress_detail'
)

select
    nullif(regexp_replace({{ json_text('congress_json', 'name') }}, '\D', '', 'g'), '')::integer as congress_number,
    {{ json_text('congress_json', 'name') }} as congress_name,
    {{ json_int('congress_json', 'startYear') }} as start_year,
    {{ json_int('congress_json', 'endYear') }} as end_year
from detail
