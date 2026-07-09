{{ config(tags=["committees", "staging"]) }}

with detail as (
    select
        response -> 'committee' as committee_json
    from {{ source('raw', 'raw_responses') }}
    where source = 'congress'
      and category = 'committees'
      and node_type = 'committee_detail'
),
unnested as (
    select
        {{ json_text('committee_json', 'systemCode') }} as committee_id,
        history.value as history_json
    from detail
    cross join lateral {{ unnest_array('committee_json', 'history', 'history') }}
)

select
    committee_id,
    {{ json_text('history_json', 'officialName') }} as name,
    {{ json_text('history_json', 'libraryOfCongressName') }} as loc_name,
    {{ json_timestamptz('history_json', 'startDate') }} as started_at,
    {{ json_timestamptz('history_json', 'endDate') }} as ended_at,
    {{ json_text('history_json', 'committeeTypeCode') }} as committee_type,
    {{ json_text('history_json', 'establishingAuthority') }} as establishing_authority,
    {{ json_text('history_json', 'superintendentDocumentNumber') }} as su_doc_class_number,
    {{ json_text('history_json', 'naraId') }} as nara_id,
    {{ json_text('history_json', 'locLinkedDataId') }} as loc_linked_data_id
from unnested
