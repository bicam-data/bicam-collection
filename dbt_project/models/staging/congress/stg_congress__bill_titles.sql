{{ config(tags=["bills", "staging"]) }}

with title_responses as (
    {{ resolve_child('bill_detail', 'bill_titles', 'bills') }}
),
bill_detail as (
    select
        raw_id,
        bill_id
    from {{ ref('stg_congress__bill_detail') }}
),
unnested as (
    select
        bill_detail.bill_id,
        title.value as title_json
    from title_responses
    join bill_detail
      on title_responses.parent_raw_id = bill_detail.raw_id
    cross join lateral {{ unnest_array('response', 'titles', 'title') }}
)

select
    bill_id,
    nullif(title_json #>> '{title}', '') as title,
    nullif(title_json #>> '{titleType}', '') as title_type,
    lower(nullif(title_json #>> '{chamberName}', '')) as chamber,
    nullif(title_json #>> '{titleTypeCode}', '') as title_type_code,
    nullif(title_json #>> '{billTextVersionName}', '') as bill_text_version_name,
    nullif(title_json #>> '{billTextVersionCode}', '') as bill_text_version_code,
    (nullif(title_json #>> '{updateDate}', ''))::timestamptz as updated_at
from unnested
