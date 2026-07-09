{{ config(tags=["bills", "staging"]) }}

with subject_responses as (
    {{ resolve_child('bill_detail', 'bill_subjects', 'bills') }}
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
        subject_item.value as subject_json
    from subject_responses
    join bill_detail
      on subject_responses.parent_raw_id = bill_detail.raw_id
    cross join lateral {{ unnest_array('response', 'subjects.legislativeSubjects', 'subject_item') }}
)

select
    bill_id,
    nullif(subject_json #>> '{name}', '') as subject,
    (nullif(subject_json #>> '{updateDate}', ''))::timestamptz as updated_at
from unnested
