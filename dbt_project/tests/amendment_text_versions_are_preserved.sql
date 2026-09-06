/* A text-version row remains useful even when Congress.gov supplies no format
   URL. Do not silently discard those versions during format pivoting. */
with text_responses as (
    {{ resolve_child('amendment_detail', 'amendment_texts', 'amendments') }}
), amendment_detail as (
    select raw_id, amendment_id
    from {{ ref('stg_congress__amendment_detail') }}
), expected as (
    select distinct
        detail.amendment_id,
        {{ json_timestamptz('text_version.value', 'date') }} as date,
        {{ json_text('text_version.value', 'type') }} as type
    from text_responses
    join amendment_detail as detail
      on detail.raw_id = text_responses.parent_raw_id
    cross join lateral {{ unnest_array('response', 'textVersions', 'text_version') }}
)
select expected.amendment_id, expected.date, expected.type
from expected
left join {{ ref('stg_congress__amendment_texts') }} as actual
  on actual.amendment_id = expected.amendment_id
 and actual.date is not distinct from expected.date
 and actual.type is not distinct from expected.type
where actual.amendment_id is null
