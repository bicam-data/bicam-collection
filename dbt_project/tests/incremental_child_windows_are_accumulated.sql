/*
   Every non-empty bill-cosponsor delta after the latest complete response is
   part of the resolved child relation. This guards against the old
   class_rank=1 behavior, which retained only the newest incremental window
   and silently discarded an earlier non-overlapping window.

   The query deliberately checks raw response identities rather than child
   row counts: duplicate observations are a separate natural-key
   reconciliation concern, while this test is about propagation completeness.
*/
with raw_children as (
    select
        child.raw_id,
        child.parent_id,
        child.endpoint_url,
        child.response,
        child.fetched_at,
        regexp_replace(child.endpoint_url, '[?].*$', '') as logical_endpoint,
        child.endpoint_url ~ '[?&](fromDateTime|toDateTime)=' as is_incremental,
        exists (
            select 1
            from jsonb_each(child.response) as response_field(field_name, field_value)
            where jsonb_typeof(response_field.field_value) = 'array'
        ) as has_array_field,
        exists (
            select 1
            from jsonb_each(child.response) as response_field(field_name, field_value)
            where jsonb_typeof(response_field.field_value) = 'array'
              and case
                when jsonb_typeof(response_field.field_value) = 'array'
                  then jsonb_array_length(response_field.field_value) > 0
                else false
              end
        ) as has_array_items
    from {{ ref('int_raw_responses__current') }} as child
    where child.source = 'congress'
      and child.category = 'bills'
      and child.node_type = 'bill_cosponsors'
), latest_complete as (
    select
        logical_endpoint,
        max(fetched_at) as fetched_at
    from raw_children
    where not is_incremental
      and has_array_field
    group by logical_endpoint
), expected_deltas as (
    select raw_children.raw_id
    from raw_children
    left join latest_complete using (logical_endpoint)
    where raw_children.is_incremental
      and raw_children.has_array_items
      and (
        latest_complete.fetched_at is null
        or raw_children.fetched_at >= latest_complete.fetched_at
      )
), observed_deltas as (
    {{ resolve_child('bill_detail', 'bill_cosponsors', 'bills') }}
)
select expected_deltas.raw_id
from expected_deltas
left join observed_deltas using (raw_id)
where observed_deltas.raw_id is null
