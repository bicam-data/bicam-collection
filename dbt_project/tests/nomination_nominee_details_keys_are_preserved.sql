with nominee_responses as (
    {{ resolve_child('nomination_detail', 'nomination_nominee_detail', 'nominations') }}
), expected as (
    select
        parent_raw_id,
        count(*) as expected_rows
    from nominee_responses
    cross join lateral jsonb_array_elements(
        coalesce(response -> 'nominees', '[]'::jsonb)
    ) as nominee(value)
    group by 1
), actual as (
    select
        parent_raw_id,
        count(*) as actual_rows,
        count(distinct raw_id) as distinct_raw_ids
    from {{ ref('stg_congress__nomination_nominee_details') }}
    group by 1
)
select expected.parent_raw_id
from expected
left join actual using (parent_raw_id)
where actual.parent_raw_id is null
   or actual.actual_rows <> expected.expected_rows
   or actual.distinct_raw_ids <> expected.expected_rows
