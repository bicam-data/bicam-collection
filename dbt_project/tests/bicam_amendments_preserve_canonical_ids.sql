with expected as (
    select canonical_amendment_id as amendment_id
    from {{ ref('int_amendments__congress') }}
),
actual as (
    select amendment_id
    from {{ ref('amendments') }}
)

select expected.amendment_id
from expected
left join actual using (amendment_id)
where actual.amendment_id is null
