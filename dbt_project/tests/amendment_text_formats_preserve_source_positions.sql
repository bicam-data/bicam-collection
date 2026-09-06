with expected as (
    select
        amendment_id,
        count(distinct text_version_key) as expected_versions
    from {{ ref('int_congress_amendment_text_versions') }}
    group by 1
), actual as (
    select
        amendment_id,
        count(*) as actual_versions
    from {{ ref('stg_congress__amendment_texts') }}
    group by 1
)
select expected.*
from expected
left join actual using (amendment_id)
where actual.amendment_id is null
   or actual.actual_versions <> expected.expected_versions
