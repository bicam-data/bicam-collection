with accepted as (
    select
        congress_source_record_id,
        govinfo_source_record_id
    from {{ ref('fct_bill_matches') }}
    where decision = 'accepted'
),
conflicts as (
    select congress_source_record_id
    from accepted
    where congress_source_record_id is not null
    group by 1
    having count(*) > 1
    union all
    select govinfo_source_record_id
    from accepted
    where govinfo_source_record_id is not null
    group by 1
    having count(*) > 1
)

select * from conflicts
