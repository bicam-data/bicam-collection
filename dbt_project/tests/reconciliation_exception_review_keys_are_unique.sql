with duplicate_keys as (
    select entity, canonical_id
    from {{ ref('reconciliation_exception_reviews') }}
    group by 1, 2
    having count(*) > 1
)

select * from duplicate_keys
