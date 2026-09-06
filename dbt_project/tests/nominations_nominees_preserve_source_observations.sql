/* The position ordinal is not a nominee key. The public child must retain
   every source observation rather than collapsing duplicate ordinals. */
with expected as (
    select count(*) as row_count
    from (
        select distinct
            regexp_replace(
                regexp_replace(source_endpoint_url, '([?&])(fromDateTime|toDateTime|limit)=[^&]*&?', '\\1', 'g'),
                '[?&]$', ''
            ) as source_logical_endpoint,
            source_ordinal
        from {{ ref('int_congress_nominee_observations') }}
        where nomination_part_id is not null
          and nominee_ordinal is not null
    ) as distinct_observations
), observed as (
    select count(*) as row_count
    from {{ ref('nominations_nominees') }}
)
select expected.row_count, observed.row_count
from expected cross join observed
where expected.row_count <> observed.row_count
