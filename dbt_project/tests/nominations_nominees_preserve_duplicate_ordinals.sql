/* Position ordinals are reused for multiple people. Verify that the
   source-array observation key, rather than (nomination_id, ordinal), is the
   public reconciliation grain: repeated snapshots may be removed, but every
   person in a position must survive. */
with source_groups as (
    select
        {{ bicam_public_nomination_id('nomination_part_id') }} as nomination_id,
        nominee_ordinal as ordinal,
        count(distinct (
            regexp_replace(
                regexp_replace(source_endpoint_url, '([?&])(fromDateTime|toDateTime|limit)=[^&]*&?', '\\1', 'g'),
                '[?&]$', ''
            ),
            source_ordinal
        )) as source_count
    from {{ ref('int_congress_nominee_observations') }}
    where nomination_part_id is not null
      and nominee_ordinal is not null
    group by 1, 2
    having count(*) > 1
), public_groups as (
    select nomination_id, ordinal, count(*) as public_count
    from {{ ref('nominations_nominees') }}
    group by 1, 2
)
select source_groups.nomination_id, source_groups.ordinal
from source_groups
left join public_groups using (nomination_id, ordinal)
where coalesce(public_groups.public_count, 0) <> source_groups.source_count
