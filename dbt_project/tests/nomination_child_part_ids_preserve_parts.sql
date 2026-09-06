/* Nomination detail URLs use /{number}-{part}; child models must preserve
   that part in the canonical pn{number}-{part}-{congress} key. */
with expected as (
    select
        'pn123-01-118'::text as nomination_part_id,
        'HSAG'::text as committee_id
), observed as (
    select nomination_part_id, committee_id
    from {{ ref('stg_congress__nomination_committees') }}
    where nomination_part_id = 'pn123-01-118'
), malformed as (
    select nomination_part_id from {{ ref('stg_congress__nomination_actions') }}
    where nomination_part_id ~ '-[1-9]-[0-9]+$'
    union all
    select nomination_part_id from {{ ref('stg_congress__nomination_actions_committees') }}
    where nomination_part_id ~ '-[1-9]-[0-9]+$'
    union all
    select nomination_part_id from {{ ref('stg_congress__nomination_committeeactivities') }}
    where nomination_part_id ~ '-[1-9]-[0-9]+$'
    union all
    select nomination_part_id from {{ ref('stg_congress__nomination_hearings') }}
    where nomination_part_id ~ '-[1-9]-[0-9]+$'
    union all
    select nomination_part_id from {{ ref('stg_congress__nomination_nominees') }}
    where nomination_part_id ~ '-[1-9]-[0-9]+$'
)
select expected.nomination_part_id, expected.committee_id
from expected
left join observed using (nomination_part_id, committee_id)
where observed.nomination_part_id is null
union all
select malformed.nomination_part_id, null::text
from malformed
