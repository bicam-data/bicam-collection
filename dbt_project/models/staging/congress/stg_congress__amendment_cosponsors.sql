{{ config(tags=["amendments", "staging"]) }}

/*
   The child resolver and source-array expansion live in the materialized
   observation relation.  Reading that relation here prevents this staging
   model from silently reverting to latest-URL state when adjacent incremental
   windows or equivalent query-string variants exist.
*/
with observed as (
    select
        observations.*,
        row_number() over (
            partition by observations.amendment_id, observations.bioguide_id
            order by observations.source_fetched_at desc nulls last,
                     observations.source_raw_response_version_id desc nulls last,
                     observations.source_raw_id desc,
                     observations.source_ordinal desc
        ) as key_rank,
        observations.amendment_id is not null
            and observations.bioguide_id is not null as key_complete
    from {{ ref('int_congress_amendment_cosponsor_observations') }} as observations
)

select
    amendment_id,
    bioguide_id,
    display_name,
    first_name,
    middle_name,
    last_name,
    party,
    state,
    district,
    is_original_cosponsor,
    sponsorship_date,
    sponsorship_withdrawal_date
from observed
where key_rank = 1 or not key_complete
