{{ config(tags=["bills", "staging"]) }}

/*
   The child resolver intentionally returns the complete baseline plus every
   applicable non-empty delta.  Reconcile that evidence at the entity grain
   here: a bill can have one current cosponsor observation per Bioguide ID.
   The intermediate relation retains the source-array occurrences for audit.
*/
with observations as (
    select
        observations.*,
        row_number() over (
            partition by observations.bill_id, observations.bioguide_id
            order by observations.source_fetched_at desc nulls last,
                     observations.source_raw_id desc,
                     observations.source_ordinal desc
        ) as _rn
    from {{ ref('int_congress_bill_cosponsor_observations') }} as observations
    where observations.bioguide_id is not null
)

select
    bill_id,
    bioguide_id,
    display_name,
    party,
    state,
    district,
    is_original_cosponsor,
    sponsorship_date,
    sponsorship_withdrawal_date
from observations
where _rn = 1
