/*
   A child endpoint without a pagination object is one complete payload. The
   resolver must not downgrade those rows to an unexplained incomplete
   generation merely because no crawl-run page aggregate exists.
*/
with observations as (
    select source_raw_id, source_is_complete_generation
    from {{ ref('int_congress_bill_related_bill_observations') }}

    union all

    select source_raw_id, source_is_complete_generation
    from {{ ref('int_congress_bill_action_observations') }}
), violations as (
    select distinct
        observations.source_raw_id,
        raw.endpoint_url
    from observations
    join {{ source('raw', 'raw_responses') }} as raw
      on raw.id = observations.source_raw_id
    where not (raw.response ? 'pagination')
      and not observations.source_is_complete_generation
)
select *
from violations
