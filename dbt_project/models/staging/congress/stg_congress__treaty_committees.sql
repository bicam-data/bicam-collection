{{ config(tags=["treaties", "staging"]) }}

with extracted as (
    select
        observations.treaty_id,
        observations.treaty_part_id,
        {{ json_text('observations.committee_json', 'systemCode') }} as committee_id,
        {{ json_text('observations.committee_json', 'name') }} as committee_name,
        observations.source_fetched_at,
        observations.source_raw_id,
        observations.source_ordinal
    from {{ ref('int_congress_treaty_committee_observations') }} as observations
), keyed as (
    select
        extracted.*,
        row_number() over (
            partition by treaty_id, committee_id
            order by source_fetched_at desc nulls last,
                     source_raw_id desc,
                     source_ordinal desc,
                     committee_name desc nulls last
        ) as key_rank,
        treaty_id is not null and committee_id is not null as key_complete
    from extracted
)

select treaty_id, treaty_part_id, committee_id, committee_name
from keyed
where key_rank = 1 or not key_complete
