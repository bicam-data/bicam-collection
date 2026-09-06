{{ config(tags=["nominations", "staging"]) }}

select
    nomination_id,
    nomination_part_id,
    position_id,
    nominee_ordinal,
    first_name,
    middle_name,
    last_name,
    prefix,
    suffix,
    state,
    effective_date,
    predecessor_name,
    corps_code
from {{ ref('int_congress_nominee_observations') }}
