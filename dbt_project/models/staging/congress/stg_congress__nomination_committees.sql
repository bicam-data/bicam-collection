{{ config(tags=["nominations", "staging"]) }}

select distinct
    nomination_id,
    nomination_part_id,
    committee_id
from {{ ref('stg_congress__nomination_committeeactivities') }}
