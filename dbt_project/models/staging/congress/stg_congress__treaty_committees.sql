{{ config(tags=["treaties", "staging"]) }}

select distinct
    treaty_id,
    treaty_part_id,
    committee_id,
    committee_name
from {{ ref('stg_congress__treaty_committeeactivities') }}
