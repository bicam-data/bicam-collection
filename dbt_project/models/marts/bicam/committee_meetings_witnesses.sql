{{ config(schema='bicam', materialized='table', tags=['bicam_public', 'committee_meetings'], indexes=[{'columns': ['meeting_id', 'name'], 'unique': true}, {'columns': ['meeting_id']}]) }}
select distinct on (meeting_id, witness_name)
    {{ bicam_public_committee_meeting_id('meeting_id') }} as meeting_id,
    witness_name as name,
    witness_position as position,
    witness_org as organization
from {{ ref('stg_congress__committee_meeting_witnesses') }}
where meeting_id is not null and witness_name is not null
order by
    meeting_id,
    witness_name,
    (witness_position is not null) desc,
    (witness_org is not null) desc,
    witness_position nulls last,
    witness_org nulls last
