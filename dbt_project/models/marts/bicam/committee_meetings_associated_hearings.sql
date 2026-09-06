{{ config(schema='bicam', materialized='table', tags=['bicam_public', 'committee_meetings'], indexes=[{'columns': ['meeting_id', 'hearing_id'], 'unique': true}, {'columns': ['hearing_id']}]) }}
/* The reconciliation bridge is the union of hearing->meeting and meeting->hearing evidence. */
select distinct
    {{ bicam_public_committee_meeting_id('committee_meeting_id') }} as meeting_id,
    canonical_hearing_id as hearing_id
from {{ ref('bridge_hearings__committee_meetings') }}
where committee_meeting_id is not null
  and canonical_hearing_id is not null
