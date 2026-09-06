/* Every valid canonical edge in the reconciliation bridge must be public. */
select
    b.bridge_id,
    b.committee_meeting_id as meeting_id,
    b.canonical_hearing_id as hearing_id
from {{ ref('bridge_hearings__committee_meetings') }} as b
left join {{ ref('committee_meetings_associated_hearings') }} as p
  on p.meeting_id = {{ bicam_public_committee_meeting_id('b.committee_meeting_id') }}
 and p.hearing_id = b.canonical_hearing_id
where b.committee_meeting_id is not null
  and b.canonical_hearing_id is not null
  and p.meeting_id is null
