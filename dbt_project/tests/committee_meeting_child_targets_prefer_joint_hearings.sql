/*
   A meeting transcript edge may arrive through a House/Senate URL route even
   when the canonical hearing detail is joint.  The relationship model must
   target that unique joint identity instead of manufacturing a chamber copy.
*/
with routed_joint_conflicts as (
    select
        links.source_id as meeting_id,
        links.target_id,
        links.congress,
        hearings.hearing_jacketnumber
    from {{ ref('int_hearing_committee_meeting_links') }} as links
    join {{ ref('int_hearings__congress') }} as hearings
      on hearings.hearing_id = links.target_id
    where links.relationship_type = 'committee_meeting_to_congress_hearing_child'
      and hearings.chamber in ('house', 'senate')
      and exists (
          select 1
          from {{ ref('int_hearings__congress') }} as joint
          where joint.congress = links.congress
            and joint.hearing_jacketnumber = hearings.hearing_jacketnumber
            and joint.chamber = 'joint'
      )
)
select *
from routed_joint_conflicts
