/* Every retained Congress.gov/GovInfo hearing↔meeting source edge must be
   represented by at least one durable bridge row. */
with expected as (
    select link_id::text as source_link_id
    from {{ ref('int_hearing_committee_meeting_links') }}
    where relationship_type in (
        'congress_hearing_to_committee_meeting_associated',
        'committee_meeting_to_congress_hearing_child',
        'govinfo_hearing_to_committee_meeting',
        'govinfo_joint_hearing_to_committee_meeting'
    )
), actual as (
    select distinct unnest(evidence_link_ids)::text as source_link_id
    from {{ ref('bridge_hearings__committee_meetings') }}
)
select expected.source_link_id
from expected
left join actual using (source_link_id)
where actual.source_link_id is null
