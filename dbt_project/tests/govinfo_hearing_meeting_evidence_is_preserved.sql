/* Every GovInfo hearing-to-meeting link produced by the linker must remain
   visible on the hearing reconciliation row. This guards against silently
   counting only joint packages while dropping ordinary HHRG/SHRG evidence. */
with linked_packages as (
    select distinct source_id as package_id
    from {{ ref('int_hearing_committee_meeting_links') }}
    where relationship_type in (
        'govinfo_hearing_to_committee_meeting',
        'govinfo_joint_hearing_to_committee_meeting'
    )
      and source_system = 'govinfo'
), hearing_matches as (
    select govinfo_source_record_id as package_id,
           committee_meeting_evidence_count
    from {{ ref('fct_hearing_matches') }}
)
select linked_packages.package_id
from linked_packages
left join hearing_matches using (package_id)
where hearing_matches.package_id is null
   or coalesce(hearing_matches.committee_meeting_evidence_count, 0) = 0
