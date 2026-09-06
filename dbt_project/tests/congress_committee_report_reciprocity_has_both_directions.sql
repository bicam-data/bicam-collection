/* Committee report links are published from both sides of the Congress.gov
   API: committee/{id}/reports and committee-report/{id}/committees.  The
   canonical union must retain both directions so a mismatch is visible
   rather than silently discarding one source's relationship. */
select relationship_type
from {{ ref('int_congress_internal_relationship_edges') }}
where relationship_type = 'committee_committee_report'
group by relationship_type
having not bool_or(edge_side = 'left')
    or not bool_or(edge_side = 'right')
