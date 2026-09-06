/* A report-derived member candidate must be supported by a cited bill edge.
   This protects the identity enrichment from turning report text or a
   coincidental name match into a Bioguide assignment. */
with report_candidates as (
    select distinct
        candidates.source_member_reference_key,
        candidates.candidate_bioguide_id,
        candidates.candidate_count,
        candidates.resolution_status
    from {{ ref('int_govinfo_member_reference_candidates') }} as candidates
    where candidates.candidate_match_basis in (
        'report_bill_relationship',
        'report_bill_sponsor_relationship'
    )
), report_evidence as (
    select distinct
        refs.source_member_reference_key,
        bills.bill_id,
        edges.left_id as bioguide_id
    from {{ ref('int_govinfo_member_references') }} as refs
    join {{ ref('stg_govinfo__committeereports_reference_bills') }} as bills
      on bills.report_id = refs.report_id
    join {{ ref('int_congress_internal_relationship_edges') }} as edges
      on edges.right_id = bills.bill_id
     and edges.relationship_type in ('member_bill_sponsor', 'member_bill_cosponsor')
    where refs.category = 'govinfo-committee-reports'
      and bills.bill_id is not null
)
select candidates.source_member_reference_key
from report_candidates as candidates
where not exists (
    select 1
    from report_evidence as evidence
    where evidence.source_member_reference_key = candidates.source_member_reference_key
      and evidence.bioguide_id = candidates.candidate_bioguide_id
)
union all
select source_member_reference_key
from report_candidates
where candidate_count = 1
  and resolution_status <> 'unique_name_context_candidate'
