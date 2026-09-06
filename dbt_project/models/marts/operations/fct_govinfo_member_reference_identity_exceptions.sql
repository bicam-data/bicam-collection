{{ config(
    materialized="table",
    tags=["operations", "quality", "govinfo", "identity_review"]
) }}

/*
   All-category review queue for GovInfo member references that do not have a
   safe canonical Bioguide assignment.  The source-neutral bridge remains the
   system of record; this table is a compact work queue that includes the
   candidate IDs, raw context, and an explicit reason for review.
*/
with component_resolution as (
    /* A composite SUBMITTEDBY object is not assignable to one Bioguide, but
       it is no longer unresolved when every named component has an
       evidence-backed link in the component bridge. */
    select source_member_reference_key,
           bool_and(resolved_bioguide_id is not null)
               and bool_and(not requires_manual_review) as all_components_resolved
    from {{ ref('bridge_govinfo__member_reference_components') }}
    group by source_member_reference_key
)
select
    refs.member_reference_bridge_id,
    refs.source_member_reference_key,
    refs.raw_id,
    refs.raw_response_version_id,
    refs.category,
    refs.node_type,
    refs.package_id,
    refs.granule_id,
    refs.report_id,
    refs.congress,
    refs.state,
    refs.chamber,
    refs.role,
    refs.identity_context,
    refs.parsed_name,
    refs.member_name_raw,
    refs.authority_fnf,
    refs.authority_lnf,
    refs.authority_other,
    refs.multi_legislator_flag,
    refs.source_bioguide_id,
    refs.candidate_bioguide_ids,
    refs.candidate_count,
    refs.candidate_match_basis,
    refs.candidate_resolution_status,
    refs.resolution_status,
    case refs.resolution_status
        when 'ambiguous_review' then 'ambiguous_member_name'
        when 'multi_legislator_review' then 'composite_member_parent'
        when 'missing_identifier_review' then 'no_safe_congress_member_match'
        else 'unclassified_identity_exception'
    end as review_reason,
    'pending_manual_review' as review_status
from {{ ref('bridge_govinfo__member_references') }} as refs
left join component_resolution
  on component_resolution.source_member_reference_key = refs.source_member_reference_key
where refs.resolved_bioguide_id is null
  and refs.review_disposition is null
  and not (
      refs.multi_legislator_flag
      and coalesce(component_resolution.all_components_resolved, false)
  )
