{{ config(
    materialized="table",
    tags=["operations", "quality", "govinfo", "identity_review"]
) }}

with component_resolution as (
    /* A composite source object has no single Bioguide, but it is fully
       linkable when every named component has an evidence-backed ID.  Keep
       those parents in the source-facing staging model and component bridge,
       but do not send them back to reviewers as unresolved identities. */
    select
        source_member_key,
        bool_and(resolved_bioguide_id is not null)
            and bool_and(not requires_manual_review) as all_components_resolved
    from {{ ref('bridge_govinfo__member_components') }}
    group by source_member_key
), members as (
    select members.*
    from {{ ref('stg_govinfo__committeereports_members') }} as members
    left join component_resolution
      on component_resolution.source_member_key = members.source_member_key
    -- Manual overrides are already reviewed, durable decisions. Keep the
    -- exception queue limited to unresolved source values; otherwise an
    -- override or deterministic enrichment would paradoxically continue to
    -- appear as pending review. Source omission status is retained separately
    -- for data-quality metrics.
    where bioguide_resolution_status not in ('provided_valid', 'manual_override')
      and linkable_bioguide_id is null
      and not (
          multi_legislator_flag
          and coalesce(component_resolution.all_components_resolved, false)
      )
),
reports as (
    select distinct report_id, congress
    from {{ ref('stg_govinfo__committeereports') }}
),
committees as (
    select
        report_id,
        string_agg(distinct committee_id, ',' order by committee_id)
            filter (where committee_id is not null) as committee_ids,
        string_agg(distinct committee_name, ' | ' order by committee_name)
            filter (where committee_name is not null) as committee_names
    from {{ ref('stg_govinfo__committeereports_committees') }}
    group by report_id
)

select
    members.source_member_key,
    members.report_id,
    members.report_set_id,
    members.package_id,
    members.granule_id,
    reports.congress,
    committees.committee_ids,
    committees.committee_names,
    members.bioguide_id,
    members.bioguide_id_raw,
    members.parsed_name,
    members.member_name_raw,
    members.member_name_normalized,
    members.state,
    members.role,
    members.party,
    members.chamber,
    members.identity_context,
    members.multi_legislator_flag,
    members.bioguide_resolution_status,
    'pending_manual_review' as review_status
from members
left join reports using (report_id)
left join committees using (report_id)
