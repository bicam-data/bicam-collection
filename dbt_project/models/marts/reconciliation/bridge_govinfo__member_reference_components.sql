{{ config(
    materialized="table",
    tags=["govinfo_bills", "govinfo_committee_reports", "govinfo_hearings", "reconciliation", "quality", "identity_review"],
    indexes=[
        {"columns": ["component_key"], "unique": true},
        {"columns": ["resolved_bioguide_id"]}
    ]
) }}

/* Component-level IDs for composite GovInfo member references. */
with components as (
    select *
    from {{ ref('int_govinfo_member_reference_components') }}
), component_rows as (
    select distinct on (component_key) *
    from components
    order by component_key,
        case resolution_status
            when 'source_bioguide_evidence' then 0
            when 'unique_name_context_candidate' then 0
            when 'ambiguous_candidate' then 1
            else 2
        end,
        candidate_bioguide_id nulls last
), reviewed_components as (
    /*
       The original committee-report component bridge contains a small set
       of manually reviewed report identities.  Reuse that durable evidence
       for the all-category component bridge by joining on report and the
       normalized component name; the raw source key formats differ between
       the two generations of models.
    */
    select
        report_id,
        regexp_replace(
            lower(trim(component_name)),
            '^(mr|ms|mrs|miss|dr)\.?\s+',
            '',
            'i'
        ) as component_name_key,
        max(resolved_bioguide_id) as reviewed_bioguide_id
    from {{ ref('bridge_govinfo__member_components') }}
    where resolved_bioguide_id is not null
    group by report_id, component_name_key
), rollup as (
    select
        component_key,
        array_agg(distinct candidate_bioguide_id order by candidate_bioguide_id)
            filter (where candidate_bioguide_id is not null) as candidate_bioguide_ids,
        count(distinct candidate_bioguide_id) filter (where candidate_bioguide_id is not null)
            as candidate_id_count,
        max(candidate_bioguide_id) filter (where matches_source_bioguide)
            as source_bioguide_id,
        max(candidate_bioguide_id) filter (where candidate_count = 1)
            as proposed_bioguide_id,
        max(candidate_name) filter (where candidate_count = 1) as proposed_member_name,
        max(candidate_state) filter (where candidate_count = 1) as proposed_state,
        max(candidate_chamber) filter (where candidate_count = 1) as proposed_chamber
    from components
    group by component_key
)
select
    {{ hash_to_uuid("'govinfo_member_reference_component|' || component_rows.component_key") }}
        as member_reference_component_bridge_id,
    component_rows.component_key,
    component_rows.source_member_reference_key,
    component_rows.raw_id,
    component_rows.category,
    component_rows.node_type,
    component_rows.package_id,
    component_rows.granule_id,
    component_rows.report_id,
    component_rows.congress,
    component_rows.state,
    component_rows.chamber,
    component_rows.identity_context,
    component_rows.component_ordinal,
    component_rows.component_name,
    component_rows.component_name_clean,
    component_rows.component_last_name_key,
    coalesce(rollup.candidate_bioguide_ids, '{}'::text[]) as candidate_bioguide_ids,
    coalesce(rollup.candidate_id_count, 0) as candidate_id_count,
    reviewed_components.reviewed_bioguide_id,
    coalesce(
        reviewed_components.reviewed_bioguide_id,
        rollup.source_bioguide_id,
        rollup.proposed_bioguide_id
    )
        as proposed_bioguide_id,
    case
        when reviewed_components.reviewed_bioguide_id is not null
            then reviewed_components.reviewed_bioguide_id
        when component_rows.resolution_status = 'source_bioguide_evidence'
            then rollup.source_bioguide_id
        when component_rows.resolution_status = 'unique_name_context_candidate'
            then rollup.proposed_bioguide_id
        else null
    end as resolved_bioguide_id,
    rollup.proposed_member_name,
    rollup.proposed_state,
    rollup.proposed_chamber,
    case
        when reviewed_components.reviewed_bioguide_id is not null
            then 'reviewed_component_evidence'
        else component_rows.resolution_status
    end as resolution_status,
    reviewed_components.reviewed_bioguide_id is null
        and component_rows.resolution_status not in (
            'unique_name_context_candidate',
            'source_bioguide_evidence'
        )
        as requires_manual_review
from component_rows
join rollup using (component_key)
left join reviewed_components
  on reviewed_components.report_id = component_rows.report_id
 and reviewed_components.component_name_key = lower(trim(component_rows.component_name))
