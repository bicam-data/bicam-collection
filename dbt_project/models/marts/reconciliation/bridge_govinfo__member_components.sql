{{ config(
    tags=["govinfo_committee_reports", "reconciliation", "marts", "identity_review"],
    indexes=[
        {"columns": ["component_key"], "unique": true},
        {"columns": ["proposed_bioguide_id"]},
        {"columns": ["resolved_bioguide_id"]}
    ]
) }}

/*
   A GovInfo SUBMITTEDBY object can name more than one legislator while
   carrying at most one Bioguide value.  Keep the source object and its
   component-level evidence intact, but expose one stable bridge row per
   named component.  ``proposed_bioguide_id`` is populated only when the
   candidate set is exactly one.  ``resolved_bioguide_id`` is limited to the
   strongest source-ID evidence or an explicitly reviewed component override;
   surname-only and ambiguous candidates remain available without being
   silently promoted to authoritative identities.
*/
with candidates as (
    select *
    from {{ ref('int_govinfo_multi_legislator_candidates') }}
), component_metadata as (
    select distinct on (component_key)
        source_member_key,
        component_key,
        report_id,
        report_set_id,
        package_id,
        granule_id,
        congress,
        report_congress,
        matching_congress,
        state,
        chamber,
        member_name_raw,
        parsed_name,
        bioguide_id_raw,
        component_ordinal,
        component_name,
        component_last_name_key,
        resolution_status
    from candidates
    order by
        component_key,
        case resolution_status
            when 'source_bioguide_evidence' then 0
            when 'unique_name_congress_candidate' then 1
            when 'ambiguous_candidate' then 2
            else 3
        end
), candidate_rollup as (
    select
        component_key,
        array_agg(distinct candidate_bioguide_id order by candidate_bioguide_id)
            filter (where candidate_bioguide_id is not null) as candidate_bioguide_ids,
        count(distinct candidate_bioguide_id) filter (where candidate_bioguide_id is not null)
            as candidate_id_count,
        max(candidate_bioguide_id) filter (where candidate_count = 1)
            as sole_candidate_bioguide_id,
        max(candidate_name) filter (where candidate_count = 1)
            as sole_candidate_name,
        max(candidate_state) filter (where candidate_count = 1)
            as sole_candidate_state,
        max(candidate_chamber) filter (where candidate_count = 1)
            as sole_candidate_chamber,
        max(matches_source_bioguide::int)::boolean as has_source_bioguide_evidence,
        max(matches_source_state::int)::boolean as has_source_state_evidence
    from candidates
    group by component_key
), component_overrides as (
    select
        component_key,
        upper(nullif(trim(candidate_bioguide_id::text), '')) as candidate_bioguide_id,
        reviewer,
        reviewed_at,
        reason,
        expires_at
    from {{ ref('govinfo_member_component_identity_overrides') }}
    where expires_at is null or expires_at::date >= current_date
)
select
    {{ hash_to_uuid("'govinfo_member_component|' || metadata.component_key") }}
        as member_component_bridge_id,
    metadata.source_member_key,
    metadata.component_key,
    metadata.report_id,
    metadata.report_set_id,
    metadata.package_id,
    metadata.granule_id,
    metadata.congress,
    metadata.report_congress,
    metadata.matching_congress,
    metadata.state,
    metadata.chamber,
    metadata.member_name_raw,
    metadata.parsed_name,
    metadata.bioguide_id_raw,
    case
        when upper(trim(metadata.bioguide_id_raw)) ~ '^[A-Z][0-9]{6}$'
            then upper(trim(metadata.bioguide_id_raw))
        else null
    end as source_bioguide_id,
    metadata.component_ordinal,
    metadata.component_name,
    metadata.component_last_name_key,
    coalesce(rollup.candidate_bioguide_ids, '{}'::text[]) as candidate_bioguide_ids,
    coalesce(rollup.candidate_id_count, 0) as candidate_id_count,
    rollup.sole_candidate_bioguide_id as proposed_bioguide_id,
    case
        when overrides.candidate_bioguide_id is not null
            then overrides.candidate_bioguide_id
        when metadata.resolution_status = 'source_bioguide_evidence'
            then rollup.sole_candidate_bioguide_id
        else null
    end as resolved_bioguide_id,
    overrides.candidate_bioguide_id as manual_override_bioguide_id,
    overrides.reviewer as manual_override_reviewer,
    overrides.reviewed_at as manual_override_reviewed_at,
    overrides.reason as manual_override_reason,
    overrides.expires_at as manual_override_expires_at,
    rollup.sole_candidate_name as proposed_member_name,
    rollup.sole_candidate_state as proposed_state,
    rollup.sole_candidate_chamber as proposed_chamber,
    case
        when overrides.candidate_bioguide_id is not null then 'manual_component_override'
        else metadata.resolution_status
    end as resolution_status,
    coalesce(rollup.has_source_bioguide_evidence, false) as has_source_bioguide_evidence,
    coalesce(rollup.has_source_state_evidence, false) as has_source_state_evidence,
    case
        when overrides.candidate_bioguide_id is not null
            then 'manual_component_override'
        when metadata.resolution_status = 'source_bioguide_evidence'
            then 'source_bioguide_evidence'
        when metadata.resolution_status = 'unique_name_congress_candidate'
            then 'unique_name_congress_candidate_pending_review'
        when metadata.resolution_status = 'ambiguous_candidate'
            then 'ambiguous_manual_review'
        else 'no_candidate_manual_review'
    end as bridge_decision,
    overrides.candidate_bioguide_id is null
        and metadata.resolution_status <> 'source_bioguide_evidence'
        as requires_manual_review
from component_metadata as metadata
join candidate_rollup as rollup using (component_key)
left join component_overrides as overrides using (component_key)
