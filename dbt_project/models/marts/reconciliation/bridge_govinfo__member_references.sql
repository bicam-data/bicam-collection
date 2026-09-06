{{ config(
    materialized="table",
    tags=["govinfo_bills", "govinfo_committee_reports", "govinfo_hearings", "reconciliation", "quality", "identity_review"],
    indexes=[
        {"columns": ["source_member_reference_key"]},
        {"columns": ["resolved_bioguide_id"]},
        {"columns": ["legacy_source_member_key"]}
    ]
) }}

/*
   Source-neutral link table for every GovInfo member reference.  Provided,
   canonically formatted Bioguide IDs and reviewed committee-report overrides
   are retained; the raw source value remains available separately. Unique
   full-name/context candidates are filled with explicit evidence status.
   Composite/name-only objects stay reviewable rather than assigning one
   person to a multi-person source value.
*/
with member_refs as (
    select *
    from {{ ref('int_govinfo_member_references') }}
), candidates as (
    select *
    from {{ ref('int_govinfo_member_reference_candidates') }}
), candidate_rollup as (
    select
        source_member_reference_key,
        count(distinct candidate_bioguide_id) filter (
            where candidate_bioguide_id is not null
        ) as candidate_count,
        max(candidate_bioguide_id) filter (
            where resolution_status in (
                'unique_name_context_candidate',
                'unique_name_global_candidate',
                'unique_last_name_context_candidate'
            )
        ) as proposed_bioguide_id,
        max(candidate_name) filter (where candidate_rank = 1) as proposed_member_name,
        max(candidate_congress) filter (where candidate_rank = 1) as proposed_congress,
        max(candidate_state) filter (where candidate_rank = 1) as proposed_state,
        max(candidate_chamber) filter (where candidate_rank = 1) as proposed_chamber,
        max(candidate_match_basis) filter (where candidate_rank = 1) as candidate_match_basis,
        max(resolution_status) filter (where candidate_rank = 1) as candidate_resolution_status,
        array_agg(distinct candidate_bioguide_id order by candidate_bioguide_id)
            filter (where candidate_bioguide_id is not null) as candidate_bioguide_ids
    from candidates
    group by source_member_reference_key
), overrides as (
    select
        source_member_key::text as override_source_member_key,
        upper(trim(bioguide_id::text)) as override_bioguide_id,
        reviewer as override_reviewer,
        reviewed_at as override_reviewed_at,
        reason as override_reason,
        expires_at as override_expires_at
    from {{ ref('govinfo_member_identity_overrides') }}
    where expires_at is null or expires_at::date >= current_date
), reviewed_dispositions as (
    select
        source_member_reference_key::text as disposition_source_key,
        classification as review_disposition,
        reviewer as review_disposition_reviewer,
        reviewed_at as review_disposition_reviewed_at,
        reason as review_disposition_reason,
        expires_at as review_disposition_expires_at
    from {{ ref('govinfo_member_reference_reviews') }}
    where expires_at is null or expires_at::date >= current_date
)
select
    {{ hash_to_uuid("'govinfo_member_reference|' || member_refs.source_member_reference_key") }}
        as member_reference_bridge_id,
    member_refs.source_member_reference_key,
    member_refs.legacy_source_member_key,
    member_refs.raw_id,
    member_refs.endpoint_url,
    member_refs.raw_response_version_id,
    member_refs.source_issued_at,
    member_refs.category,
    member_refs.node_type,
    member_refs.package_id,
    member_refs.granule_id,
    member_refs.report_id,
    member_refs.members_array_hash,
    member_refs.member_ordinal,
    member_refs.parsed_name,
    member_refs.member_name_raw,
    member_refs.authority_fnf,
    member_refs.authority_lnf,
    member_refs.authority_other,
    member_refs.authority_id,
    member_refs.gpo_id,
    member_refs.state,
    member_refs.chamber,
    member_refs.role,
    member_refs.party,
    member_refs.congress,
    member_refs.identity_context,
    member_refs.multi_legislator_flag,
    member_refs.bioguide_id_raw,
    case
        when upper(trim(member_refs.bioguide_id_raw)) ~ '^[A-Z][0-9]{6}$'
            then upper(trim(member_refs.bioguide_id_raw))
        else null
    end as source_bioguide_id,
    overrides.override_bioguide_id,
    coalesce(
        overrides.override_bioguide_id,
        case
            when upper(trim(member_refs.bioguide_id_raw)) ~ '^[A-Z][0-9]{6}$'
                then upper(trim(member_refs.bioguide_id_raw))
            else null
        end,
        case
            when not member_refs.multi_legislator_flag
                 and candidate_rollup.candidate_count = 1
                then candidate_rollup.proposed_bioguide_id
            else null
        end
    ) as resolved_bioguide_id,
    /*
       Stable join key for downstream research models.  Keep the source value
       above untouched (``source_bioguide_id``) and expose the evidence-backed
       value separately so inferred IDs can be used for joins without being
       mistaken for a value that GovInfo actually supplied.  Composite parent
       objects intentionally remain null here; their component bridge carries
       one linkable ID per named legislator.
    */
    coalesce(
        overrides.override_bioguide_id,
        case
            when upper(trim(member_refs.bioguide_id_raw)) ~ '^[A-Z][0-9]{6}$'
                then upper(trim(member_refs.bioguide_id_raw))
            else null
        end,
        case
            when not member_refs.multi_legislator_flag
                 and candidate_rollup.candidate_count = 1
                then candidate_rollup.proposed_bioguide_id
            else null
        end
    ) as linkable_bioguide_id,
    /* Backwards-friendly name for consumers that expect a member key named
       bioguide_id.  It is deliberately the same resolved/linkable value; use
       source_bioguide_id when the distinction between source and enrichment
       matters. */
    coalesce(
        overrides.override_bioguide_id,
        case
            when upper(trim(member_refs.bioguide_id_raw)) ~ '^[A-Z][0-9]{6}$'
                then upper(trim(member_refs.bioguide_id_raw))
            else null
        end,
        case
            when not member_refs.multi_legislator_flag
                 and candidate_rollup.candidate_count = 1
                then candidate_rollup.proposed_bioguide_id
            else null
        end
    ) as bioguide_id,
    coalesce(candidate_rollup.candidate_bioguide_ids, '{}'::text[]) as candidate_bioguide_ids,
    coalesce(candidate_rollup.candidate_count, 0) as candidate_count,
    candidate_rollup.proposed_member_name,
    candidate_rollup.proposed_congress,
    candidate_rollup.proposed_state,
    candidate_rollup.proposed_chamber,
    candidate_rollup.candidate_match_basis,
    candidate_rollup.candidate_resolution_status,
    case
        when overrides.override_bioguide_id is not null then 'manual_override'
        when reviewed_dispositions.review_disposition is not null
            then reviewed_dispositions.review_disposition
        when member_refs.bioguide_resolution_status = 'provided_valid'
            then 'source_bioguide'
        when member_refs.multi_legislator_flag then 'multi_legislator_review'
        when candidate_rollup.candidate_count = 1
            then candidate_rollup.candidate_resolution_status
        when candidate_rollup.candidate_count > 1 then 'ambiguous_review'
        else 'missing_identifier_review'
    end as resolution_status,
    overrides.override_reviewer,
    overrides.override_reviewed_at,
    overrides.override_reason,
    overrides.override_expires_at,
    reviewed_dispositions.review_disposition,
    reviewed_dispositions.review_disposition_reviewer,
    reviewed_dispositions.review_disposition_reviewed_at,
    reviewed_dispositions.review_disposition_reason,
    reviewed_dispositions.review_disposition_expires_at,
    (
        overrides.override_bioguide_id is null
        and reviewed_dispositions.review_disposition is null
        and member_refs.bioguide_resolution_status <> 'provided_valid'
        and (
            member_refs.multi_legislator_flag
            or coalesce(candidate_rollup.candidate_count, 0) <> 1
        )
    ) as requires_manual_review
from member_refs
left join candidate_rollup
  on candidate_rollup.source_member_reference_key
      = member_refs.source_member_reference_key
left join overrides
  on overrides.override_source_member_key in (
      member_refs.legacy_source_member_key,
      member_refs.source_member_reference_key
  )
left join reviewed_dispositions
  on reviewed_dispositions.disposition_source_key = member_refs.source_member_reference_key
