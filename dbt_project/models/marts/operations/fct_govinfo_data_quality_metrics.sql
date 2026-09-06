{{ config(
    materialized="table",
    tags=["operations", "quality", "govinfo", "identity_review"]
) }}

with committee_counts as (
    select
        count(*) as total,
        count(*) filter (where committee_id_normalization_status = 'manual_override') as manual_override,
        count(*) filter (where committee_id_normalization_status = 'exact_canonical') as exact_canonical,
        count(*) filter (where committee_id_normalization_status = 'verified_o_to_zero') as verified_o_to_zero,
        count(*) filter (where committee_id_normalization_status = 'unmapped_identifier') as unmapped_identifier,
        count(*) filter (where committee_id_normalization_status = 'non_identifier_text') as non_identifier_text
    from {{ ref('stg_govinfo__committeereports_committees') }}
),
committee_report_members as (
    select
        members.*,
        coalesce(component_resolution.all_components_resolved, false)
            as all_components_resolved
    from {{ ref('stg_govinfo__committeereports_members') }} as members
    left join (
        select
            source_member_key,
            bool_and(resolved_bioguide_id is not null)
                and bool_and(not requires_manual_review) as all_components_resolved
        from {{ ref('bridge_govinfo__member_components') }}
        group by source_member_key
    ) as component_resolution
      on component_resolution.source_member_key = members.source_member_key
), member_counts as (
    select
        count(*) as total,
        count(*) filter (where bioguide_resolution_status = 'provided_valid') as provided_valid,
        count(*) filter (where bioguide_resolution_status = 'missing_source_identifier') as missing_source_identifier,
        count(*) filter (where bioguide_resolution_status = 'provided_invalid') as provided_invalid,
        count(*) filter (
            where bioguide_resolution_status = 'multi_legislator_review'
              and not all_components_resolved
        ) as multi_legislator_review,
        count(*) filter (
            where multi_legislator_flag
              and all_components_resolved
        ) as multi_legislator_components_resolved,
        count(*) filter (where bioguide_resolution_status = 'manual_override') as manual_override
    from committee_report_members
), submittedby_counts as (
    select
        count(*) as total,
        count(*) filter (where bioguide_resolution_status = 'provided_valid') as provided_valid,
        count(*) filter (where bioguide_resolution_status = 'missing_source_identifier') as missing_source_identifier,
        count(*) filter (
            where bioguide_resolution_status = 'multi_legislator_review'
              and not all_components_resolved
        ) as multi_legislator_review,
        count(*) filter (
            where multi_legislator_flag
              and all_components_resolved
        ) as multi_legislator_components_resolved,
        count(*) filter (where bioguide_resolution_status = 'manual_override') as manual_override
    from committee_report_members
    where identity_context = 'submittedby'
), committee_map_counts as (
    select
        count(*) as total,
        count(*) filter (where normalization_status = 'exact_canonical') as exact_canonical,
        count(*) filter (where normalization_status = 'verified_o_to_zero') as verified_o_to_zero,
        count(*) filter (where normalization_status = 'manual_override') as manual_override,
        count(*) filter (where normalization_status = 'reviewed_authority_exception') as reviewed_authority_exception,
        count(*) filter (where normalization_status = 'unmapped_identifier') as unmapped_identifier,
        count(*) filter (where normalization_status = 'non_identifier_text') as non_identifier_text
    from {{ ref('int_govinfo_committee_id_map') }}
), nested_identity_counts as (
    select
        count(*) as total,
        count(*) filter (where identity_field = 'bioguide_id') as bioguide_id_total,
        count(*) filter (where identity_status = 'valid_bioguide') as valid_bioguide,
        count(*) filter (where identity_status = 'invalid_bioguide') as invalid_bioguide,
        count(*) filter (where identity_status = 'resolved_authority_name') as resolved_authority_name,
        count(*) filter (where identity_status = 'reviewed_authority_exception') as reviewed_authority_exception,
        count(*) filter (where identity_field in ('thomas_id', 'authority_id')) as thomas_or_authority_total,
        count(*) filter (where identity_status = 'non_identifier_text') as non_identifier_text
    from {{ ref('int_govinfo_identity_fields') }}
), member_component_counts as (
    select
        count(*) as total,
        count(*) filter (where resolved_bioguide_id is not null) as resolved,
        count(*) filter (where resolution_status = 'manual_component_override') as manual_override,
        count(*) filter (where proposed_bioguide_id is not null) as proposed,
        count(*) filter (where requires_manual_review) as pending_review,
        count(*) filter (where resolution_status = 'ambiguous_candidate') as ambiguous
    from {{ ref('bridge_govinfo__member_components') }}
), member_reference_counts as (
    select
        count(*) as total,
        count(*) filter (where source_bioguide_id is not null) as source_bioguide,
        count(*) filter (where resolution_status = 'manual_override') as manual_override,
        count(*) filter (where review_disposition = 'reviewed_non_federal_participant') as reviewed_non_federal_participant,
        count(*) filter (where resolution_status like 'unique_%_candidate') as resolved_candidate,
        count(*) filter (where source_bioguide_id is null) as missing_source_identifier,
        count(*) filter (
            where source_bioguide_id is null
              and resolution_status like 'unique_%_candidate'
        ) as inferred_bioguide,
        count(*) filter (
            where source_bioguide_id is null
              and resolved_bioguide_id is null
              and review_disposition is null
              and not (multi_legislator_flag and coalesce(component_resolution.all_components_resolved, false))
        ) as unresolved_missing_source_identifier,
        count(*) filter (where resolved_bioguide_id is not null) as resolved,
        count(*) filter (where linkable_bioguide_id is not null) as linkable,
        count(*) filter (
            where requires_manual_review
              and review_disposition is null
              and not (multi_legislator_flag and coalesce(component_resolution.all_components_resolved, false))
        ) as pending_review,
        count(*) filter (where resolution_status = 'ambiguous_review') as ambiguous,
        count(*) filter (
            where resolution_status = 'multi_legislator_review'
              and review_disposition is null
              and not coalesce(component_resolution.all_components_resolved, false)
        ) as multi_legislator_review,
        count(*) filter (
            where multi_legislator_flag
              and component_resolution.all_components_resolved
        ) as resolved_composite_parent
    from {{ ref('bridge_govinfo__member_references') }} as refs
    left join (
        select source_member_reference_key,
               bool_and(resolved_bioguide_id is not null)
                   and bool_and(not requires_manual_review) as all_components_resolved
        from {{ ref('bridge_govinfo__member_reference_components') }}
        group by source_member_reference_key
    ) as component_resolution
      on component_resolution.source_member_reference_key = refs.source_member_reference_key
), member_reference_component_counts as (
    select
        count(*) as total,
        count(*) filter (where resolved_bioguide_id is not null) as resolved,
        count(*) filter (where requires_manual_review) as pending_review,
        count(*) filter (where resolution_status = 'ambiguous_candidate') as ambiguous,
        count(*) filter (where resolution_status = 'no_candidate') as no_candidate
    from {{ ref('bridge_govinfo__member_reference_components') }}
),
metrics as (
    select 'committee_id_total' as metric_name, total::numeric as metric_value, 'pass' as quality_status from committee_counts
    union all select 'committee_id_manual_override', manual_override::numeric, 'pass' from committee_counts
    union all select 'committee_id_exact_canonical', exact_canonical::numeric, 'pass' from committee_counts
    union all select 'committee_id_verified_o_to_zero', verified_o_to_zero::numeric, 'pass' from committee_counts
    union all select 'committee_id_unmapped_identifier', unmapped_identifier::numeric, case when unmapped_identifier > 0 then 'review' else 'pass' end from committee_counts
    union all select 'committee_id_non_identifier_text', non_identifier_text::numeric, case when non_identifier_text > 0 then 'review' else 'pass' end from committee_counts
    union all select 'committee_id_map_total', total::numeric, 'pass' from committee_map_counts
    union all select 'committee_id_map_exact_canonical', exact_canonical::numeric, 'pass' from committee_map_counts
    union all select 'committee_id_map_verified_o_to_zero', verified_o_to_zero::numeric, 'pass' from committee_map_counts
    union all select 'committee_id_map_manual_override', manual_override::numeric, 'pass' from committee_map_counts
    union all select 'committee_id_map_reviewed_authority_exception', reviewed_authority_exception::numeric, 'pass' from committee_map_counts
    union all select 'committee_id_map_unmapped_identifier', unmapped_identifier::numeric,
        case when unmapped_identifier > 0 then 'review' else 'pass' end from committee_map_counts
    union all select 'committee_id_map_non_identifier_text', non_identifier_text::numeric,
        case when non_identifier_text > 0 then 'review' else 'pass' end from committee_map_counts
    union all select 'member_total', total::numeric, 'pass' from member_counts
    union all select 'member_provided_valid', provided_valid::numeric, 'pass' from member_counts
    union all select 'member_missing_source_identifier', missing_source_identifier::numeric, case when missing_source_identifier > 0 then 'review' else 'pass' end from member_counts
    union all select 'member_provided_invalid', provided_invalid::numeric, case when provided_invalid > 0 then 'review' else 'pass' end from member_counts
    union all select 'member_multi_legislator_review', multi_legislator_review::numeric, case when multi_legislator_review > 0 then 'review' else 'pass' end from member_counts
    union all select 'member_multi_legislator_components_resolved', multi_legislator_components_resolved::numeric, 'pass' from member_counts
    union all select 'member_manual_override', manual_override::numeric, 'pass' from member_counts
    union all select 'submittedby_total', total::numeric, 'pass' from submittedby_counts
    union all select 'submittedby_provided_valid', provided_valid::numeric, 'pass' from submittedby_counts
    union all select 'submittedby_missing_source_identifier', missing_source_identifier::numeric,
        case when missing_source_identifier > 0 then 'review' else 'pass' end from submittedby_counts
    union all select 'submittedby_multi_legislator_review', multi_legislator_review::numeric,
        case when multi_legislator_review > 0 then 'review' else 'pass' end from submittedby_counts
    union all select 'submittedby_multi_legislator_components_resolved', multi_legislator_components_resolved::numeric, 'pass' from submittedby_counts
    union all select 'submittedby_manual_override', manual_override::numeric, 'pass' from submittedby_counts
    union all select 'nested_identity_field_total', total::numeric, 'pass' from nested_identity_counts
    union all select 'nested_bioguide_id_total', bioguide_id_total::numeric, 'pass' from nested_identity_counts
    union all select 'nested_valid_bioguide', valid_bioguide::numeric, 'pass' from nested_identity_counts
    union all select 'nested_resolved_authority_name', resolved_authority_name::numeric, 'pass' from nested_identity_counts
    union all select 'nested_reviewed_authority_exception', reviewed_authority_exception::numeric, 'pass' from nested_identity_counts
    union all select 'nested_invalid_bioguide', invalid_bioguide::numeric,
        case when invalid_bioguide > 0 then 'review' else 'pass' end from nested_identity_counts
    union all select 'nested_thomas_or_authority_id_total', thomas_or_authority_total::numeric, 'pass' from nested_identity_counts
    union all select 'nested_non_identifier_text', non_identifier_text::numeric,
        case when non_identifier_text > 0 then 'review' else 'pass' end from nested_identity_counts
    union all select 'member_component_total', total::numeric, 'pass' from member_component_counts
    union all select 'member_component_resolved', resolved::numeric, 'pass' from member_component_counts
    union all select 'member_component_manual_override', manual_override::numeric, 'pass' from member_component_counts
    union all select 'member_component_proposed', proposed::numeric, 'pass' from member_component_counts
    union all select 'member_component_pending_review', pending_review::numeric,
        case when pending_review > 0 then 'review' else 'pass' end from member_component_counts
    union all select 'member_component_ambiguous', ambiguous::numeric,
        case when ambiguous > 0 then 'review' else 'pass' end from member_component_counts
    union all select 'member_reference_total', total::numeric, 'pass' from member_reference_counts
    union all select 'member_reference_source_bioguide', source_bioguide::numeric, 'pass' from member_reference_counts
    union all select 'member_reference_manual_override', manual_override::numeric, 'pass' from member_reference_counts
    union all select 'member_reference_reviewed_non_federal_participant', reviewed_non_federal_participant::numeric, 'pass' from member_reference_counts
    union all select 'member_reference_resolved_candidate', resolved_candidate::numeric, 'pass' from member_reference_counts
    union all select 'member_reference_missing_source_identifier', missing_source_identifier::numeric,
        case when missing_source_identifier > 0 then 'review' else 'pass' end from member_reference_counts
    union all select 'member_reference_inferred_bioguide', inferred_bioguide::numeric, 'pass' from member_reference_counts
    union all select 'member_reference_unresolved_missing_source_identifier', unresolved_missing_source_identifier::numeric,
        case when unresolved_missing_source_identifier > 0 then 'review' else 'pass' end from member_reference_counts
    union all select 'member_reference_resolved', resolved::numeric, 'pass' from member_reference_counts
    union all select 'member_reference_linkable', linkable::numeric, 'pass' from member_reference_counts
    union all select 'member_reference_pending_review', pending_review::numeric,
        case when pending_review > 0 then 'review' else 'pass' end from member_reference_counts
    union all select 'member_reference_ambiguous', ambiguous::numeric,
        case when ambiguous > 0 then 'review' else 'pass' end from member_reference_counts
    union all select 'member_reference_multi_legislator_review', multi_legislator_review::numeric,
        case when multi_legislator_review > 0 then 'review' else 'pass' end from member_reference_counts
    union all select 'member_reference_resolved_composite_parent', resolved_composite_parent::numeric, 'pass' from member_reference_counts
    union all select 'member_reference_component_total', total::numeric, 'pass' from member_reference_component_counts
    union all select 'member_reference_component_resolved', resolved::numeric, 'pass' from member_reference_component_counts
    union all select 'member_reference_component_pending_review', pending_review::numeric,
        case when pending_review > 0 then 'review' else 'pass' end from member_reference_component_counts
    union all select 'member_reference_component_ambiguous', ambiguous::numeric,
        case when ambiguous > 0 then 'review' else 'pass' end from member_reference_component_counts
    union all select 'member_reference_component_no_candidate', no_candidate::numeric,
        case when no_candidate > 0 then 'review' else 'pass' end from member_reference_component_counts
)

select
    metric_name,
    metric_value,
    quality_status
from metrics
