{{ config(
    tags=["govinfo_bills", "govinfo_committee_reports", "govinfo_hearings", "govinfo_committee_prints", "staging", "identity_review"]
) }}

/*
   Person-level GovInfo member references for downstream research joins.

   ``bridge_govinfo__member_references`` deliberately preserves the original
   source object, including a composite object such as "Mr. Green and Mr.
   Hastings".  Such an object cannot safely carry one scalar Bioguide ID.  The
   component bridge splits it into one row per named legislator.  This view
   presents a single relation for consumers: ordinary source objects remain
   one row each and composite objects contribute their resolved components.
   Raw parent lineage is retained, and reviewed non-federal participants are
   kept with a null ID rather than being coerced into a member.
*/
with parent_references as (
    select
        refs.member_reference_bridge_id as member_reference_person_id,
        refs.source_member_reference_key,
        refs.source_member_reference_key as parent_source_member_reference_key,
        null::text as component_key,
        'member_object'::text as reference_kind,
        refs.member_ordinal,
        refs.raw_id,
        refs.raw_response_version_id,
        refs.endpoint_url,
        refs.source_issued_at,
        refs.category,
        refs.node_type,
        refs.package_id,
        refs.granule_id,
        refs.report_id,
        refs.congress,
        refs.state,
        refs.chamber,
        refs.role,
        refs.party,
        refs.identity_context,
        refs.parsed_name,
        refs.member_name_raw,
        refs.authority_fnf,
        refs.authority_lnf,
        refs.authority_other,
        refs.multi_legislator_flag,
        refs.bioguide_id_raw,
        refs.source_bioguide_id,
        refs.resolved_bioguide_id,
        refs.linkable_bioguide_id as bioguide_id,
        refs.resolution_status,
        refs.review_disposition,
        refs.requires_manual_review
    from {{ ref('bridge_govinfo__member_references') }} as refs
    where not refs.multi_legislator_flag
), composite_components as (
    select
        components.member_reference_component_bridge_id as member_reference_person_id,
        components.component_key as source_member_reference_key,
        components.source_member_reference_key as parent_source_member_reference_key,
        components.component_key,
        'member_component'::text as reference_kind,
        components.component_ordinal,
        refs.raw_id,
        refs.raw_response_version_id,
        refs.endpoint_url,
        refs.source_issued_at,
        refs.category,
        refs.node_type,
        refs.package_id,
        refs.granule_id,
        refs.report_id,
        components.congress,
        components.state,
        components.chamber,
        refs.role,
        refs.party,
        components.identity_context,
        components.component_name as parsed_name,
        refs.member_name_raw,
        refs.authority_fnf,
        refs.authority_lnf,
        refs.authority_other,
        true as multi_legislator_flag,
        refs.bioguide_id_raw,
        case
            when components.resolved_bioguide_id = refs.source_bioguide_id
                then refs.source_bioguide_id
            else null
        end as source_bioguide_id,
        components.resolved_bioguide_id,
        components.resolved_bioguide_id as bioguide_id,
        components.resolution_status,
        null::text as review_disposition,
        components.requires_manual_review
    from {{ ref('bridge_govinfo__member_reference_components') }} as components
    join {{ ref('bridge_govinfo__member_references') }} as refs
      on refs.source_member_reference_key = components.source_member_reference_key
)
select * from parent_references
union all
select * from composite_components
