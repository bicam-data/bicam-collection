{{ config(tags=["committee_print_reconciliation", "committee_prints", "reconciliation", "marts"]) }}

select
    {{ hash_to_uuid("'committee_print|' || govinfo.package_id") }} as committee_print_package_bridge_id,
    govinfo.package_id,
    govinfo.source_record_id as govinfo_source_record_id,
    govinfo.source_raw_id as govinfo_raw_id,
    govinfo.source_raw_response_version_id as govinfo_raw_response_version_id,
    govinfo.source_endpoint_url as govinfo_endpoint_url,
    govinfo.canonical_print_id,
    govinfo.print_type,
    govinfo.print_jacketnumber,
    govinfo.congress,
    govinfo.chamber,
    govinfo.title,
    govinfo.collection_code,
    govinfo.identity_status,
    govinfo.is_representative_package,
    case
        when congress.canonical_print_id is not null then 'accepted'
        else 'unmatched'
    end as link_decision,
    case
        when congress.canonical_print_id is not null then 'exact_canonical_identity'
        when govinfo.canonical_print_id is null then 'unparseable_package_id'
        else 'no_congress_print_identity'
    end as link_rule
from {{ ref('int_committee_prints__govinfo') }} as govinfo
left join (
    select distinct canonical_print_id
    from {{ ref('int_committee_prints__congress') }}
) as congress
  on congress.canonical_print_id = govinfo.canonical_print_id
