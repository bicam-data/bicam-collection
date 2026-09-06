{{ config(tags=["committee_print_reconciliation", "committee_prints", "intermediate"]) }}

with congress_ranked as (
    select
        congress.*,
        public_ids.public_print_id,
        row_number() over (
            partition by public_ids.public_print_id
            order by (congress.print_type <> 'nprt') desc,
                     congress.source_updated_at desc nulls last,
                     congress.source_record_id
        ) as public_identity_rank
    from {{ ref('int_committee_prints__congress') }} as congress
    join {{ ref('int_committee_print_public_id_map') }} as public_ids
      on public_ids.source_print_id = congress.canonical_print_id
),
congress_records as (
    select
        source_record_id,
        source_name,
        source_raw_id,
        source_raw_response_version_id,
        source_endpoint_url,
        public_print_id as canonical_print_id,
        print_type,
        print_jacketnumber,
        congress,
        chamber,
        title,
        normalized_title,
        citation,
        texts_count,
        source_updated_at
    from congress_ranked
    where public_identity_rank = 1
),
govinfo_records as (
    select *
    from {{ ref('int_committee_prints__govinfo') }}
    where is_representative_package
),
paired as (
    select
        congress.source_record_id as congress_source_record_id,
        congress.source_raw_id as congress_raw_id,
        congress.source_raw_response_version_id as congress_raw_response_version_id,
        congress.source_endpoint_url as congress_endpoint_url,
        congress.canonical_print_id as congress_canonical_print_id,
        congress.print_type as congress_print_type,
        congress.print_jacketnumber as congress_print_jacketnumber,
        congress.congress as congress_congress,
        congress.chamber as congress_chamber,
        congress.title as congress_title,
        congress.normalized_title as congress_normalized_title,
        congress.source_updated_at as congress_source_updated_at,
        govinfo.source_record_id as govinfo_source_record_id,
        govinfo.source_raw_id as govinfo_raw_id,
        govinfo.source_raw_response_version_id as govinfo_raw_response_version_id,
        govinfo.source_endpoint_url as govinfo_endpoint_url,
        govinfo.canonical_print_id as govinfo_canonical_print_id,
        govinfo.package_id as govinfo_package_id,
        govinfo.print_type as govinfo_print_type,
        govinfo.print_jacketnumber as govinfo_print_jacketnumber,
        govinfo.congress as govinfo_congress,
        govinfo.chamber as govinfo_chamber,
        govinfo.title as govinfo_title,
        govinfo.normalized_title as govinfo_normalized_title,
        govinfo.source_updated_at as govinfo_source_updated_at,
        govinfo.identity_status as govinfo_identity_status
    from congress_records as congress
    full outer join govinfo_records as govinfo
      on congress.canonical_print_id = govinfo.canonical_print_id
)

select
    {{ hash_to_uuid("coalesce(congress_source_record_id, '') || '|' || coalesce(govinfo_source_record_id, '')") }} as candidate_id,
    /* A deterministic source key keeps malformed GovInfo rows addressable
       without pretending they have a Congress.gov print identity. */
    coalesce(
        congress_canonical_print_id,
        govinfo_canonical_print_id,
        'govinfo-package:' || govinfo_package_id
    ) as canonical_print_id,
    congress_source_record_id,
    govinfo_source_record_id,
    congress_raw_id,
    congress_raw_response_version_id,
    congress_endpoint_url,
    govinfo_raw_id,
    govinfo_raw_response_version_id,
    govinfo_endpoint_url,
    govinfo_package_id,
    case
        when congress_source_record_id is not null and govinfo_source_record_id is not null
            then 'exact_canonical_identity'
        when congress_source_record_id is null and govinfo_identity_status = 'unparseable_package_id'
            then 'unparseable_govinfo_package_id'
        when congress_source_record_id is null then 'missing_congress_record'
        else 'missing_govinfo_record'
    end as candidate_rule,
    case
        when congress_source_record_id is null or govinfo_source_record_id is null then null
        when congress_normalized_title is null or govinfo_normalized_title is null then null
        else congress_normalized_title = govinfo_normalized_title
    end as title_agrees,
    congress_canonical_print_id is not null and govinfo_canonical_print_id is not null as has_exact_identity,
    case
        when congress_source_record_id is not null and govinfo_source_record_id is not null then 1.0
        else 0.0
    end as candidate_confidence,
    congress_canonical_print_id as congress_print_id,
    govinfo_canonical_print_id as govinfo_print_id,
    congress_print_type,
    govinfo_print_type,
    congress_print_jacketnumber,
    govinfo_print_jacketnumber,
    congress_congress,
    govinfo_congress,
    congress_chamber,
    govinfo_chamber,
    congress_title,
    govinfo_title,
    govinfo_identity_status
from paired
