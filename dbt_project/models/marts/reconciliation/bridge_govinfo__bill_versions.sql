{{ config(
    tags=["bill_reconciliation", "reconciliation", "marts"],
    indexes=[
        {"columns": ["package_id"], "unique": true},
        {"columns": ["canonical_bill_id"]}
    ]
) }}

with packages as (
    select
        package_id,
        'govinfo:' || package_id as govinfo_source_record_id,
        raw_id as govinfo_raw_id,
        raw_response_version_id as govinfo_raw_response_version_id,
        endpoint_url as govinfo_endpoint_url,
        {{ bill_canonical_id('bill_type', 'bill_number', 'congress') }} as canonical_bill_id,
        bill_type,
        bill_number,
        congress,
        bill_version,
        title as package_title,
        {{ normalize_match_text('title') }} as normalized_package_title,
        short_title as package_short_title,
        {{ normalize_match_text('short_title') }} as normalized_package_short_title,
        issued_at,
        last_modified
    from {{ ref('stg_govinfo__bills_packages') }}
),
congress_bills as (
    select
        canonical_bill_id,
        source_record_id as congress_source_record_id,
        source_raw_id as congress_raw_id,
        source_raw_response_version_id as congress_raw_response_version_id,
        source_endpoint_url as congress_endpoint_url
    from {{ ref('int_bills__congress') }}
),
selected_govinfo_packages as (
    select source_record_id
    from {{ ref('int_bills__govinfo') }}
)

select
    {{ hash_to_uuid("'govinfo_bill_version|' || packages.package_id") }} as bill_version_bridge_id,
    packages.package_id,
    packages.govinfo_source_record_id,
    packages.canonical_bill_id,
    packages.bill_type,
    packages.bill_number,
    packages.congress,
    packages.bill_version,
    packages.package_title,
    packages.package_short_title,
    matched_title.title as matched_congress_title,
    matched_title.title_type as matched_congress_title_type,
    matched_title.title_type_code as matched_congress_title_type_code,
    matched_title.govinfo_title_field,
    matched_title.normalized_title is not null as title_agrees,
    case
        when matched_title.title_source = 'selected_primary' then 'selected_primary'
        when matched_title.title_source = 'congress_title_variant' then 'congress_title_variant'
        when coalesce(
            packages.normalized_package_title,
            packages.normalized_package_short_title
        ) is null then 'no_comparable_govinfo_title'
        else 'no_exact_title_match'
    end as title_agreement_basis,
    selected_govinfo_packages.source_record_id is not null as is_representative_package,
    packages.issued_at,
    packages.last_modified,
    packages.govinfo_raw_id,
    packages.govinfo_raw_response_version_id,
    packages.govinfo_endpoint_url,
    congress_bills.congress_source_record_id,
    congress_bills.congress_raw_id,
    congress_bills.congress_raw_response_version_id,
    congress_bills.congress_endpoint_url,
    case
        when congress_bills.congress_source_record_id is not null then 'accepted'
        else 'unmatched'
    end as link_decision,
    case
        when congress_bills.congress_source_record_id is not null then 'exact_canonical_bill_identity'
        else 'missing_congress_bill_identity'
    end as link_rule
from packages
left join congress_bills using (canonical_bill_id)
left join selected_govinfo_packages
  on selected_govinfo_packages.source_record_id = packages.govinfo_source_record_id
left join lateral (
    select
        variants.*,
        case
            when variants.normalized_title = packages.normalized_package_title then 'title'
            else 'short_title'
        end as govinfo_title_field
    from {{ ref('int_bill_title_variants') }} as variants
    where variants.canonical_bill_id = packages.canonical_bill_id
      and variants.normalized_title in (
          packages.normalized_package_title,
          packages.normalized_package_short_title
      )
    order by
        (variants.normalized_title = packages.normalized_package_title) desc,
        (variants.title_source = 'selected_primary') desc,
        variants.updated_at desc nulls last,
        variants.title_type_code nulls last
    limit 1
) as matched_title on true
