{{ config(tags=["bill_reconciliation", "intermediate"]) }}

with congress_records as (
    select * from {{ ref('int_bills__congress') }}
),
govinfo_records as (
    select * from {{ ref('int_bills__govinfo') }}
),
candidate_pairs as (
    select
        congress.*,
        govinfo.source_record_id as paired_govinfo_source_record_id,
        govinfo.source_name as paired_govinfo_source_name,
        govinfo.source_raw_id as paired_govinfo_raw_id,
        govinfo.source_raw_response_version_id as paired_govinfo_raw_response_version_id,
        govinfo.source_endpoint_url as paired_govinfo_endpoint_url,
        govinfo.canonical_bill_id as paired_govinfo_canonical_bill_id,
        govinfo.bill_type as paired_govinfo_bill_type,
        govinfo.bill_number as paired_govinfo_bill_number,
        govinfo.congress as paired_govinfo_congress,
        govinfo.title as paired_govinfo_title,
        govinfo.normalized_title as paired_govinfo_normalized_title,
        govinfo.short_title as paired_govinfo_short_title,
        govinfo.normalized_short_title as paired_govinfo_normalized_short_title,
        govinfo.origin_chamber as paired_govinfo_origin_chamber,
        govinfo.issued_at as paired_govinfo_issued_at,
        govinfo.source_updated_at as paired_govinfo_source_updated_at
    from congress_records as congress
    full outer join govinfo_records as govinfo
        on congress.canonical_bill_id = govinfo.canonical_bill_id
)

select
    {{ hash_to_uuid("coalesce(pairs.source_record_id, '') || '|' || coalesce(pairs.paired_govinfo_source_record_id, '')") }} as candidate_id,
    coalesce(pairs.canonical_bill_id, pairs.paired_govinfo_canonical_bill_id) as canonical_bill_id,
    pairs.source_record_id as congress_source_record_id,
    pairs.paired_govinfo_source_record_id as govinfo_source_record_id,
    pairs.source_raw_id as congress_raw_id,
    pairs.source_raw_response_version_id as congress_raw_response_version_id,
    pairs.source_endpoint_url as congress_endpoint_url,
    pairs.paired_govinfo_raw_id as govinfo_raw_id,
    pairs.paired_govinfo_raw_response_version_id as govinfo_raw_response_version_id,
    pairs.paired_govinfo_endpoint_url as govinfo_endpoint_url,
    case
        when pairs.source_record_id is not null and pairs.paired_govinfo_source_record_id is not null
         and pairs.source_record_id like 'congress:list:%'
            then 'exact_canonical_identity_list_fallback'
        when pairs.source_record_id is not null and pairs.paired_govinfo_source_record_id is not null
            then 'exact_canonical_identity'
        when pairs.source_record_id is null then 'missing_congress_record'
        else 'missing_govinfo_record'
    end as candidate_rule,
    case
        when pairs.source_record_id is null or pairs.paired_govinfo_source_record_id is null then null
        when matched_title.normalized_title is not null then true
        when coalesce(
            pairs.paired_govinfo_normalized_title,
            pairs.paired_govinfo_normalized_short_title
        ) is null then null
        else false
    end as title_agrees,
    case
        when pairs.normalized_title is not null
         and pairs.normalized_title in (
            pairs.paired_govinfo_normalized_title,
            pairs.paired_govinfo_normalized_short_title
         ) then true
        when pairs.source_record_id is null or pairs.paired_govinfo_source_record_id is null then null
        when coalesce(
            pairs.paired_govinfo_normalized_title,
            pairs.paired_govinfo_normalized_short_title
        ) is null then null
        else false
    end as primary_title_agrees,
    case
        when pairs.source_record_id is null or pairs.paired_govinfo_source_record_id is null
            then 'not_applicable'
        when matched_title.title_source = 'selected_primary' then 'selected_primary'
        when matched_title.title_source = 'congress_title_variant' then 'congress_title_variant'
        when coalesce(
            pairs.paired_govinfo_normalized_title,
            pairs.paired_govinfo_normalized_short_title
        ) is null then 'no_comparable_govinfo_title'
        else 'no_exact_title_match'
    end as title_agreement_basis,
    matched_title.title as matched_congress_title,
    matched_title.title_type as matched_congress_title_type,
    matched_title.title_type_code as matched_congress_title_type_code,
    matched_title.govinfo_title_field,
    case
        when pairs.origin_chamber is not null
         and pairs.origin_chamber = pairs.paired_govinfo_origin_chamber then true
        when pairs.source_record_id is null or pairs.paired_govinfo_source_record_id is null then null
        else false
    end as origin_chamber_agrees,
    case
        when pairs.source_record_id is not null and pairs.paired_govinfo_source_record_id is not null then 1.0
        else 0.0
    end as candidate_confidence,
    pairs.title as congress_title,
    pairs.paired_govinfo_title as govinfo_title,
    pairs.paired_govinfo_short_title as govinfo_short_title
from candidate_pairs as pairs
left join lateral (
    select
        variants.*,
        case
            when variants.normalized_title = pairs.paired_govinfo_normalized_title then 'title'
            else 'short_title'
        end as govinfo_title_field
    from {{ ref('int_bill_title_variants') }} as variants
    where variants.canonical_bill_id = coalesce(
        pairs.canonical_bill_id,
        pairs.paired_govinfo_canonical_bill_id
    )
      and variants.normalized_title in (
          pairs.paired_govinfo_normalized_title,
          pairs.paired_govinfo_normalized_short_title
      )
    order by
        (variants.normalized_title = pairs.paired_govinfo_normalized_title) desc,
        (variants.title_source = 'selected_primary') desc,
        variants.updated_at desc nulls last,
        variants.title_type_code nulls last
    limit 1
) as matched_title on true
