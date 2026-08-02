{{ config(tags=["bill_reconciliation", "intermediate"]) }}

with congress_records as (
    select * from {{ ref('int_bills__congress') }}
),
govinfo_records as (
    select * from {{ ref('int_bills__govinfo') }}
)

select
    {{ hash_to_uuid("coalesce(congress.source_record_id, '') || '|' || coalesce(govinfo.source_record_id, '')") }} as candidate_id,
    coalesce(congress.canonical_bill_id, govinfo.canonical_bill_id) as canonical_bill_id,
    congress.source_record_id as congress_source_record_id,
    govinfo.source_record_id as govinfo_source_record_id,
    congress.source_raw_id as congress_raw_id,
    congress.source_raw_response_version_id as congress_raw_response_version_id,
    congress.source_endpoint_url as congress_endpoint_url,
    govinfo.source_raw_id as govinfo_raw_id,
    govinfo.source_raw_response_version_id as govinfo_raw_response_version_id,
    govinfo.source_endpoint_url as govinfo_endpoint_url,
    case
        when congress.source_record_id is not null and govinfo.source_record_id is not null
            then 'exact_canonical_identity'
        when congress.source_record_id is null then 'missing_congress_record'
        else 'missing_govinfo_record'
    end as candidate_rule,
    case
        when congress.normalized_title is not null
         and congress.normalized_title = govinfo.normalized_title then true
        when congress.source_record_id is null or govinfo.source_record_id is null then null
        else false
    end as title_agrees,
    case
        when congress.origin_chamber is not null
         and congress.origin_chamber = govinfo.origin_chamber then true
        when congress.source_record_id is null or govinfo.source_record_id is null then null
        else false
    end as origin_chamber_agrees,
    case
        when congress.source_record_id is not null and govinfo.source_record_id is not null then 1.0
        else 0.0
    end as candidate_confidence,
    congress.title as congress_title,
    govinfo.title as govinfo_title
from congress_records as congress
full outer join govinfo_records as govinfo
    on congress.canonical_bill_id = govinfo.canonical_bill_id
