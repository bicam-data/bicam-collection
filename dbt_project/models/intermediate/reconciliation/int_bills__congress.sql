{{ config(tags=["bill_reconciliation", "intermediate"]) }}

with detail_records as (
    select
        'congress:' || bill_id as source_record_id,
        'congress' as source_name,
        raw_id as source_raw_id,
        raw_response_version_id as source_raw_response_version_id,
        endpoint_url as source_endpoint_url,
        {{ bill_canonical_id('bill_type', 'bill_number', 'congress') }} as canonical_bill_id,
        bill_type,
        bill_number,
        congress,
        title,
        {{ normalize_match_text('title') }} as normalized_title,
        {{ normalize_chamber('origin_chamber') }} as origin_chamber,
        introduced_at,
        updated_at as source_updated_at,
        2 as source_priority
    from {{ ref('stg_congress__bill_detail') }}
),
-- A detail endpoint can be unavailable even though its complete identity and
-- title were returned by a successfully harvested bill-list page.  Retain
-- that list item as a lower-priority source record so reconciliation can use
-- it without claiming that the detail request succeeded.  The raw list
-- response remains the provenance for this fallback record.
list_records as (
    select
        'congress:list:' || (item->>'url') as source_record_id,
        'congress' as source_name,
        raw_id as source_raw_id,
        raw_response_version_id as source_raw_response_version_id,
        endpoint_url as source_endpoint_url,
        {{ bill_canonical_id(
            "lower(nullif(item->>'type', ''))",
            normalize_congress_bill_number_text("nullif(item->>'number', '')"),
            "nullif(item->>'congress', '')::integer"
        ) }} as canonical_bill_id,
        lower(nullif(item->>'type', '')) as bill_type,
        {{ normalize_congress_bill_number_text("nullif(item->>'number', '')") }} as bill_number,
        nullif(item->>'congress', '')::integer as congress,
        nullif(item->>'title', '') as title,
        {{ normalize_match_text("nullif(item->>'title', '')") }} as normalized_title,
        {{ normalize_chamber("nullif(item->>'originChamber', '')") }} as origin_chamber,
        nullif(item->>'introducedDate', '')::date as introduced_at,
        nullif(item->>'updateDate', '')::timestamptz as source_updated_at,
        1 as source_priority
    from {{ ref('int_raw_responses__current') }} as responses
    cross join lateral jsonb_array_elements(coalesce(responses.response->'bills', '[]'::jsonb)) as entries(item)
    where responses.source = 'congress'
      and responses.category = 'bills'
      and responses.node_type = 'bill_list'
      and nullif(item->>'url', '') is not null
),
ranked as (
    select
        *,
        row_number() over (
            partition by canonical_bill_id
            order by
                (title is not null and nullif(title, '') is not null) desc,
                source_priority desc,
                source_updated_at desc nulls last,
                source_endpoint_url,
                source_record_id
        ) as canonical_rank
    from (
        select * from detail_records
        union all
        select * from list_records
    ) as records
)

select
    source_record_id,
    source_name,
    source_raw_id,
    source_raw_response_version_id,
    source_endpoint_url,
    canonical_bill_id,
    bill_type,
    bill_number,
    congress,
    title,
    normalized_title,
    origin_chamber,
    introduced_at,
    source_updated_at
from ranked
where canonical_rank = 1
