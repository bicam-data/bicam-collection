{{ config(tags=["amendments", "congress", "intermediate"]) }}

with detail_records as (
    select
        'congress:' || amendment_id as source_record_id,
        amendment_id as canonical_amendment_id,
        raw_id as source_raw_id,
        endpoint_url as source_endpoint_url,
        raw_response_version_id as source_raw_response_version_id,
        amendment_type,
        amendment_number,
        congress,
        chamber,
        purpose,
        description,
        proposed_at,
        submitted_at,
        is_bill_amendment,
        is_treaty_amendment,
        is_amendment_amendment,
        actions_count,
        cosponsors_count,
        cosponsors_withdrawn_count,
        texts_count,
        2 as source_priority
    from {{ ref('stg_congress__amendment_detail') }}
),
-- Preserve the stable identity returned by amendment list pages when a
-- detail endpoint is unavailable.  List payloads do not expose the detail
-- counts or amendment relationships, so those fields remain explicit nulls
-- or zeroes rather than inferred values.
list_records as (
    select
        'congress:list:' || (item->>'url') as source_record_id,
        lower(nullif(item->>'type', '')) || nullif(item->>'number', '')
            || '-' || nullif(item->>'congress', '') as canonical_amendment_id,
        responses.raw_id as source_raw_id,
        responses.endpoint_url as source_endpoint_url,
        responses.raw_response_version_id as source_raw_response_version_id,
        lower(nullif(item->>'type', '')) as amendment_type,
        nullif(item->>'number', '') as amendment_number,
        nullif(item->>'congress', '')::integer as congress,
        null::text as chamber,
        nullif(item->>'purpose', '') as purpose,
        null::text as description,
        null::timestamptz as proposed_at,
        null::timestamptz as submitted_at,
        false as is_bill_amendment,
        false as is_treaty_amendment,
        false as is_amendment_amendment,
        0 as actions_count,
        0 as cosponsors_count,
        0 as cosponsors_withdrawn_count,
        0 as texts_count,
        1 as source_priority
    from {{ ref('int_raw_responses__current') }} as responses
    cross join lateral jsonb_array_elements(coalesce(responses.response->'amendments', '[]'::jsonb)) as entries(item)
    where responses.source = 'congress'
      and responses.category = 'amendments'
      and responses.node_type = 'amendment_list'
      and nullif(item->>'url', '') is not null
),
ranked as (
    select
        *,
        row_number() over (
            partition by canonical_amendment_id
            order by source_priority desc, source_endpoint_url, source_record_id
        ) as canonical_rank
    from (
        select * from detail_records
        union all
        select * from list_records
    ) as records
)

select
    source_record_id,
    canonical_amendment_id,
    source_raw_id,
    source_endpoint_url,
    source_raw_response_version_id,
    amendment_type,
    amendment_number,
    congress,
    chamber,
    purpose,
    description,
    proposed_at,
    submitted_at,
    is_bill_amendment,
    is_treaty_amendment,
    is_amendment_amendment,
    actions_count,
    cosponsors_count,
    cosponsors_withdrawn_count,
    texts_count
from ranked
where canonical_rank = 1
