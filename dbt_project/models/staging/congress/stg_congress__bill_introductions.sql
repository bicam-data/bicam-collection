{{ config(tags=["bills", "historical", "staging", "operations"]) }}

with detail as (
    select
        raw_id,
        endpoint_url,
        raw_response_version_id,
        response -> 'bill' as bill_payload
    from {{ ref('int_raw_responses__current') }}
    where source = 'congress'
      and category = 'bills'
      and node_type = 'bill_detail'
),
expanded as (
    select
        detail.raw_id,
        detail.endpoint_url,
        detail.raw_response_version_id,
        variants.ordinality::integer as introduction_ordinal,
        jsonb_typeof(detail.bill_payload) = 'array' as is_historical_array,
        variants.bill_json
    from detail
    cross join lateral jsonb_array_elements(
        case
            when jsonb_typeof(detail.bill_payload) = 'array' then detail.bill_payload
            else jsonb_build_array(detail.bill_payload)
        end
    ) with ordinality as variants(bill_json, ordinality)
),
identified as (
    select
        *,
        lower(nullif(bill_json #>> '{type}', '')) as bill_type,
        {{ congress_bill_number('bill_json') }} as bill_number,
        nullif(bill_json #>> '{congress}', '')::integer as congress,
        nullif(bill_json #>> '{introducedDate}', '')::date as introduced_at,
        nullif(bill_json #>> '{detailUrl}', '') as detail_url
    from expanded
)

select
    raw_id,
    endpoint_url,
    raw_response_version_id,
    {{ build_custom_id(["bill_type", "bill_number"]) }} || '-' || coalesce(congress::text, '') as bill_id,
    case
        when is_historical_array then
            {{ build_custom_id(["bill_type", "bill_number"]) }} || '-' || coalesce(congress::text, '')
            || '-'
            || substr(
                md5(coalesce(detail_url, introduced_at::text, introduction_ordinal::text)),
                1,
                12
            )
        else {{ build_custom_id(["bill_type", "bill_number"]) }} || '-' || coalesce(congress::text, '')
    end as introduction_id,
    introduction_ordinal,
    is_historical_array,
    bill_type,
    bill_number,
    -- Congress.gov documents that bill numbers were not standardized for the
    -- 6th–14th Congresses; the URL number is a locator, not an authoritative
    -- legislative number. Preserve it for navigation but expose its status.
    coalesce(congress > 14, false) as bill_number_authoritative,
    congress,
    introduced_at,
    detail_url,
    nullif(bill_json #>> '{title}', '') as title,
    nullif(bill_json #>> '{originChamber}', '') as origin_chamber,
    nullif(bill_json #>> '{originChamberCode}', '') as origin_chamber_code,
    nullif(bill_json #>> '{legislationUrl}', '') as legislation_url,
    nullif(bill_json #>> '{updateDate}', '')::timestamptz as updated_at
from identified
