{{ config(tags=["amendments", "staging"]) }}

with detail as (
    select
        id as raw_id,
        endpoint_url,
        raw_response_version_id,
        response -> 'amendment' as amendment_json
    from {{ source('raw', 'raw_responses') }}
    where source = 'congress'
      and category = 'amendments'
      and node_type = 'amendment_detail'
)

select
    raw_id,
    endpoint_url,
    raw_response_version_id,
    {{ build_custom_id([
        "lower(" ~ json_text('amendment_json', 'type') ~ ")",
        json_text('amendment_json', 'number')
    ]) }} || '-' || coalesce((nullif(amendment_json #>> '{congress}', ''))::text, '') as amendment_id,
    lower({{ json_text('amendment_json', 'type') }}) as amendment_type,
    {{ json_text('amendment_json', 'number') }} as amendment_number,
    {{ json_int('amendment_json', 'congress') }} as congress,
    lower({{ json_text('amendment_json', 'chamber') }}) as chamber,
    {{ json_text('amendment_json', 'purpose') }} as purpose,
    {{ json_text('amendment_json', 'description') }} as description,
    {{ json_timestamptz('amendment_json', 'proposedDate') }} as proposed_at,
    {{ json_timestamptz('amendment_json', 'submittedDate') }} as submitted_at,
    case
        when amendment_json #> '{amendedBill}' is not null
         and coalesce(amendment_json #>> '{amendedBill,number}', '') <> ''
        then true
        else false
    end as is_bill_amendment,
    case
        when amendment_json #> '{amendedTreaty}' is not null
         and coalesce(amendment_json #>> '{amendedTreaty,treatyNumber}', '') <> ''
        then true
        else false
    end as is_treaty_amendment,
    case
        when amendment_json #> '{amendedAmendment}' is not null
         and coalesce(amendment_json #>> '{amendedAmendment,number}', '') <> ''
        then true
        else false
    end as is_amendment_amendment,
    {{ json_int('amendment_json', 'actions.count', 0) }} as actions_count,
    {{ json_int('amendment_json', 'cosponsors.count', 0) }} as cosponsors_count,
    greatest(
        {{ json_int('amendment_json', 'cosponsors.countIncludingWithdrawnCosponsors', 0) }}
        - {{ json_int('amendment_json', 'cosponsors.count', 0) }},
        0
    ) as cosponsors_withdrawn_count,
    {{ json_int('amendment_json', 'textVersions.count', 0) }} as texts_count
from detail
