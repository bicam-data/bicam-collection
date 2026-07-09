{{ config(tags=["bills", "staging"]) }}

with relatedbill_responses as (
    {{ resolve_child('bill_detail', 'bill_relatedbills', 'bills') }}
),
bill_detail as (
    select
        raw_id,
        bill_id
    from {{ ref('stg_congress__bill_detail') }}
),
unnested_relatedbills as (
    select
        bill_detail.bill_id,
        relatedbill_item.value as relatedbill_json
    from relatedbill_responses
    join bill_detail
      on relatedbill_responses.parent_raw_id = bill_detail.raw_id
    cross join lateral {{ unnest_array('response', 'relatedBills', 'relatedbill_item') }}
),
unnested_relationships as (
    select
        bill_id,
        relatedbill_json,
        relationship_item.value as relationship_json
    from unnested_relatedbills
    cross join lateral {{ unnest_array('relatedbill_json', 'relationshipDetails', 'relationship_item') }}
)

select
    bill_id,
    {{ build_custom_id([
        "lower(nullif(relatedbill_json #>> '{type}', ''))",
        "relatedbill_json #>> '{number}'"
    ]) }} || '-' || coalesce(relatedbill_json #>> '{congress}', '') as relatedbill_id,
    lower(nullif(relatedbill_json #>> '{type}', '')) as relatedbill_type,
    (nullif(relatedbill_json #>> '{number}', ''))::integer as relatedbill_number,
    (nullif(relatedbill_json #>> '{congress}', ''))::integer as congress,
    nullif(relationship_json #>> '{type}', '') as relationship_type,
    nullif(relationship_json #>> '{identifiedBy}', '') as relationship_identified_by
from unnested_relationships
