{{ config(tags=["amendments", "staging"]) }}

with child_amendments as (
    select
        parent_raw_id,
        lower(amendment_type) as amendment_type,
        amendment_number,
        congress
    from {{ ref('stg_congress__amendment_amendments_to_amendment') }}
),
parent_amendments as (
    select
        raw_id,
        amendment_id
    from {{ ref('stg_congress__amendment_detail') }}
)

select
    parent_amendments.amendment_id,
    {{ build_custom_id([
        "child_amendments.amendment_type",
        "child_amendments.amendment_number"
    ]) }} || '-' || child_amendments.congress::text as amending_amendment_id,
    child_amendments.amendment_type,
    child_amendments.amendment_number,
    child_amendments.congress
from child_amendments
inner join parent_amendments
    on child_amendments.parent_raw_id = parent_amendments.raw_id
