{{ config(schema='bicam', materialized='table', tags=['bills', 'public'], indexes=[{'columns': ['bill_id'], 'unique': true}, {'columns': ['govinfo_package_id'], 'unique': true}]) }}
with packages as (
    select
        p.package_id as govinfo_package_id,
        p.bill_type || p.bill_number::text || '-' || p.congress::text as bill_id,
        p.raw_id,
        p.last_modified
    from {{ ref('stg_govinfo__bills_packages') }} p
    join {{ ref('bills') }} b
      on b.bill_id = p.bill_type || p.bill_number::text || '-' || p.congress::text
),
ranked as (
    -- Keep the metadata package in lock-step with the representative package
    -- used by bills/version reconciliation.  Package IDs are not chronological
    -- (for example, an RDS package can sort after a later ENR package), so use
    -- the source's last-modified timestamp first and the package ID only as a
    -- deterministic tie-breaker.
    select *, row_number() over (
        partition by bill_id
        order by last_modified desc nulls last, govinfo_package_id desc
    ) as _rn
    from packages
),
payloads as (
    select r.*, raw.response as package_json
    from ranked r
    join {{ ref('int_raw_responses__current') }} raw on raw.raw_id = r.raw_id
    where r._rn = 1
)
select
    bill_id,
    govinfo_package_id,
    nullif(package_json #>> '{suDocClassNumber}', '') as su_doc_class_number,
    nullif(package_json #>> '{otherIdentifier,migrated-doc-id}', '') as migrated_doc_id,
    nullif(package_json #>> '{otherIdentifier,stock-number}', '') as stock_number,
    nullif(package_json #>> '{otherIdentifier,child-ils-system-id}', '') as child_ils_system_id,
    nullif(package_json #>> '{otherIdentifier,parent-ils-system-id}', '') as parent_ils_system_id,
    nullif(package_json #>> '{collectionCode}', '') as govinfo_collection_code
from payloads
