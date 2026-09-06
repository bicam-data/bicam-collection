with representatives as (
    select
        canonical_bill_id,
        package_id,
        upper(bill_version) as bill_version
    from {{ ref('bridge_govinfo__bill_versions') }}
    where is_representative_package
), mismatches as (
    select
        representatives.canonical_bill_id,
        representatives.package_id,
        representatives.bill_version,
        bills.version_code,
        bills_metadata.govinfo_package_id
    from representatives
    left join {{ ref('bills') }} as bills
      on bills.bill_id = representatives.canonical_bill_id
    left join {{ ref('bills_metadata') }} as bills_metadata
      on bills_metadata.bill_id = representatives.canonical_bill_id
    where bills.bill_id is null
       or bills.version_code is distinct from representatives.bill_version
       or bills_metadata.govinfo_package_id is distinct from representatives.package_id
)
select * from mismatches
