{{ config(schema='bicam', materialized='table', tags=['bicam_public', 'committee_prints'], indexes=[{'columns': ['print_id'], 'unique': true}, {'columns': ['govinfo_package_id'], 'unique': true}]) }}

with representative_packages as (
    select distinct on (canonical_print_id)
        canonical_print_id as print_id,
        package_id as govinfo_package_id,
        source_raw_id as govinfo_raw_id
    from {{ ref('int_committee_prints__govinfo') }}
    where is_representative_package
      and canonical_print_id is not null
      and package_id is not null
    order by canonical_print_id, source_updated_at desc nulls last, package_id
), payloads as (
    select
        matches.print_id,
        matches.govinfo_package_id,
        raw.response as package_json
    from representative_packages as matches
    join {{ ref('int_raw_responses__current') }} as raw
      on raw.raw_id = matches.govinfo_raw_id
)
select
    print_id,
    govinfo_package_id,
    nullif(package_json #>> '{suDocClassNumber}', '') as su_doc_class_number,
    nullif(package_json #>> '{otherIdentifier,migrated-doc-id}', '') as migrated_doc_id,
    nullif(package_json #>> '{collectionCode}', '') as govinfo_collection_code
from payloads
