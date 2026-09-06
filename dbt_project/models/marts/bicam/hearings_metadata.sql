{{ config(schema='bicam', materialized='table', tags=['bicam_public', 'hearings'], indexes=[{'columns': ['hearing_id'], 'unique': true}, {'columns': ['govinfo_package_id'], 'unique': true}]) }}

with ranked as (
    select
        identity_map.hearing_id,
        packages.package_id as govinfo_package_id,
        raw.response as package_json,
        packages.collection_code,
        row_number() over (
            partition by identity_map.hearing_id
            order by
                (packages.package_id !~* '(op|p)[0-9]') desc,
                packages.last_modified desc nulls last,
                packages.package_id
        ) as _rank
    from {{ ref('int_govinfo_hearing_identity_map') }} as identity_map
    join {{ ref('stg_govinfo__hearings_packages') }} as packages using (package_id)
    join {{ ref('int_raw_responses__current') }} as raw on raw.raw_id = packages.raw_id
)

select
    hearing_id,
    govinfo_package_id,
    nullif(package_json #>> '{suDocClassNumber}', '') as su_doc_class_number,
    coalesce(
        nullif(package_json #>> '{otherIdentifier,migrated-doc-id}', ''),
        nullif(package_json #>> '{migratedDocId}', '')
    ) as migrated_doc_id,
    coalesce(nullif(package_json #>> '{collectionCode}', ''), collection_code) as govinfo_collection_code
from ranked
where _rank = 1

