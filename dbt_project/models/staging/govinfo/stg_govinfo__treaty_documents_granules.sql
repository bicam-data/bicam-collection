{{ config(tags=['govinfo_treaty_documents', 'staging']) }}

with granule_detail as (
    select
        raw_id,
        endpoint_url,
        raw_response_version_id,
        response as granule_json
    from {{ ref('int_raw_responses__current') }}
    where source = 'govinfo'
      and category = 'govinfo-treaty-documents'
      and node_type = 'govinfo_treaty_document_granule_detail'
),
typed as (
    select
        raw_id,
        endpoint_url,
        raw_response_version_id,
        {{ json_text('granule_json', 'packageId') }} as package_id,
        {{ json_text('granule_json', 'granuleId') }} as granule_id,
        upper({{ json_text('granule_json', 'docClass') }}) as document_class,
        {{ json_timestamptz('granule_json', 'lastModified') }} as last_modified
    from granule_detail
),
ranked as (
    select
        packages.treaty_id,
        typed.raw_id,
        typed.endpoint_url,
        typed.raw_response_version_id,
        typed.package_id,
        typed.granule_id,
        typed.document_class,
        typed.last_modified,
        row_number() over (
            partition by packages.treaty_id, typed.granule_id
            order by typed.last_modified desc nulls last, typed.raw_id
        ) as _rank
    from typed
    join {{ ref('stg_govinfo__treaty_documents_packages') }} as packages
      on packages.package_id = typed.package_id
    where typed.granule_id is not null
      and coalesce(typed.document_class, 'TDOC') = 'TDOC'
)

select
    raw_id,
    endpoint_url,
    raw_response_version_id,
    treaty_id,
    package_id,
    granule_id,
    document_class,
    last_modified
from ranked
where _rank = 1
