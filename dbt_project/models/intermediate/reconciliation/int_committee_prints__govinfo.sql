{{ config(tags=["committee_print_reconciliation", "committee_prints", "intermediate"]) }}

with parsed as (
    select
        raw_id,
        raw_response_version_id,
        endpoint_url,
        package_id,
        title,
        congress,
        {{ normalize_chamber('chamber') }} as chamber,
        session,
        pages,
        issued_at,
        last_modified,
        collection_code,
        /*
          CPRT package identifiers normally look like CPRT-119HPRT123. Older
          records append volume/part/OCR suffixes (for example ...64355O),
          which do not change the base jacket identity. WPRT is the published
          Ways and Means print type; malformed package identifiers remain
          explicit non-linkable records.
        */
        regexp_match(
            lower(package_id),
            '^(?:gpo-)?cprt-([0-9]+)([hsjw]prt)([0-9]+)'
        ) as package_parts
    from {{ ref('stg_govinfo__committee_prints_packages') }}
    /* The CPRT collection endpoint has occasionally returned GPO/GOVPUB
       package details. collectionCode is the source authority for this
       relation, so retain those raw rows elsewhere but exclude them here. */
    where upper(coalesce(collection_code, '')) = 'CPRT'
),
typed as (
    select
        raw_id,
        raw_response_version_id,
        endpoint_url,
        package_id,
        title,
        congress,
        chamber,
        session,
        pages,
        issued_at,
        last_modified,
        collection_code,
        package_parts,
        case
            when package_parts is null then null
            else package_parts[2]
                || coalesce(nullif(ltrim(package_parts[3], '0'), ''), '0')
                || '-'
                || package_parts[1]
        end as canonical_print_id,
        case
            when package_parts is null
             and lower(package_id) ~ '^(?:gpo-)?cprt-[0-9]+[a-z]+[0-9]+'
                then 'unsupported_package_type'
            when package_parts is null then 'unparseable_package_id'
            when package_parts[2] = 'hprt' then 'house'
            when package_parts[2] = 'sprt' then 'senate'
            when package_parts[2] = 'jprt' then 'joint'
            when package_parts[2] = 'wprt' then 'house'
            else 'unsupported_package_type'
        end as identity_status
    from parsed
),
ranked as (
    select
        *,
        row_number() over (
            /* Keep every malformed package independently reviewable. */
            partition by coalesce(canonical_print_id, package_id)
            order by last_modified desc nulls last, package_id desc
        ) as representative_rank
    from typed
)

select
    'govinfo:' || package_id as source_record_id,
    'govinfo' as source_name,
    raw_id as source_raw_id,
    raw_response_version_id as source_raw_response_version_id,
    endpoint_url as source_endpoint_url,
    canonical_print_id,
    package_id,
    package_parts[2] as print_type,
    nullif(ltrim(package_parts[3], '0'), '')::integer as print_jacketnumber,
    congress,
    chamber,
    session,
    pages,
    title,
    {{ normalize_match_text('title') }} as normalized_title,
    issued_at,
    last_modified as source_updated_at,
    collection_code,
    identity_status,
    representative_rank = 1 as is_representative_package
from ranked
