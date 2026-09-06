{{ config(
    materialized="table",
    tags=["govinfo_bills", "govinfo_committee_reports", "govinfo_hearings", "reconciliation", "quality", "identity_review"]
) }}

/*
   GovInfo repeats legislator references in ``members`` arrays at several
   payload levels.  Committee-report members were historically the only
   curated array, which meant bill-package sponsors/cosponsors and hearing
   committee members without a bioGuideId could not be linked.  Expand every
   members array below the current package/granule payloads and retain the
   immutable raw id, array hash, and ordinal as lineage for each reference.

   JSONPath recursive descent intentionally handles a future nested members
   array as well as the current root-level arrays.  The member object itself
   is retained so name-only references can be reviewed without re-fetching the
   source document.

   Congressional Directory granules are included as member metadata evidence;
   they use the same evidence-backed Bioguide resolution path as other
   GovInfo member references.
*/
with base as (
    select
        raw_id,
        endpoint_url,
        raw_response_version_id,
        category,
        node_type,
        response,
        nullif(response->>'dateIssued', '')::date as source_issued_at,
        nullif(response #>> '{packageId}', '') as package_id,
        nullif(response #>> '{granuleId}', '') as granule_id
    from {{ ref('int_raw_responses__current') }}
    where source = 'govinfo'
      and category in (
          'govinfo-bills',
          'govinfo-committee-reports',
          'govinfo-hearings',
          'govinfo-committee-prints',
          'govinfo-congressional-directories'
      )
      and node_type in (
          'govinfo_bill_package_detail',
          'govinfo_committee_report_granule_detail',
          'govinfo_hearing_granule_detail',
          'govinfo_committee_print_package_detail',
          'govinfo_committee_print_granule_detail',
          'govinfo_congressional_directory_granule_detail'
      )
), member_arrays as (
    select
        base.*,
        arrays.members_json,
        md5(arrays.members_json::text) as members_array_hash
    from base
    cross join lateral jsonb_path_query(base.response, '$.**.members')
        as arrays(members_json)
    where jsonb_typeof(arrays.members_json) = 'array'
), unnested as (
    select
        member_arrays.*,
        member.value as member_json,
        member.ordinality::integer as member_ordinal
    from member_arrays
    cross join lateral jsonb_array_elements(member_arrays.members_json)
        with ordinality as member(value, ordinality)
    where jsonb_typeof(member.value) = 'object'
), report_keys as (
    /* Preserve the key used by the existing committee-report override seed. */
    select
        package_id,
        granule_id,
        report_id
    from {{ ref('stg_govinfo__committeereports_granules') }}
    where package_id is not null
      and granule_id is not null
)
select
    unnested.raw_id,
    unnested.endpoint_url,
    unnested.raw_response_version_id,
    unnested.source_issued_at,
    unnested.category,
    unnested.node_type,
    unnested.package_id,
    unnested.granule_id,
    report_keys.report_id,
    unnested.members_array_hash,
    unnested.member_ordinal,
    unnested.raw_id::text
        || '|'
        || unnested.members_array_hash
        || '|'
        || unnested.member_ordinal::text as source_member_reference_key,
    case
        when report_keys.report_id is not null then
            report_keys.report_id
                || '|'
                || unnested.granule_id
                || '|'
                || md5(unnested.member_json::text)
        else null
    end as legacy_source_member_key,
    unnested.member_json,
    coalesce(
        nullif(trim(unnested.member_json->>'bioGuideId'), ''),
        nullif(trim(unnested.member_json->>'bioguideId'), ''),
        nullif(trim(unnested.member_json->>'bioguideID'), ''),
        nullif(trim(unnested.member_json->>'bioGuideID'), ''),
        nullif(trim(unnested.member_json->>'bioguide_id'), '')
    ) as bioguide_id_raw,
    nullif(trim(unnested.member_json #>> '{name,0,parsed}'), '') as parsed_name,
    nullif(trim(unnested.member_json->>'memberName'), '') as member_name_raw,
    nullif(trim(unnested.member_json #>> '{name,0,authority-fnf}'), '') as authority_fnf,
    nullif(trim(unnested.member_json #>> '{name,0,authority-lnf}'), '') as authority_lnf,
    nullif(trim(unnested.member_json #>> '{name,0,authority-other}'), '') as authority_other,
    nullif(trim(coalesce(
        unnested.member_json->>'authorityId',
        unnested.member_json->>'authorityID',
        unnested.member_json->>'authority_id'
    )), '') as authority_id,
    nullif(trim(coalesce(
        unnested.member_json->>'gpoId',
        unnested.member_json->>'gpoID',
        unnested.member_json->>'gpo_id'
    )), '') as gpo_id,
    upper(nullif(trim(unnested.member_json->>'state'), '')) as state,
    nullif(trim(unnested.member_json->>'chamber'), '') as chamber,
    nullif(trim(unnested.member_json->>'role'), '') as role,
    nullif(trim(unnested.member_json->>'party'), '') as party,
    nullif(trim(unnested.member_json->>'congress'), '')::integer as congress,
    case
        when upper(trim(unnested.member_json->>'role')) = 'SUBMITTEDBY'
            then 'submittedby'
        when unnested.category = 'govinfo-bills'
            then 'bill_member'
        when unnested.category = 'govinfo-hearings'
            then 'hearing_committee_member'
        when unnested.category = 'govinfo-congressional-directories'
            then 'member_metadata'
        else 'member_metadata'
    end as identity_context,
    /* GovInfo sometimes serializes a single list member as ``and Mr. X``
       when it is carrying forward a conjunction from the source document.
       Remove only a leading conjunction before looking for separators so
       those rows can resolve like any other one-person reference; genuine
       ``Mr. X and Ms. Y`` parents remain composite and reviewable. */
    regexp_replace(
        coalesce(unnested.member_json->>'memberName', '')
        || ' '
        || coalesce(unnested.member_json #>> '{name,0,parsed}', '')
        || ' '
        || coalesce(unnested.member_json #>> '{name,0,authority-fnf}', ''),
        '^\s*(and|or)\s+',
        '',
        'i'
    ) ~* '( and | & |;|, and |,\s+and\s+)'
        as multi_legislator_flag,
    case
        when coalesce(
            nullif(trim(unnested.member_json->>'bioGuideId'), ''),
            nullif(trim(unnested.member_json->>'bioguideId'), ''),
            nullif(trim(unnested.member_json->>'bioguideID'), ''),
            nullif(trim(unnested.member_json->>'bioGuideID'), ''),
            nullif(trim(unnested.member_json->>'bioguide_id'), '')
        ) is null then 'missing_source_identifier'
        when upper(trim(coalesce(
            nullif(trim(unnested.member_json->>'bioGuideId'), ''),
            nullif(trim(unnested.member_json->>'bioguideId'), ''),
            nullif(trim(unnested.member_json->>'bioguideID'), ''),
            nullif(trim(unnested.member_json->>'bioGuideID'), ''),
            nullif(trim(unnested.member_json->>'bioguide_id'), '')
        ))) ~ '^[A-Z][0-9]{6}$' then 'provided_valid'
        else 'provided_invalid'
    end as bioguide_resolution_status
from unnested
left join report_keys
  on report_keys.package_id = unnested.package_id
 and report_keys.granule_id = unnested.granule_id
