{{ config(tags=["govinfo_committee_reports", "staging"]) }}

with granules as (
    select
        report_id,
        report_set_id,
        package_id,
        granule_id
    from {{ ref('stg_govinfo__committeereports_granules') }}
),
granule_detail as (
    select response as granule_json
    from {{ ref('int_raw_responses__current') }}
    where source = 'govinfo'
      and category = 'govinfo-committee-reports'
      and node_type = 'govinfo_committee_report_granule_detail'
),
unnested as (
    select
        granules.report_id,
        granules.report_set_id,
        granules.package_id,
        granules.granule_id,
        member.value as member_json
    from granules
    join granule_detail
      on granules.package_id = {{ json_text('granule_json', 'packageId') }}
     and granules.granule_id = {{ json_text('granule_json', 'granuleId') }}
    cross join lateral {{ unnest_array('granule_json', 'members', 'member') }}
),
normalized as (
    select
        report_id,
        report_set_id,
        package_id,
        granule_id,
        member_json,
        report_id || '|' || granule_id || '|' || md5(member_json::text) as source_member_key,
        upper(coalesce(
            nullif(trim({{ json_text('member_json', 'bioGuideId') }}), ''),
            nullif(trim({{ json_text('member_json', 'bioguideId') }}), ''),
            nullif(trim({{ json_text('member_json', 'bioguideID') }}), ''),
            nullif(trim({{ json_text('member_json', 'bioGuideID') }}), ''),
            nullif(trim({{ json_text('member_json', 'bioguide_id') }}), '')
        )) as bioguide_id_provided,
        coalesce(
            nullif(trim({{ json_text('member_json', 'bioGuideId') }}), ''),
            nullif(trim({{ json_text('member_json', 'bioguideId') }}), ''),
            nullif(trim({{ json_text('member_json', 'bioguideID') }}), ''),
            nullif(trim({{ json_text('member_json', 'bioGuideID') }}), ''),
            nullif(trim({{ json_text('member_json', 'bioguide_id') }}), '')
        ) as bioguide_id_raw,
        {{ json_text('member_json', 'name.0.parsed') }} as parsed_name,
        {{ json_text('member_json', 'memberName') }} as member_name_raw,
        {{ normalize_match_text("coalesce(" ~ json_text('member_json', 'name.0.authority-lnf') ~ ", " ~ json_text('member_json', 'memberName') ~ ", " ~ json_text('member_json', 'name.0.authority-fnf') ~ ")") }} as member_name_normalized,
        {{ json_text('member_json', 'name.0.authority-fnf') }} as direct_order_name,
        {{ json_text('member_json', 'name.0.authority-lnf') }} as inverted_order_name,
        {{ json_text('member_json', 'name.0.authority-other') }} as other_name,
        {{ json_text('member_json', 'authorityId') }} as authority_id,
        {{ json_text('member_json', 'gpoId') }} as gpo_id,
        upper(nullif(trim({{ json_text('member_json', 'state') }}), '')) as state,
        {{ json_text('member_json', 'role') }} as role,
        {{ json_text('member_json', 'party') }} as party,
        {{ json_text('member_json', 'chamber') }} as chamber,
        {{ json_int('member_json', 'congress') }} as congress,
        case
            when upper(trim({{ json_text('member_json', 'role') }})) = 'SUBMITTEDBY'
                then 'submittedby'
            else 'member_metadata'
        end as identity_context,
        case
            when {{ json_text('member_json', 'memberName') }} ~* '( and | & |;|, and )'
                or {{ json_text('member_json', 'name.0.parsed') }} ~* '( and | & |;|, and )'
                then true
            else false
        end as multi_legislator_flag
    from unnested
),
with_overrides as (
    select
        normalized.*,
        overrides.bioguide_id::text as override_bioguide_id
    from normalized
    left join {{ ref('govinfo_member_identity_overrides') }} as overrides
     on overrides.source_member_key::text = normalized.source_member_key
     and (overrides.expires_at is null or overrides.expires_at::text::date >= current_date)
), with_links as (
    /*
       Keep this source-facing model useful for joins without rewriting the
       raw/source value.  The all-category bridge is built from immutable raw
       payloads, so it can safely provide deterministic inferred IDs back to
       this committee-report view.  Composite parents and ambiguous names
       remain null here; use the component bridge for the former.
    */
    select
        with_overrides.*,
        refs.resolved_bioguide_id as resolved_bioguide_id,
        refs.linkable_bioguide_id as linkable_bioguide_id,
        refs.resolution_status as link_resolution_status
    from with_overrides
    left join {{ ref('bridge_govinfo__member_references') }} as refs
      on refs.legacy_source_member_key = with_overrides.source_member_key
)

select
    source_member_key,
    report_id,
    report_set_id,
    package_id,
    granule_id,
    coalesce(override_bioguide_id, bioguide_id_provided, linkable_bioguide_id) as bioguide_id,
    bioguide_id_raw,
    parsed_name,
    member_name_raw,
    member_name_normalized,
    direct_order_name,
    inverted_order_name,
    other_name,
    authority_id,
    gpo_id,
    state,
    role,
    party,
    chamber,
    congress,
    identity_context,
    multi_legislator_flag,
    case
        when override_bioguide_id is not null then 'manual_override'
        when multi_legislator_flag then 'multi_legislator_review'
        when bioguide_id_provided is null then 'missing_source_identifier'
        when bioguide_id_provided ~ '^[A-Z][0-9]{6}$' then 'provided_valid'
        else 'provided_invalid'
    end as bioguide_resolution_status,
    resolved_bioguide_id,
    linkable_bioguide_id,
    bioguide_id_provided as source_bioguide_id,
    link_resolution_status
from with_links
