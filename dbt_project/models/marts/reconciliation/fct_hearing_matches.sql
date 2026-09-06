{{ config(tags=["hearing_reconciliation", "reconciliation", "marts", "quality"]) }}

with govinfo_base as (
    select
        raw_id as govinfo_raw_id,
        raw_response_version_id as govinfo_raw_response_version_id,
        endpoint_url as govinfo_endpoint_url,
        package_id,
        title as govinfo_title,
        congress as govinfo_congress,
        chamber as govinfo_chamber,
        regexp_match(lower(package_id), '^chrg-([0-9]+)([hsj]hrg)([0-9]+).*$') as package_parts
    from {{ ref('stg_govinfo__hearings_packages') }}
    where package_id is not null
      -- The GovInfo hearings collection also contains GOVPUB-* publications.
      -- They are valid ingestion records but have no CHRG hearing identity and
      -- must not enter the cross-source hearing reconciliation denominator.
      and package_id ~* '^CHRG-[0-9]+[hsj]hrg[0-9]+.*$'
),
govinfo as (
    select
        govinfo_base.*,
        {{ bicam_govinfo_hearing_id('package_id') }} as govinfo_hearing_id,
        coalesce(nullif(ltrim(package_parts[3], '0'), ''), '0') as normalized_jacket_number,
        case package_parts[2]
            when 'hhrg' then '/v3/hearing/' || package_parts[1] || '/house/' || coalesce(nullif(ltrim(package_parts[3], '0'), ''), '0')
            when 'shrg' then '/v3/hearing/' || package_parts[1] || '/senate/' || coalesce(nullif(ltrim(package_parts[3], '0'), ''), '0')
            -- Congress.gov serves joint hearings through its legacy
            -- ``nochamber`` route.  The public reconciliation chamber remains
            -- canonical ``joint``; this path is only for exact source-fetch
            -- evidence and must mirror the upstream API route.
            when 'jhrg' then '/v3/hearing/' || package_parts[1] || '/nochamber/' || coalesce(nullif(ltrim(package_parts[3], '0'), ''), '0')
        end as expected_congress_endpoint_path
    from govinfo_base
),
confirmed_404 as (
    select distinct
        -- failed_requests stores the absolute API URL, while the expected
        -- identity path below is deliberately host-neutral.  Normalize both
        -- forms before classifying a GovInfo-only hearing as a source gap.
        regexp_replace(
            regexp_replace(endpoint_url, '^https?://[^/]+', ''),
            '[?].*$',
            ''
        ) as endpoint_path
    from {{ source('raw', 'failed_requests') }}
    where category = 'hearings'
      and node_type = 'hearing_detail'
      and http_status = 404
),
congress as (
    select
        hearings.raw_id as congress_raw_id,
        hearings.raw_response_version_id as congress_raw_response_version_id,
        hearings.endpoint_url as congress_endpoint_url,
        hearings.hearing_id,
        public_ids.public_hearing_id,
        public_ids.format_package_id,
        public_ids.identity_rule as public_identity_rule,
        hearings.hearing_jacketnumber,
        hearings.congress,
        hearings.chamber,
        hearings.source_chamber,
        hearings.citation
    from {{ ref('int_hearings__congress') }} as hearings
    join {{ ref('int_congress_hearing_public_id_map') }} as public_ids
      on public_ids.source_hearing_id = hearings.hearing_id
),
committee_meeting_evidence as (
    select
        source_id as package_id,
        count(distinct target_id) as committee_meeting_evidence_count,
        array_agg(distinct target_id order by target_id) as committee_meeting_evidence_ids
    from {{ ref('int_hearing_committee_meeting_links') }}
    /*
       GovInfo hearing packages can connect to Congress.gov committee
       meetings through either the ordinary hearing identity path (HHRG/SHRG)
       or the explicit joint-package path (JHRG).  Keep both kinds of
       evidence; restricting this to JHRG silently discarded valid
       House/Senate meeting links even though the intermediate linker had
       already resolved them.
    */
    where relationship_type in (
        'govinfo_hearing_to_committee_meeting',
        'govinfo_joint_hearing_to_committee_meeting'
    )
    group by source_id
),
exact_ranked as (
    select
        g.package_id,
        c.*,
        /* Multiple Congress routes can alias the same exact CHRG package.
           They are source aliases for one entity, not multiple candidates. */
        1::bigint as candidate_match_count,
        row_number() over (
            partition by g.package_id
            order by
                case
                    when g.package_parts[2] = 'hhrg' and c.chamber = 'house' then 0
                    when g.package_parts[2] = 'shrg' and c.chamber = 'senate' then 0
                    when g.package_parts[2] = 'jhrg' and c.chamber = 'joint' then 0
                    else 1
                end,
                c.hearing_id,
                c.congress_raw_id
        ) as match_rank
    from govinfo as g
    join congress as c
      on g.package_parts is not null
     and c.public_hearing_id = g.govinfo_hearing_id
),
exact as (
    select * from exact_ranked where match_rank = 1
),
fallback_ranked as (
    -- The public hearing chamber remains the Congress.gov canonical value.
    -- Some GovInfo HHRG/SHRG packages have a unique Congress.gov record whose
    -- joint-committee evidence makes its chamber ``joint``. Preserve that
    -- source disagreement as provenance; do not manufacture chamber copies.
    select
        g.package_id,
        c.*,
        count(*) over (partition by g.package_id) as candidate_match_count,
        row_number() over (
            partition by g.package_id
            order by c.hearing_id, c.congress_raw_id
        ) as match_rank
    from govinfo as g
    join congress as c
      on g.package_parts is not null
     and c.congress = g.package_parts[1]::integer
     and c.hearing_jacketnumber::text = g.normalized_jacket_number
     -- Publication-package evidence supersedes Congress.gov's route jacket
     -- number. Only records without that evidence may use this fallback.
     and c.format_package_id is null
     and (
          (g.package_parts[2] = 'hhrg' and c.chamber in ('house', 'joint'))
       or (g.package_parts[2] = 'shrg' and c.chamber in ('senate', 'joint'))
       or (g.package_parts[2] = 'jhrg' and c.chamber = 'joint')
     )
    where not exists (
        select 1 from exact as e where e.package_id = g.package_id
    )
),
fallback as (
    select * from fallback_ranked where match_rank = 1
),
resolved as (
    select
        g.*,
        coalesce(e.hearing_id, f.hearing_id) as congress_hearing_id,
        coalesce(e.congress_raw_id, f.congress_raw_id) as congress_raw_id,
        coalesce(e.congress_raw_response_version_id, f.congress_raw_response_version_id)
            as congress_raw_response_version_id,
        coalesce(e.congress_endpoint_url, f.congress_endpoint_url) as congress_endpoint_url,
        coalesce(e.congress, f.congress) as congress,
        coalesce(e.chamber, f.chamber) as congress_chamber,
        coalesce(e.source_chamber, f.source_chamber) as congress_source_chamber,
        coalesce(e.citation, f.citation) as congress_citation,
        coalesce(e.candidate_match_count, f.candidate_match_count, 0) as candidate_match_count,
        coalesce(meeting.committee_meeting_evidence_count, 0) as committee_meeting_evidence_count,
        coalesce(meeting.committee_meeting_evidence_ids, array[]::text[]) as committee_meeting_evidence_ids,
        case
            when e.package_id is not null and e.candidate_match_count > 1
                then 'ambiguous_exact_hearing_identity'
            when f.package_id is not null and f.candidate_match_count > 1
                then 'ambiguous_jacket_number_fallback'
            when e.package_id is not null then 'exact_hearing_identity'
            when f.package_id is not null then 'jacket_number_fallback'
            when meeting.package_id is not null then 'committee_meeting_evidence_only'
            when missing.endpoint_path is not null then 'confirmed_source_gap_404'
            when g.package_parts is null then 'unmatched_unparseable_package_id'
            when (g.package_parts[1])::integer < 104 then 'confirmed_source_gap_pre_104'
            /* The complete modern Congress hearing collection contains no
               route whose publication-format evidence identifies this exact
               CHRG package. This is a current source-side absence, not a
               license to match an unrelated equal route jacket number. */
            else 'confirmed_source_gap_no_congress_package_evidence'
        end as decision_rule
    from govinfo as g
    left join exact as e on e.package_id = g.package_id
    left join fallback as f on f.package_id = g.package_id
    left join committee_meeting_evidence as meeting on meeting.package_id = g.package_id
    left join confirmed_404 as missing
      on missing.endpoint_path = g.expected_congress_endpoint_path
)

select
    md5(concat_ws('|', 'hearing', package_id)) as match_id,
    package_id as govinfo_source_record_id,
    govinfo_raw_id,
    govinfo_raw_response_version_id,
    govinfo_endpoint_url,
    congress_hearing_id as congress_source_record_id,
    congress_raw_id,
    congress_raw_response_version_id,
    congress_endpoint_url,
    congress,
    govinfo_congress,
    govinfo_title,
    /* The package prefix defines the public publication chamber. Congress
       route chamber remains available separately as source evidence. */
    case package_parts[2]
        when 'hhrg' then 'house'
        when 'shrg' then 'senate'
        when 'jhrg' then 'joint'
    end as chamber,
    govinfo_chamber,
    congress_chamber,
    congress_source_chamber,
    congress_citation,
    candidate_match_count,
    committee_meeting_evidence_count,
    committee_meeting_evidence_ids,
    decision_rule,
    case
        when candidate_match_count > 1 then 'ambiguous'
        when congress_hearing_id is not null then 'accepted'
        else 'unmatched'
    end as decision,
    case
        when candidate_match_count > 1 then 0.5
        when congress_hearing_id is not null then 1.0
        when decision_rule = 'committee_meeting_evidence_only' then 0.75
        when decision_rule in (
            'confirmed_source_gap_pre_104',
            'confirmed_source_gap_404',
            'confirmed_source_gap_no_congress_package_evidence'
        ) then 0.0
        else 0.25
    end::numeric as candidate_confidence,
    decision_rule in (
        'committee_meeting_evidence_only',
        'confirmed_source_gap_pre_104',
        'confirmed_source_gap_404',
        'confirmed_source_gap_no_congress_package_evidence'
    ) as confirmed_source_gap
from resolved
