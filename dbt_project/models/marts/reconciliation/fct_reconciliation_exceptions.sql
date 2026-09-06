{{ config(tags=["reconciliation", "operations", "quality"]) }}

with bill_identities as (
    select
        source_record_id,
        bill_type::text as bill_type,
        bill_number::text as bill_number,
        congress::integer as bill_congress
    from {{ ref('int_bills__congress') }}
    union all
    select
        source_record_id,
        bill_type::text as bill_type,
        bill_number::text as bill_number,
        congress::integer as bill_congress
    from {{ ref('int_bills__govinfo') }}
),
unmatched as (
    select
        'bill' as entity,
        match_id::text as match_id,
        canonical_bill_id::text as canonical_id,
        matches.decision as match_decision,
        congress_source_record_id,
        govinfo_source_record_id,
        congress_raw_id,
        congress_raw_response_version_id,
        congress_endpoint_url,
        govinfo_raw_id,
        govinfo_raw_response_version_id,
        govinfo_endpoint_url,
        decision_rule,
        congress_title,
        govinfo_title,
        coalesce(congress_identity.bill_type, govinfo_identity.bill_type) as bill_type,
        coalesce(congress_identity.bill_number, govinfo_identity.bill_number) as bill_number,
        coalesce(congress_identity.bill_congress, govinfo_identity.bill_congress) as bill_congress,
        false as intrinsic_source_gap
    from {{ ref('fct_bill_matches') }} as matches
    left join bill_identities as congress_identity
      on congress_identity.source_record_id = matches.congress_source_record_id
    left join bill_identities as govinfo_identity
      on govinfo_identity.source_record_id = matches.govinfo_source_record_id
    where matches.decision in ('unmatched', 'rejected', 'ambiguous')

    union all

    select
        'committee_report' as entity,
        match_id::text as match_id,
        canonical_report_id::text as canonical_id,
        decision as match_decision,
        congress_source_record_id,
        govinfo_source_record_id,
        congress_raw_id,
        congress_raw_response_version_id,
        congress_endpoint_url,
        govinfo_raw_id,
        govinfo_raw_response_version_id,
        govinfo_endpoint_url,
        decision_rule,
        congress_title,
        govinfo_title,
        null::text as bill_type,
        null::text as bill_number,
        null::integer as bill_congress,
        false as intrinsic_source_gap
    from {{ ref('fct_committee_report_matches') }}
    where decision in ('unmatched', 'rejected', 'ambiguous')

    union all

    select
        'hearing' as entity,
        match_id::text as match_id,
        govinfo_source_record_id::text as canonical_id,
        decision as match_decision,
        congress_source_record_id,
        govinfo_source_record_id,
        congress_raw_id,
        congress_raw_response_version_id,
        congress_endpoint_url,
        govinfo_raw_id,
        govinfo_raw_response_version_id,
        govinfo_endpoint_url,
        decision_rule,
        null::text as congress_title,
        govinfo_title,
        null::text as bill_type,
        null::text as bill_number,
        null::integer as bill_congress,
        confirmed_source_gap as intrinsic_source_gap
    from {{ ref('fct_hearing_matches') }}
    where decision in ('unmatched', 'rejected', 'ambiguous')
),
reviews as (
    select
        nullif(entity::text, '') as entity,
        nullif(canonical_id::text, '') as canonical_id,
        nullif(disposition::text, '') as disposition,
        nullif(reviewer::text, '') as reviewer,
        nullif(reviewed_at::text, '')::timestamptz as reviewed_at,
        nullif(reason::text, '') as review_reason,
        nullif(expires_at::text, '')::timestamptz as expires_at,
        nullif(contract_version::text, '') as contract_version
    from {{ ref('reconciliation_exception_reviews') }}
),
source_gap_rules as (
    select
        nullif(entity::text, '') as entity,
        nullif(missing_source::text, '') as missing_source,
        nullif(congress_min::text, '')::integer as congress_min,
        nullif(congress_max::text, '')::integer as congress_max,
        nullif(disposition::text, '') as disposition,
        nullif(reviewer::text, '') as reviewer,
        nullif(reviewed_at::text, '')::timestamptz as reviewed_at,
        nullif(reason::text, '') as review_reason,
        nullif(expires_at::text, '')::timestamptz as expires_at,
        nullif(contract_version::text, '') as contract_version
    from {{ ref('reconciliation_source_gap_rules') }}
),
enriched_base as (
    select
        unmatched.*,
        case
            when unmatched.match_decision <> 'unmatched' then null
            when unmatched.congress_source_record_id is null
             and unmatched.govinfo_source_record_id is null then 'both'
            when unmatched.congress_source_record_id is null then 'congress'
            else 'govinfo'
        end as missing_source,
        case
            when unmatched.entity = 'bill'
             and unmatched.canonical_id ~ '^[^-]+-[0-9]+$'
            then split_part(unmatched.canonical_id, '-', 2)::integer
            when unmatched.entity = 'committee_report'
             and unmatched.canonical_id ~ '^[^-]+-[^-]+-[0-9]+$'
            then split_part(unmatched.canonical_id, '-', 3)::integer
            else null
        end as congress_number
    from unmatched
),
enriched as (
    select
        enriched_base.*,
        case
            when enriched_base.entity = 'committee_report'
             and enriched_base.govinfo_source_record_id is null
             and enriched_base.canonical_id like '%-%-%'
            then 'https://api.govinfo.gov/packages/CRPT-'
                || split_part(enriched_base.canonical_id, '-', 3)
                || split_part(enriched_base.canonical_id, '-', 1)
                || '/summary'
            when enriched_base.entity = 'hearing'
             and enriched_base.missing_source = 'congress'
             and enriched_base.canonical_id ~* '^CHRG-[0-9]+[hsj]hrg[0-9]+'
            then 'https://api.congress.gov/v3/hearing/'
                || (regexp_match(
                    lower(enriched_base.canonical_id),
                    '^chrg-([0-9]+)([hsj]hrg)([0-9]+)'
                ))[1]
                || '/'
                || case (regexp_match(
                    lower(enriched_base.canonical_id),
                    '^chrg-([0-9]+)([hsj]hrg)([0-9]+)'
                ))[2]
                    when 'hhrg' then 'house'
                    when 'shrg' then 'senate'
                    -- Congress.gov retains ``nochamber`` in its route even
                    -- though the canonical hearings table uses ``joint``.
                    when 'jhrg' then 'nochamber'
                   end
                || '/'
                || coalesce(
                    nullif(ltrim((regexp_match(
                        lower(enriched_base.canonical_id),
                        '^chrg-([0-9]+)([hsj]hrg)([0-9]+)'
                    ))[3], '0'), ''),
                    '0'
                )
                || '?format=json'
            when enriched_base.entity = 'bill'
             and enriched_base.missing_source = 'govinfo'
             and enriched_base.bill_type is not null
             and enriched_base.bill_number is not null
             and enriched_base.bill_congress between 103 and 119
            then 'https://api.govinfo.gov/packages/BILLS-'
                || enriched_base.bill_congress::text
                || enriched_base.bill_type
                || enriched_base.bill_number
                || case
                    when lower(enriched_base.bill_type) in ('s', 'sconres', 'sjres', 'sres') then 'is'
                    else 'ih'
                   end
                || '/summary'
            when enriched_base.entity = 'bill'
             and enriched_base.missing_source = 'congress'
             and enriched_base.bill_type is not null
             and enriched_base.bill_number is not null
             and enriched_base.bill_congress is not null
            then 'https://api.congress.gov/v3/bill/'
                || enriched_base.bill_congress::text
                || '/'
                || enriched_base.bill_type
                || '/'
                || enriched_base.bill_number
                || '?format=json'
            when enriched_base.entity = 'committee_report'
             and enriched_base.missing_source = 'congress'
             and enriched_base.canonical_id ~* '^([a-z]+)([0-9]+)-([0-9]+)-([0-9]+)e?$'
            then 'https://api.congress.gov/v3/committee-report/'
                || (regexp_match(
                    lower(enriched_base.canonical_id),
                    '^([a-z]+)([0-9]+)-([0-9]+)-([0-9]+)e?$'
                ))[4]
                || '/'
                || upper((regexp_match(
                    lower(enriched_base.canonical_id),
                    '^([a-z]+)([0-9]+)-([0-9]+)-([0-9]+)e?$'
                ))[1])
                || '/'
                || (regexp_match(
                    lower(enriched_base.canonical_id),
                    '^([a-z]+)([0-9]+)-([0-9]+)-([0-9]+)e?$'
                ))[2]
                || '?format=json'
            else null
        end as expected_missing_endpoint_url
    from enriched_base
),
needed_evidence_endpoints as (
    select distinct
        coalesce(
            expected_missing_endpoint_url,
            congress_endpoint_url,
            govinfo_endpoint_url
        ) as endpoint_url
    from enriched
    where coalesce(
        expected_missing_endpoint_url,
        congress_endpoint_url,
        govinfo_endpoint_url
    ) is not null
),
fetch_evidence as (
    /*
       The current-response archive is the authoritative success marker for
       an endpoint.  Joining it by its unique endpoint index avoids replaying
       the 150M+ row crawl_run_fetches event log merely to find the latest
       successful payload.  Failed attempts are retained separately so a
       failure newer than the current success still wins in the next CTE.
    */
    select
        responses.endpoint_url,
        'fetched' as outcome,
        200 as http_status,
        responses.fetched_at as observed_at
    from {{ source('raw', 'raw_responses') }} as responses
    join needed_evidence_endpoints using (endpoint_url)

    union all

    select
        failures.endpoint_url,
        'failed' as outcome,
        failures.http_status,
        failures.failed_at as observed_at
    from {{ source('raw', 'failed_requests') }} as failures
    join needed_evidence_endpoints using (endpoint_url)
),
latest_fetch_evidence as (
    select distinct on (endpoint_url)
        endpoint_url,
        outcome,
        http_status,
        observed_at
    from fetch_evidence
    order by endpoint_url, observed_at desc
),
resolved as (
    select
        enriched.*,
        -- For a missing source, only evidence for the constructed counterpart
        -- endpoint answers whether that source was probed.  Falling back to
        -- the existing source's URL makes an unattempted counterpart look
        -- successfully fetched (for example, a Congress list response with
        -- HTTP 200 masking a missing GovInfo package).
        coalesce(
            case when expected_missing_endpoint_url is not null then expected_fetch.outcome end,
            case when expected_missing_endpoint_url is null then direct_fetch.outcome end,
            'not_attempted'
        ) as latest_fetch_outcome,
        coalesce(
            case when expected_missing_endpoint_url is not null then expected_fetch.http_status end,
            case when expected_missing_endpoint_url is null then direct_fetch.http_status end
        ) as latest_fetch_http_status,
        coalesce(
            case when expected_missing_endpoint_url is not null then expected_fetch.observed_at end,
            case when expected_missing_endpoint_url is null then direct_fetch.observed_at end
        ) as latest_fetch_at,
        reviews.disposition,
        reviews.reviewer,
        reviews.reviewed_at,
        reviews.review_reason,
        reviews.expires_at,
        reviews.contract_version,
        source_gap_rules.disposition as source_gap_disposition,
        source_gap_rules.reviewer as source_gap_reviewer,
        source_gap_rules.reviewed_at as source_gap_reviewed_at,
        source_gap_rules.review_reason as source_gap_review_reason,
        source_gap_rules.expires_at as source_gap_expires_at,
        source_gap_rules.contract_version as source_gap_contract_version
    from enriched
    left join reviews
      on reviews.entity = enriched.entity
     and reviews.canonical_id = enriched.canonical_id
     and (reviews.expires_at is null or reviews.expires_at >= now())
    left join source_gap_rules
      on source_gap_rules.entity = enriched.entity
     and source_gap_rules.missing_source = enriched.missing_source
     and (source_gap_rules.congress_min is null or enriched.congress_number >= source_gap_rules.congress_min)
     and (source_gap_rules.congress_max is null or enriched.congress_number <= source_gap_rules.congress_max)
     and (source_gap_rules.expires_at is null or source_gap_rules.expires_at >= now())
    left join latest_fetch_evidence as direct_fetch
      on direct_fetch.endpoint_url = coalesce(enriched.congress_endpoint_url, enriched.govinfo_endpoint_url)
    left join latest_fetch_evidence as expected_fetch
      on expected_fetch.endpoint_url = enriched.expected_missing_endpoint_url
),
classified as (
    select
        resolved.*,
        coalesce(
            disposition,
            source_gap_disposition,
            case
                when intrinsic_source_gap then 'confirmed_source_gap'
                when entity in ('bill', 'hearing', 'committee_report')
                 and missing_source in ('govinfo', 'congress')
                 and latest_fetch_outcome = 'failed'
                 and latest_fetch_http_status = 404
                    then 'confirmed_source_gap'
                else null
            end
        ) as final_disposition,
        case
            when disposition is null
             and source_gap_disposition is null
             and intrinsic_source_gap
                then 'system'
            when disposition is null
             and source_gap_disposition is null
             and entity in ('bill', 'hearing', 'committee_report')
             and missing_source in ('govinfo', 'congress')
             and latest_fetch_outcome = 'failed'
             and latest_fetch_http_status = 404
                then 'system'
            else null
        end as evidence_reviewer,
        case
            when disposition is null
             and source_gap_disposition is null
             and intrinsic_source_gap
                then coalesce(latest_fetch_at, now())
            when disposition is null
             and source_gap_disposition is null
             and entity in ('bill', 'hearing', 'committee_report')
             and missing_source in ('govinfo', 'congress')
             and latest_fetch_outcome = 'failed'
             and latest_fetch_http_status = 404
                then latest_fetch_at
            else null
        end as evidence_reviewed_at,
        case
            when disposition is null
             and source_gap_disposition is null
             and intrinsic_source_gap
             and decision_rule = 'confirmed_source_gap_pre_104'
                then 'Congress.gov hearing coverage begins after the historical GovInfo CHRG package range.'
            when disposition is null
             and source_gap_disposition is null
             and intrinsic_source_gap
                then 'The expected Congress.gov hearing endpoint returned HTTP 404.'
            when disposition is null
             and source_gap_disposition is null
             and entity in ('bill', 'hearing', 'committee_report')
             and missing_source in ('govinfo', 'congress')
             and latest_fetch_outcome = 'failed'
             and latest_fetch_http_status = 404
                then 'Conventional counterpart package or detail probe returned HTTP 404.'
            else null
        end as evidence_review_reason,
        case
            when disposition is null
             and source_gap_disposition is null
             and intrinsic_source_gap
                then 'v1'
            when disposition is null
             and source_gap_disposition is null
             and entity in ('bill', 'hearing', 'committee_report')
             and missing_source in ('govinfo', 'congress')
             and latest_fetch_outcome = 'failed'
             and latest_fetch_http_status = 404
                then 'v1'
            else null
        end as evidence_contract_version
        ,case
            /* A source-side 404 is evidence about the probe, not a permanent
               assertion that the counterpart can never exist.  Re-probe it
               after a bounded TTL so late publication and corrected source
               indexes become visible to the next reconciliation build. */
            when disposition is null
             and source_gap_disposition is null
             and intrinsic_source_gap
                then coalesce(latest_fetch_at, now()) + interval '30 days'
            when disposition is null
             and source_gap_disposition is null
             and entity in ('bill', 'hearing', 'committee_report')
             and missing_source in ('govinfo', 'congress')
             and latest_fetch_outcome = 'failed'
             and latest_fetch_http_status = 404
                then latest_fetch_at + interval '30 days'
            else null
        end as evidence_expires_at
    from resolved
)

select
    {{ hash_to_uuid("entity || '|' || canonical_id") }} as exception_id,
    entity,
    match_id,
    canonical_id,
    missing_source,
    congress_number,
    match_decision,
    coalesce(final_disposition, 'pending') as review_disposition,
    case when final_disposition is null then 'open' else 'reviewed' end as exception_status,
    latest_fetch_outcome,
    latest_fetch_http_status,
    latest_fetch_at,
    congress_source_record_id,
    govinfo_source_record_id,
    congress_raw_id,
    congress_raw_response_version_id,
    congress_endpoint_url,
    govinfo_raw_id,
    govinfo_raw_response_version_id,
    govinfo_endpoint_url,
    expected_missing_endpoint_url,
    decision_rule,
    congress_title,
    govinfo_title,
    coalesce(reviewer, source_gap_reviewer, evidence_reviewer) as reviewer,
    coalesce(reviewed_at, source_gap_reviewed_at, evidence_reviewed_at) as reviewed_at,
    coalesce(review_reason, source_gap_review_reason, evidence_review_reason) as review_reason,
    coalesce(expires_at, source_gap_expires_at, evidence_expires_at) as expires_at,
    coalesce(contract_version, source_gap_contract_version, evidence_contract_version) as contract_version
from classified
