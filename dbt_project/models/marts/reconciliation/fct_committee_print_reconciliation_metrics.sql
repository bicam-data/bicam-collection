{{ config(tags=["committee_print_reconciliation", "committee_prints", "reconciliation", "quality", "marts"]) }}

with congress as (
    select count(*)::bigint as congress_print_count
    from {{ ref('int_committee_prints__congress') }}
),
govinfo as (
    select
        count(*)::bigint as govinfo_package_count,
        count(*) filter (where is_representative_package)::bigint as govinfo_representative_count,
        count(*) filter (where identity_status = 'unparseable_package_id')::bigint as unparseable_package_count,
        count(*) filter (where not is_representative_package)::bigint as duplicate_variant_package_count
    from {{ ref('bridge_govinfo__committee_print_packages') }}
),
matches as (
    select
        count(*) filter (where decision = 'accepted')::bigint as exact_match_count,
        count(*) filter (where congress_source_record_id is not null and govinfo_source_record_id is null)::bigint as congress_only_count,
        count(*) filter (where congress_source_record_id is null and govinfo_source_record_id is not null)::bigint as govinfo_only_representative_count,
        count(*) filter (where title_agrees is true)::bigint as exact_title_agreement_count
    from {{ ref('fct_committee_print_matches') }}
),
leakage as (
    select
        count(*) filter (where upper(coalesce(collection_code, '')) <> 'CPRT')::bigint as excluded_non_cprt_count
    from {{ ref('stg_govinfo__committee_prints_packages') }}
)

select
    {{ hash_to_uuid("'committee_print' || '|' || 'source_boundary_v1'") }} as metric_id,
    'committee_print' as entity,
    'congress' as authoritative_source,
    'source_boundary_v1' as contract_version,
    congress.congress_print_count,
    govinfo.govinfo_package_count,
    govinfo.govinfo_representative_count,
    matches.exact_match_count,
    matches.congress_only_count,
    matches.govinfo_only_representative_count,
    govinfo.unparseable_package_count,
    govinfo.duplicate_variant_package_count,
    leakage.excluded_non_cprt_count,
    matches.exact_title_agreement_count,
    coalesce(
        matches.exact_match_count::numeric / nullif(congress.congress_print_count, 0),
        0
    ) as exact_match_rate,
    /* This is a census/overlap metric, not a quality gate: CPRT includes
       historical publications and no-chamber/unsupported records that do not
       have a Congress.gov committee-print identity. */
    'informational_source_boundary_review' as quality_status,
    now() as observed_at
from congress, govinfo, matches, leakage
