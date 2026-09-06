{{ config(
    materialized="table",
    indexes=[
        {"columns": ["observation_key"], "unique": true},
        {"columns": ["bioguide_id", "sponsorship_type"]}
    ],
    tags=["members", "congress", "reconciliation", "child_state"]
) }}

/*
   Member sponsored/cosponsored-legislation endpoints are delta-capable child
   collections. Keep one source observation per selected array position before
   deriving bill or amendment relationship grains. This prevents the bill and
   amendment reciprocity audits from independently reading the latest URL and
   losing adjacent non-overlapping windows.
*/
with sponsored_responses as (
    {{ resolve_child('member_detail', 'member_sponsored_legislation', 'members') }}
), sponsored as (
    select
        sponsored_responses.*,
        'sponsor'::text as sponsorship_type,
        legislation.array_ordinal,
        legislation.value as legislation_json
    from sponsored_responses
    cross join lateral jsonb_array_elements(
        coalesce(sponsored_responses.response -> 'sponsoredLegislation', '[]'::jsonb)
    ) with ordinality as legislation(value, array_ordinal)
), cosponsored_responses as (
    {{ resolve_child('member_detail', 'member_cosponsored_legislation', 'members') }}
), cosponsored as (
    select
        cosponsored_responses.*,
        'cosponsor'::text as sponsorship_type,
        legislation.array_ordinal,
        legislation.value as legislation_json
    from cosponsored_responses
    cross join lateral jsonb_array_elements(
        coalesce(cosponsored_responses.response -> 'cosponsoredLegislation', '[]'::jsonb)
    ) with ordinality as legislation(value, array_ordinal)
), observations as (
    select * from sponsored
    union all
    select * from cosponsored
), joined as (
    select
        observations.raw_id as source_raw_id,
        observations.raw_response_version_id as source_raw_response_version_id,
        observations.latest_crawl_run_id as source_crawl_run_id,
        observations.parent_raw_id as member_detail_raw_id,
        observations.parent_endpoint_url as member_detail_endpoint_url,
        observations.endpoint_url as source_endpoint_url,
        observations.child_fetched_at as source_fetched_at,
        observations.page_offset as source_page_offset,
        observations.is_complete_generation as source_is_complete_generation,
        observations.generation_crawl_run_id as source_generation_crawl_run_id,
        observations.generation_page_count as source_generation_page_count,
        observations.generation_expected_count as source_generation_expected_count,
        observations.generation_finished_at as source_generation_finished_at,
        observations.sponsorship_type,
        observations.array_ordinal as source_ordinal,
        members.bioguide_id,
        {{ json_text('observations.legislation_json', 'type') }} as legislation_type,
        {{ json_text('observations.legislation_json', 'number') }} as legislation_number,
        {{ json_text('observations.legislation_json', 'amendmentNumber') }} as amendment_number,
        {{ json_int('observations.legislation_json', 'congress') }} as congress,
        {{ json_date('observations.legislation_json', 'introducedDate') }} as introduced_date,
        {{ json_date('observations.legislation_json', 'latestAction.actionDate') }} as latest_action_date,
        {{ json_text('observations.legislation_json', 'latestAction.text') }} as latest_action_text,
        {{ json_text('observations.legislation_json', 'url') }} as legislation_url
    from observations
    join {{ ref('stg_congress__member_detail') }} as members
      on observations.parent_raw_id = members.raw_id
)

select
    joined.*,
    joined.source_raw_id::text || ':' || joined.sponsorship_type || ':' || joined.source_ordinal::text as observation_key
from joined
