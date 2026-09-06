{{ config(
    tags=["operations", "coverage", "quality"],
    pre_hook=[
        "set local work_mem='1GB'",
        "set local max_parallel_workers_per_gather=4"
    ]
) }}

with roots(category, node_type, item_key, path, is_partitioned) as (
    values
        ('bills', 'bill_list', 'bills', 'bill', true),
        ('amendments', 'amendment_list', 'amendments', 'amendment', true),
        ('committee-meetings', 'committee_meeting_list', 'committeeMeetings', 'committee-meeting', true),
        ('committee-prints', 'committee_print_list', 'committeePrints', 'committee-print', true),
        ('committee-reports', 'committee_report_list', 'reports', 'committee-report', true),
        ('committees', 'committee_list', 'committees', 'committee', true),
        ('hearings', 'hearing_list', 'hearings', 'hearing', true),
        ('nominations', 'nomination_list', 'nominations', 'nomination', true),
        ('treaties', 'treaty_list', 'treaties', 'treaty', true),
        ('members', 'member_list', 'members', 'member', false),
        ('congresses', 'congress_list', 'congresses', 'congress', false),
        ('laws', 'law_list', 'bills', 'law', true),
        ('house-communications', 'house_communication_list', 'houseCommunications', 'house-communication', true),
        ('senate-communications', 'senate_communication_list', 'senateCommunications', 'senate-communication', true),
        ('house-requirements', 'house_requirement_list', 'houseRequirements', 'house-requirement', false),
        ('daily-congressional-record', 'daily_congressional_record_list', 'dailyCongressionalRecord', 'daily-congressional-record', false),
        ('bound-congressional-record', 'bound_congressional_record_list', 'boundCongressionalRecord', 'bound-congressional-record', false),
        ('crs-reports', 'crs_report_list', 'CRSReports', 'crsreport', false),
        ('house-votes', 'house_vote_list', 'houseRollCallVotes', 'house-vote', true)
),
standard_root_responses as (
    select
        roots.category,
        roots.node_type,
        case
            when roots.is_partitioned
                then regexp_replace(
                    responses.endpoint_url,
                    '.*/' || roots.path || '/([0-9]+).*',
                    '\1'
                )
            else '*'
        end as partition_value,
        roots.item_key,
        responses.endpoint_url,
        responses.response
    from {{ ref('int_raw_responses__current') }} as responses
    join roots
      on roots.category = responses.category
     and roots.node_type = responses.node_type
    where responses.source = 'congress'
      and responses.response ? 'pagination'
      and (
          (roots.is_partitioned and responses.endpoint_url ~ ('/' || roots.path || '/[0-9]+'))
          or (not roots.is_partitioned and responses.endpoint_url ~ ('/' || roots.path || '(\\?|$)'))
      )
      -- Chamber-filtered hearing lists have their own offsets and counts.
      -- Mixing those ranges into the global /hearing/{congress} chain can
      -- make an unrelated chamber page appear to cover a missing global
      -- position.
      and not (
          roots.category = 'hearings'
          and responses.endpoint_url ~ '/hearing/[0-9]+/(house|senate|joint|nochamber)([/?]|$)'
      )
),
legacy_record_responses as (
    select
        'congressional-record'::text as category,
        'congressional_record_list'::text as node_type,
        '*'::text as partition_value,
        'legacyItems'::text as item_key,
        responses.endpoint_url,
        jsonb_build_object(
            'legacyItems', coalesce(responses.response #> '{Results,Issues}', '[]'::jsonb),
            'pagination', jsonb_build_object(
                'count', coalesce((responses.response #>> '{Results,TotalCount}')::bigint, 0),
                'next', case
                    when coalesce((responses.response #>> '{Results,IndexStart}')::bigint, 1) - 1
                         + coalesce((responses.response #>> '{Results,SetSize}')::bigint, 0)
                         < coalesce((responses.response #>> '{Results,TotalCount}')::bigint, 0)
                    then 'offset-remains'
                end
            )
        ) as response
    from {{ ref('int_raw_responses__current') }} as responses
    where responses.source = 'congress'
      and responses.category = 'congressional-record'
      and responses.node_type = 'congressional_record_list'
),
root_responses as (
    select * from standard_root_responses
    union all
    select * from legacy_record_responses
),
root_pages as (
    select
        roots.*,
        coalesce(
            (regexp_match(endpoint_url, '[?&]offset=([0-9]+)'))[1]::bigint,
            0
        ) as range_start,
        jsonb_array_length(coalesce(response->item_key, '[]'::jsonb))::bigint as item_count
    from root_responses as roots
),
page_aggregates as (
    select
        category,
        node_type,
        partition_value,
        max((response->'pagination'->>'count')::bigint) as expected_item_count,
        sum(item_count) as observed_row_count,
        count(*) as list_page_count,
        count(*) filter (where response->'pagination'->>'next' is not null) as pages_with_next,
        count(*) filter (where response->'pagination'->>'next' is null) as terminal_page_count
    from root_pages
    group by 1, 2, 3
),
position_multiranges as (
    select
        category,
        node_type,
        partition_value,
        range_agg(int8range(range_start, range_start + item_count, '[)')) as covered_ranges
    from root_pages
    where item_count > 0
    group by 1, 2, 3
),
position_coverage as (
    select
        covered.category,
        covered.node_type,
        covered.partition_value,
        covered.covered_ranges,
        sum(upper(position_range) - lower(position_range))::bigint as covered_position_count
    from position_multiranges as covered
    cross join lateral unnest(covered.covered_ranges) as position_range
    group by 1, 2, 3, 4
),
distinct_items as (
    select
        roots.category,
        roots.node_type,
        roots.partition_value,
        count(distinct regexp_replace(item->>'url', '[?].*$', '')) as distinct_item_count
    from root_pages as roots
    cross join lateral jsonb_array_elements(
        coalesce(roots.response->roots.item_key, '[]'::jsonb)
    ) as item
    where item->>'url' is not null
    group by 1, 2, 3
),
aggregated as (
    select
        pages.*,
        coalesce(items.distinct_item_count, 0) as distinct_item_count,
        least(
            pages.expected_item_count,
            coalesce(positions.covered_position_count, 0)
        ) as covered_position_count,
        int8multirange(int8range(0, pages.expected_item_count, '[)'))
          - coalesce(positions.covered_ranges, '{}'::int8multirange)
          as missing_position_ranges
    from page_aggregates as pages
    left join distinct_items as items
      using (category, node_type, partition_value)
    left join position_coverage as positions
      using (category, node_type, partition_value)
),
/*
   Do not expand every missing position into one row.  Most complete
   partitions have an empty multirange, but a stale or partially crawled
   partition can contain millions of positions; generate_series made this
   model effectively unbounded and blocked otherwise unrelated dbt builds.
   Build failed-page ranges once, then intersect them with the (usually very
   small) missing multirange.  The multirange union preserves the old
   de-duplication semantics without materializing individual positions.
*/
failed_source_errors as (
    select
        category,
        node_type,
        endpoint_url,
        bool_or(error like '%source_error=%') as has_structured_source_error,
        count(*)::bigint as failure_count
    from {{ source('raw', 'failed_requests') }}
    where http_status = 500
    group by 1, 2, 3
),
failed_source_error_ranges as (
    select
        failure.category,
        failure.node_type,
        case
            when roots.is_partitioned then regexp_replace(
                failure.endpoint_url,
                '.*/' || roots.path || '/([0-9]+).*',
                '\1'
            )
            else '*'
        end as partition_value,
        coalesce(
            (regexp_match(failure.endpoint_url, '[?&]offset=([0-9]+)'))[1]::bigint,
            0
        ) as range_start,
        case
            -- Structured source errors identify one irreducible position.
            when failure.has_structured_source_error then coalesce(
                (regexp_match(failure.endpoint_url, '[?&]offset=([0-9]+)'))[1]::bigint,
                0
            ) + 1
            -- A few legacy endpoint families return a plain HTTP 500. After
            -- three exact-URL attempts, account for the requested page range.
            else coalesce(
                (regexp_match(failure.endpoint_url, '[?&]offset=([0-9]+)'))[1]::bigint,
                0
            ) + coalesce(
                (regexp_match(failure.endpoint_url, '[?&]limit=([0-9]+)'))[1]::bigint,
                1
            )
        end as range_end
    from failed_source_errors as failure
    join roots
      on roots.category = failure.category
     and roots.node_type = failure.node_type
    where failure.has_structured_source_error
       or failure.failure_count >= 3
),
confirmed_source_error_overlaps as (
    select
        coverage.category,
        coverage.node_type,
        coverage.partition_value,
        int8range(
            greatest(lower(missing_range), failure.range_start),
            least(upper(missing_range), failure.range_end),
            '[)'
        ) as confirmed_range
    from aggregated as coverage
    join failed_source_error_ranges as failure
      on failure.category = coverage.category
     and failure.node_type = coverage.node_type
     and failure.partition_value = coverage.partition_value
    cross join lateral unnest(coverage.missing_position_ranges) as missing_range
    where failure.range_start < upper(missing_range)
      and failure.range_end > lower(missing_range)
),
confirmed_source_error_ranges as (
    select
        category,
        node_type,
        partition_value,
        range_agg(confirmed_range) as confirmed_ranges
    from confirmed_source_error_overlaps
    group by 1, 2, 3
),
classified as (
    select
        coverage.*,
        coalesce(
            (
                select sum(upper(confirmed_range) - lower(confirmed_range))::bigint
                from unnest(errors.confirmed_ranges) as confirmed_range
            ),
            0
        ) as confirmed_source_error_item_count,
        coverage.missing_position_ranges
          - coalesce(errors.confirmed_ranges, '{}'::int8multirange)
          as actionable_missing_position_ranges
    from aggregated as coverage
    left join confirmed_source_error_ranges as errors
      using (category, node_type, partition_value)
)

select
    {{ hash_to_uuid("category || '|' || node_type || '|' || partition_value") }} as pagination_coverage_id,
    category,
    node_type,
    partition_value,
    expected_item_count,
    distinct_item_count as enumerated_item_count,
    covered_position_count as enumerated_row_count,
    observed_row_count,
    greatest(covered_position_count - distinct_item_count, 0) as duplicate_item_count,
    greatest(expected_item_count - distinct_item_count, 0) as source_identity_count_gap,
    list_page_count,
    pages_with_next,
    terminal_page_count,
    greatest(expected_item_count - covered_position_count, 0) as missing_item_count,
    missing_position_ranges,
    confirmed_source_error_item_count,
    greatest(
        expected_item_count - covered_position_count - confirmed_source_error_item_count,
        0
    ) as actionable_missing_item_count,
    actionable_missing_position_ranges,
    ceil(
        greatest(
            expected_item_count - covered_position_count - confirmed_source_error_item_count,
            0
        )::numeric / 250
    )::bigint as estimated_missing_page_count
from classified
