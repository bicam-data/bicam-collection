{{ config(tags=["operations", "coverage", "quality"]) }}

with roots(category, list_node_type, item_key, detail_node_type) as (
    values
        ('bills', 'bill_list', 'bills', 'bill_detail'),
        ('amendments', 'amendment_list', 'amendments', 'amendment_detail'),
        ('committee-meetings', 'committee_meeting_list', 'committeeMeetings', 'committee_meeting_detail'),
        ('committee-prints', 'committee_print_list', 'committeePrints', 'committee_print_detail'),
        ('committee-reports', 'committee_report_list', 'reports', 'committee_report_detail'),
        ('committees', 'committee_list', 'committees', 'committee_detail'),
        ('hearings', 'hearing_list', 'hearings', 'hearing_detail'),
        ('nominations', 'nomination_list', 'nominations', 'nomination_detail'),
        ('treaties', 'treaty_list', 'treaties', 'treaty_detail'),
        ('members', 'member_list', 'members', 'member_detail'),
        ('congresses', 'congress_list', 'congresses', 'congress_detail'),
        ('house-communications', 'house_communication_list', 'houseCommunications', 'house_communication_detail'),
        ('senate-communications', 'senate_communication_list', 'senateCommunications', 'senate_communication_detail'),
        ('house-requirements', 'house_requirement_list', 'houseRequirements', 'house_requirement_detail'),
        ('daily-congressional-record', 'daily_congressional_record_list', 'dailyCongressionalRecord', 'daily_congressional_record_detail'),
        ('bound-congressional-record', 'bound_congressional_record_list', 'boundCongressionalRecord', 'bound_congressional_record_detail'),
        ('crs-reports', 'crs_report_list', 'CRSReports', 'crs_report_detail'),
        ('house-votes', 'house_vote_list', 'houseRollCallVotes', 'house_vote_detail')
),
standard_root_items as (
    select distinct
        'congress' as source,
        roots.category,
        roots.list_node_type,
        roots.detail_node_type,
        regexp_replace(item->>'url', '[?].*$', '') as endpoint_path
    from {{ source('raw', 'raw_responses') }} as lists
    join roots
      on roots.category = lists.category
     and roots.list_node_type = lists.node_type
    cross join lateral jsonb_array_elements(
        coalesce(lists.response->roots.item_key, '[]'::jsonb)
    ) as item
    where lists.source = 'congress'
      and item->>'url' is not null
),
law_root_items as (
    select distinct
        'congress' as source,
        'laws' as category,
        'law_list' as list_node_type,
        'law_detail' as detail_node_type,
        'https://api.congress.gov/v3/law/' || (bill->>'congress') || '/'
        || case lower(law->>'type')
            when 'public law' then 'pub'
            when 'private law' then 'priv'
        end || '/' || substring((law->>'number') from '([0-9]+)$') as endpoint_path
    from {{ source('raw', 'raw_responses') }} as lists
    cross join lateral jsonb_array_elements(coalesce(lists.response->'bills', '[]'::jsonb)) as bill
    cross join lateral jsonb_array_elements(coalesce(bill->'laws', '[]'::jsonb)) as law
    where lists.source = 'congress'
      and lists.category = 'laws'
      and lists.node_type = 'law_list'
      and lower(law->>'type') in ('public law', 'private law')
      and substring((law->>'number') from '([0-9]+)$') is not null
),
root_items as (
    select * from standard_root_items
    union all
    select * from law_root_items
),
details as (
    select
        regexp_replace(endpoint_url, '[?].*$', '') as endpoint_path,
        max(fetched_at) as last_fetched_at
    from {{ source('raw', 'raw_responses') }}
    where source = 'congress'
    group by 1
),
confirmed_source_errors as (
    -- Preserve deterministic upstream defects as evidence, but do not keep
    -- presenting an exact URL that Congress.gov has rejected repeatedly as
    -- actionable crawler work. Match on the stable path because historical
    -- attempts can differ only in query-string ordering.
    select
        category,
        node_type,
        regexp_replace(endpoint_url, '[?].*$', '') as endpoint_path
    from {{ source('raw', 'failed_requests') }}
    where http_status in (404, 500)
    group by 1, 2, 3
    having count(*) >= 3
),
classified as (
    select
        roots.source,
        roots.category,
        roots.list_node_type,
        roots.detail_node_type,
        {{ collection_partition_value('roots.endpoint_path', 'roots.category') }} as partition_value,
        roots.endpoint_path,
        details.last_fetched_at,
        source_errors.endpoint_path is not null as has_confirmed_source_error
    from root_items as roots
    left join details
      on details.endpoint_path = roots.endpoint_path
    left join confirmed_source_errors as source_errors
      on source_errors.category = roots.category
     and source_errors.node_type = roots.detail_node_type
     and source_errors.endpoint_path = roots.endpoint_path
)

select
    {{ hash_to_uuid("source || '|' || category || '|' || partition_value") }} as root_detail_coverage_id,
    source,
    category,
    list_node_type,
    detail_node_type,
    partition_value,
    count(*) as root_item_count,
    count(*) filter (where last_fetched_at is not null) as fetched_detail_count,
    count(*) filter (where last_fetched_at is null) as missing_detail_count,
    count(*) filter (
        where last_fetched_at is null and has_confirmed_source_error
    ) as confirmed_source_error_detail_count,
    count(*) filter (
        where last_fetched_at is null and not has_confirmed_source_error
    ) as actionable_missing_detail_count,
    round(
        count(*) filter (where last_fetched_at is not null)::numeric
        / nullif(count(*), 0),
        6
    ) as detail_coverage_rate,
    round(
        count(*) filter (
            where last_fetched_at is not null or has_confirmed_source_error
        )::numeric / nullif(count(*), 0),
        6
    ) as accounted_detail_coverage_rate,
    case
        when count(*) filter (
            where last_fetched_at is null and not has_confirmed_source_error
        ) = 0 then 'pass'
        else 'incomplete'
    end as quality_status,
    max(last_fetched_at) as last_fetched_at
from classified
group by 1, 2, 3, 4, 5, 6
