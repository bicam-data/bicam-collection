/*
   A detail payload must not make a Congress partition appear crawled.  This
   test requires a retained root-list response for every declared main-11
   partition; HTTP-200 empty lists satisfy the presence requirement and remain
   available as source-coverage evidence.
*/
with specs(category, node_type, path) as (
    values
        ('bills', 'bill_list', 'bill'),
        ('amendments', 'amendment_list', 'amendment'),
        ('committee-meetings', 'committee_meeting_list', 'committee-meeting'),
        ('committee-prints', 'committee_print_list', 'committee-print'),
        ('committee-reports', 'committee_report_list', 'committee-report'),
        ('committees', 'committee_list', 'committee'),
        ('hearings', 'hearing_list', 'hearing'),
        ('nominations', 'nomination_list', 'nomination'),
        ('treaties', 'treaty_list', 'treaty')
), scope as (
    select
        scopes.category,
        regexp_replace(trim(partition_value), '^p', '') as partition_value,
        specs.node_type,
        specs.path
    from {{ ref('collection_scope_partitions') }} as scopes
    join specs using (category)
    cross join lateral regexp_split_to_table(
        scopes.partition_values, ','
    ) as split(partition_value)
    where scopes.source = 'congress'
      and scopes.partition_field = 'congress'
), missing as (
    select scope.category, scope.partition_value
    from scope
    where not exists (
        select 1
        from {{ source('raw', 'raw_responses') }} as responses
        where responses.source = 'congress'
          and responses.category = scope.category
          and responses.node_type = scope.node_type
          and responses.endpoint_url ~ (
              '/'
              || scope.path
              || '/'
              || scope.partition_value
              || '(?:[?/]|$)'
          )
    )
)

select *
from missing
