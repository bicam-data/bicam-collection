with expected as (
    select count(*) as expected_count
    from {{ ref('collection_scope_partitions') }} as scopes
    cross join lateral regexp_split_to_table(scopes.partition_values, ',') as partition_value
),
actual as (
    select count(*) as actual_count
    from {{ ref('fct_collection_coverage') }}
    where source = 'congress'
      and node_type = '*'
)

select expected.expected_count, actual.actual_count
from expected cross join actual
where expected.expected_count <> actual.actual_count
