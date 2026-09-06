{{ config(tags=["bills", "historical", "quality", "operations"]) }}

with expected as (
    select
        count(*) as expected_rows
    from {{ ref('int_raw_responses__current') }} as responses
    cross join lateral jsonb_array_elements(
        case
            when jsonb_typeof(responses.response -> 'bill') = 'array'
                then responses.response -> 'bill'
            else '[]'::jsonb
        end
    ) as entries(bill_json)
    where responses.source = 'congress'
      and responses.category = 'bills'
      and responses.node_type = 'bill_detail'
      and jsonb_typeof(responses.response -> 'bill') = 'array'
),
actual as (
    select count(*) as actual_rows
    from {{ ref('stg_congress__bill_introductions') }}
    where is_historical_array
)

select
    expected.expected_rows,
    actual.actual_rows
from expected
cross join actual
where actual.actual_rows < expected.expected_rows
