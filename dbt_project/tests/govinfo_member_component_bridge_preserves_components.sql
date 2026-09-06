/* Every split GovInfo member component must have exactly one bridge row. */
with expected as (
    select distinct component_key
    from {{ ref('int_govinfo_multi_legislator_candidates') }}
), actual as (
    select component_key
    from {{ ref('bridge_govinfo__member_components') }}
)
select expected.component_key
from expected
left join actual using (component_key)
where actual.component_key is null
union all
select actual.component_key
from actual
left join expected using (component_key)
where expected.component_key is null
