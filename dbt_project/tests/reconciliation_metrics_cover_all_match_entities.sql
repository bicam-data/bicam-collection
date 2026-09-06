with expected as (
    select distinct 'bill' as entity from {{ ref('fct_bill_matches') }}
    union all
    select distinct 'committee_report' as entity from {{ ref('fct_committee_report_matches') }}
    union all
    select distinct 'hearing' as entity from {{ ref('fct_hearing_matches') }}
)

select expected.entity
from expected
left join {{ ref('fct_reconciliation_metrics') }} as metrics using (entity)
where metrics.entity is null
