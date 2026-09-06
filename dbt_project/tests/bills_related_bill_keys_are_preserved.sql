with source_keys as (
    select distinct bill_id, relatedbill_id as related_bill_id
    from {{ ref('stg_congress__bill_relatedbills') }}
), published_keys as (
    select bill_id, related_bill_id
    from {{ ref('bills_related_bills') }}
)
select source_keys.*
from source_keys
join {{ ref('bills') }} b on b.bill_id = source_keys.bill_id
join {{ ref('bills') }} rb on rb.bill_id = source_keys.related_bill_id
left join published_keys
  on published_keys.bill_id = source_keys.bill_id
 and published_keys.related_bill_id = source_keys.related_bill_id
where published_keys.bill_id is null
