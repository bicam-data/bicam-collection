{{ config(materialized='table', tags=['int','congressional','amendments'], enabled=false) }}

{# Replicates _clean_amendments_singular() transformations #}

with base as (
  select * from {{ ref('stg_congressional__amendments') }}
),
dedup as (
  select
    b.*,
    row_number() over (
      partition by b.amendment_id
      order by b.processed_at_ts desc nulls last
    ) as rn
  from base b
),
latest as (
  select * from dedup where rn = 1
),
actions as (
  select
    a.amendment_id,
    count(*) as actions_count
  from {{ ref('stg_congressional__amendments_actions') }} a
  group by 1
),
texts as (
  select
    t.amendment_id,
    count(*) as texts_count
  from {{ ref('stg_congressional__amendments_texts') }} t
  group by 1
),
cleaned as (
  select
    -- Primary key
    coalesce(l.amendment_id, 'ID_ERROR')::text as amendment_id,
    
    -- Basic fields
    lower(l.type)::text as amendment_type,
    {{ safe_int('l.number') }} as amendment_number,
    {{ safe_int('l.congress') }} as congress,
    {{ standardize_chamber('l.chamber') }}::text as chamber,
    
    -- Text fields with cleaning
    {{ clean_long_text('l.purpose') }} as purpose,
    {{ clean_long_text('l.description') }} as description,
    
    -- Date standardization
    {{ standardize_date('l.proposeddate') }} as proposed_at,
    {{ standardize_date('l.submitteddate') }} as submitted_at,
    {{ standardize_date('l.updatedate') }} as updated_at,
    
    -- Boolean flags based on URL presence
    case when l.amendedbill_url is not null and l.amendedbill_url != '' then true else false end as is_bill_amendment,
    case when l.amendedtreaty_url is not null and l.amendedtreaty_url != '' then true else false end as is_treaty_amendment,
    case when l.amendedamendment_url is not null and l.amendedamendment_url != '' then true else false end as is_amendment_amendment,
    
    -- Count fields
    coalesce(ac.actions_count, 0) as actions_count,
    {{ safe_int('l.cosponsors_count', 0) }} as cosponsors_count,
    ({{ safe_int('l.cosponsors_countincludingwithdrawncosponsors', 0) }} - {{ safe_int('l.cosponsors_count', 0) }}) as cosponsors_withdrawn_count,
    coalesce(tx.texts_count, 0) as texts_count,
    {{ safe_int('l.amendmentstoamendment_count', 0) }} as amendments_to_amendment_count,
    
    -- Extract bill_id from amendedbill fields
    case 
      when l.amendedbill_type is not null and l.amendedbill_number is not null and l.amendedbill_congress is not null
      then lower(l.amendedbill_type) || l.amendedbill_number || '-' || l.amendedbill_congress
      else null
    end::text as bill_id,
    lower(l.amendedbill_type)::text as bill_type,
    {{ safe_int('l.amendedbill_number') }} as bill_number,
    {{ safe_int('l.amendedbill_congress') }} as bill_congress,
    {{ standardize_chamber('l.amendedbill_originchamber') }}::text as bill_origin_chamber,
    l.amendedbill_title::text as bill_title,
    
    -- Extract amended amendment info
    case 
      when l.amendedamendment_type is not null and l.amendedamendment_number is not null and l.amendedamendment_congress is not null
      then lower(l.amendedamendment_type) || l.amendedamendment_number || '-' || l.amendedamendment_congress
      else null
    end::text as amended_amendment_id,
    lower(l.amendedamendment_type)::text as amended_amendment_type,
    {{ safe_int('l.amendedamendment_number') }} as amended_amendment_number,
    {{ safe_int('l.amendedamendment_congress') }} as amended_amendment_congress,
    l.amendedamendment_purpose::text as amended_amendment_purpose,
    {{ clean_long_text('l.amendedamendment_description') }} as amended_amendment_description,
    
    -- Extract treaty info
    case 
      when l.amendedtreaty_number is not null and l.amendedtreaty_congress is not null
      then 'td' || l.amendedtreaty_congress || '-' || l.amendedtreaty_number
      else null
    end::text as treaty_id,
    {{ safe_int('l.amendedtreaty_number') }} as treaty_number,
    {{ safe_int('l.amendedtreaty_congress') }} as treaty_congress
    
  from latest l
  left join actions ac using (amendment_id)
  left join texts tx using (amendment_id)
)
select * from cleaned
