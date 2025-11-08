{{ config(materialized='table', tags=['int','congressional','bills']) }}

{# Replicates _clean_bills_singular() transformations #}

with base as (
  select * from {{ ref('stg_congressional__bills') }}
),
dedup as (
  select b.*,
         row_number() over (partition by b.bill_id order by b.processed_at_ts desc nulls last) rn
  from base b
),
latest as (
  select * from dedup where rn = 1
)
select
  -- Primary key
  coalesce(l.bill_id, 'ID_ERROR')::text as bill_id,
  
  -- Field renaming and transformations matching _clean_bills_singular()
  lower(l.type)::text as bill_type,
  l.number::float as bill_number,
  {{ safe_int('l.congress') }} as congress,
  l.title::text as title,
  {{ standardize_chamber('l.originchamber') }}::text as origin_chamber,
  l.policyarea_name::text as policy_area,
  null::boolean as is_law,  -- added via postprocessing
  
  -- Date standardization
  {{ standardize_date('l.introduceddate') }} as introduced_at,
  {{ standardize_date('l.updatedate') }} as updated_at,
  
  -- Text cleaning
  {{ clean_long_text('l.constitutionalauthoritystatementtext') }} as constitutional_authority_statement,
  
  -- Count fields with safe_int and defaults
  {{ safe_int('l.actions_count', 0) }} as actions_count,
  {{ safe_int('l.amendments_count', 0) }} as amendments_count,
  {{ safe_int('l.committees_count', 0) }} as committees_count,
  {{ safe_int('l.cosponsors_count', 0) }} as cosponsors_count,
  
  -- Computed field: cosponsors_withdrawn_count
  ({{ safe_int('l.cosponsors_countincludingwithdrawncosponsors', 0) }} - {{ safe_int('l.cosponsors_count', 0) }}) as cosponsors_withdrawn_count,
  
  {{ safe_int('l.relatedbills_count', 0) }} as relatedbills_count,
  {{ safe_int('l.subjects_count', 0) }} as subjects_count,
  {{ safe_int('l.summaries_count', 0) }} as summaries_count,
  {{ safe_int('l.textversions_count', 0) }} as texts_count,
  {{ safe_int('l.titles_count', 0) }} as titles_count
from latest l

