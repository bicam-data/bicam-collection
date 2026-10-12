{{ config(tags=['bills', 'public', 'quality']) }}

-- Preserve the value conventions used by the published BICAM bills archive.
-- The reference rows themselves remain package-grain, but their public labels
-- should be directly comparable to the legacy export.
select 'bills_reference_codes' as table_name, bill_code_id as row_id
from {{ ref('bills_reference_codes') }}
where reference_code !~ '^USC-[0-9]+$'

union all

select 'bills_reference_statutes' as table_name, bill_statute_id as row_id
from {{ ref('bills_reference_statutes') }}
where reference_statute !~ '^stat[.][0-9]+$'

union all

select 'bills_reference_laws' as table_name, bill_id || '|' || law_id as row_id
from {{ ref('bills_reference_laws') }}
where law_id !~ '^PL[0-9]+-[0-9]+$'
   or law_type not in ('Public Law', 'Private Law')
