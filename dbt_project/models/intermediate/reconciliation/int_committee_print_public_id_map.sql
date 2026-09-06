{{ config(tags=["committee_print_reconciliation", "committee_prints", "intermediate"]) }}

/* Congress.gov preserves a historical `NoChamber` route whose derived `nprt`
   identifier was never part of the published product. The published stable
   identity treats that route as the joint (`jprt`) variant while preserving
   separate House/Senate variants with the same jacket and Congress. */

select
    canonical_print_id as source_print_id,
    case
        when print_type = 'nprt'
            then 'jprt' || print_jacketnumber::text || '-' || congress::text
        else canonical_print_id
    end as public_print_id,
    case
        when print_type = 'nprt' then 'nochamber_to_joint'
        else 'source_identity'
    end as mapping_rule
from {{ ref('int_committee_prints__congress') }}
