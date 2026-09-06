/* Treaty relatedDocs identify a committee report at report level.  The
   report detail relation must therefore make report_id available in addition
   to report_part_id; otherwise valid counterparts appear unavailable. */
select
    relationship_id
from {{ ref('fct_congress_internal_reciprocity') }} as reciprocity
join {{ ref('stg_congress__treaty_committeereports') }} as treaty_side
  on lower(treaty_side.treaty_id) = lower(reciprocity.left_id)
 and lower(treaty_side.report_id) = lower(reciprocity.right_id)
where reciprocity.relationship_type = 'treaty_committee_report'
  and reciprocity.reciprocity_status like '%counterpart_unavailable'
