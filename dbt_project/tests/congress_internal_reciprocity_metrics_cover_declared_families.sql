/* Every declared Congress.gov reciprocity family must have an aggregate row,
   including an explicit zero/review row when the current scope has no edges. */
with declared as (
    select * from (values
        ('member_bill_sponsor'),
        ('member_bill_cosponsor'),
        ('member_amendment_sponsor'),
        ('member_amendment_cosponsor'),
        ('committee_bill'),
        ('bill_amendment'),
        ('bill_committee_report'),
        ('committee_nomination'),
        ('committee_committee_report'),
        ('committee_meeting_hearing'),
        ('treaty_committee_report')
    ) as families(relationship_type)
)
select declared.relationship_type
from declared
left join {{ ref('fct_congress_internal_reciprocity_metrics') }} as metrics using (relationship_type)
where metrics.relationship_type is null
   or metrics.relationship_count < 0
