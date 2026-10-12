{{ config(
    materialized="table",
    tags=["operations", "quality", "congress_internal_reconciliation"]
) }}

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
), aggregated as (
    select
        relationship_type,
        count(*) as relationship_count,
        count(*) filter (where reciprocity_status = 'reciprocal') as reciprocal_count,
        count(*) filter (where reciprocity_status like '%counterpart_unavailable') as unavailable_count,
        count(*) filter (where reciprocity_status like '%asymmetric_source') as asymmetric_count,
        count(*) filter (where reciprocity_status like '%contradiction') as contradiction_count,
        count(*) filter (where reciprocity_status = 'unclassified') as unclassified_count
    from {{ ref('fct_congress_internal_reciprocity') }}
    group by relationship_type
), counts as (
    select
        declared.relationship_type,
        coalesce(aggregated.relationship_count, 0)::bigint as relationship_count,
        coalesce(aggregated.reciprocal_count, 0)::bigint as reciprocal_count,
        coalesce(aggregated.unavailable_count, 0)::bigint as unavailable_count,
        coalesce(aggregated.asymmetric_count, 0)::bigint as asymmetric_count,
        coalesce(aggregated.contradiction_count, 0)::bigint as contradiction_count,
        coalesce(aggregated.unclassified_count, 0)::bigint as unclassified_count
    from declared
    left join aggregated using (relationship_type)
)
select
    relationship_type,
    relationship_count,
    reciprocal_count,
    unavailable_count,
    asymmetric_count,
    contradiction_count,
    unclassified_count,
    coalesce(reciprocal_count::numeric / nullif(relationship_count, 0), 0::numeric) as reciprocal_rate,
    coalesce(contradiction_count::numeric / nullif(relationship_count, 0), 0::numeric) as contradiction_rate,
    case
        when relationship_count = 0 then 'review'
        when contradiction_count > 0 then 'review'
        when unclassified_count > 0 then 'review'
        when asymmetric_count > 0 then 'review'
        when unavailable_count > 0 then 'review'
        else 'pass'
    end as quality_status,
    /*
       Reciprocity is accepted for release when the source evidence is
       classifiable and contains no contradiction.  One-sided edges and
       unavailable counterpart endpoints are expected for several Congress
       API families (for example member lists and meeting/hearing links), so
       they remain visible in quality_status but do not block the canonical
       union.  This explicit field prevents downstream exporters from having
       to infer that policy from the diagnostic status.
    */
    case
        when relationship_count = 0 then 'reject_no_evidence'
        when contradiction_count > 0 then 'reject_contradiction'
        when unclassified_count > 0 then 'reject_unclassified'
        else 'accepted_with_source_boundaries'
    end as release_acceptance_status
from counts
