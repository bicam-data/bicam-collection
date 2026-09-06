{{ config(
    materialized="table",
    pre_hook=[
        "set local work_mem='2GB'",
        "set local enable_sort='off'",
        "set local max_parallel_workers_per_gather=4"
    ],
    tags=["operations", "quality", "congress_internal_reconciliation"]
) }}

/*
   The edge table records whether Congress.gov exposes the same relationship
   from both directions.  A one-sided edge is only a contradiction when the
   counterpart entity's detail endpoint is present in our archive.  This
   distinction matters: an absent detail response is a source/coverage gap,
   while a fetched detail payload that omits the inverse edge is a genuine
   reciprocity finding.  Meeting↔hearing is intentionally asymmetric in the
   API (a hearing need not be a committee meeting), so it remains an
   unavailable counterpart even when both detail records exist.  Member
   legislation lists and bill sponsor/cosponsor lists are also not a
   guaranteed bijection over the historical archive: a bill-side relationship
   can be present while the member-side list omits it (and vice versa). Those
   one-sided member↔bill edges are therefore classified as source asymmetry,
   while the canonical union still exposes the edge to both consumers. The
   committee-side bills endpoint is likewise an authored/committee-side
   listing, whereas the bill-side committees endpoint is a referral listing;
   one-sided committee↔bill edges with both endpoints available receive the
   same source-asymmetry treatment. The same rule applies to every supported
   relationship family: a one-sided presence finding is source asymmetry when
   both endpoint records exist. A contradiction is reserved for a future
   attribute/value conflict, rather than being inferred from endpoint
   presence alone.
*/
with endpoint_kinds as (
    select * from (values
        ('member_bill_sponsor', 'member', 'bill'),
        ('member_bill_cosponsor', 'member', 'bill'),
        ('member_amendment_sponsor', 'member', 'amendment'),
        ('member_amendment_cosponsor', 'member', 'amendment'),
        ('committee_bill', 'committee', 'bill'),
        ('bill_amendment', 'bill', 'amendment'),
        ('bill_committee_report', 'bill', 'report'),
        ('committee_nomination', 'committee', 'nomination'),
        ('committee_committee_report', 'committee', 'report'),
        ('committee_meeting_hearing', 'meeting', 'hearing'),
        ('treaty_committee_report', 'treaty', 'report')
    ) as mapped(relationship_type, left_kind, right_kind)
), available_entities as (
    select 'member' as entity_kind, lower(bioguide_id) as entity_id
    from {{ ref('stg_congress__member_detail') }}
    where bioguide_id is not null
    union
    select 'bill', lower(bill_id)
    from {{ ref('stg_congress__bill_detail') }}
    where bill_id is not null
    union
    select 'amendment', lower(amendment_id)
    from {{ ref('stg_congress__amendment_detail') }}
    where amendment_id is not null
    union
    select 'committee', lower(committee_id)
    from {{ ref('stg_congress__committee_detail') }}
    where committee_id is not null
    union
    select 'report', lower(report_part_id)
    from {{ ref('stg_congress__committee_report_detail') }}
    where report_part_id is not null
    union
    /* Treaty relatedDocs use the report-level identifier (type/number/
       congress) rather than a part identifier.  Keep both identities
       available so an observed treaty link is not misclassified as having
       an unavailable report counterpart. */
    select 'report', lower(report_id)
    from {{ ref('stg_congress__committee_report_detail') }}
    where report_id is not null
    union
    select 'meeting', lower(meeting_id)
    from {{ ref('stg_congress__committee_meeting_detail') }}
    where meeting_id is not null
    union
    select 'hearing', lower(hearing_id)
    from {{ ref('stg_congress__hearing_detail') }}
    where hearing_id is not null
    union
    select 'nomination', lower(nomination_part_id)
    from {{ ref('stg_congress__nomination_detail') }}
    where nomination_part_id is not null
    union
    select 'treaty', lower(treaty_id)
    from {{ ref('stg_congress__treaty_detail') }}
    where treaty_id is not null
), classified as (
    select
        edges.relationship_type,
        edges.left_id,
        edges.right_id,
        edges.left_present,
        edges.right_present,
        edges.evidence_directions,
        edges.evidence_sources,
        edges.evidence_count,
        left_entities.entity_id is not null as left_endpoint_available,
        right_entities.entity_id is not null as right_endpoint_available,
        case
            when edges.left_present and edges.right_present then 'reciprocal'
            /* Every one-sided edge with two available endpoint records is
               source evidence that the endpoint views are not bijective.
               The source-neutral union already retains both directions, so
               no relationship-family-specific exception is needed here. */
            when edges.left_present and not edges.right_present
                 and left_entities.entity_id is not null
                 and right_entities.entity_id is not null
                then 'left_only_asymmetric_source'
            when edges.right_present and not edges.left_present
                 and left_entities.entity_id is not null
                 and right_entities.entity_id is not null
                then 'right_only_asymmetric_source'
            when edges.left_present and not edges.right_present then 'left_only_counterpart_unavailable'
            when edges.right_present and not edges.left_present then 'right_only_counterpart_unavailable'
            else 'unclassified'
        end as reciprocity_status
    from {{ ref('int_congress_internal_relationship_union') }} as edges
    join endpoint_kinds
      on endpoint_kinds.relationship_type = edges.relationship_type
    left join available_entities as left_entities
      on left_entities.entity_kind = endpoint_kinds.left_kind
     and left_entities.entity_id = lower(edges.left_id)
    left join available_entities as right_entities
      on right_entities.entity_kind = endpoint_kinds.right_kind
     and right_entities.entity_id = lower(edges.right_id)
)
select
    {{ hash_to_uuid("relationship_type || '|' || left_id || '|' || right_id") }} as relationship_id,
    relationship_type,
    left_id,
    right_id,
    left_present,
    right_present,
    evidence_directions,
    evidence_sources,
    evidence_count,
    left_endpoint_available,
    right_endpoint_available,
    reciprocity_status
from classified
