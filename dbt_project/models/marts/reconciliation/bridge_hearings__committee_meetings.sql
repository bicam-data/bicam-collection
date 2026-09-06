{{ config(
    materialized="table",
    tags=["hearings", "committee_meetings", "reconciliation", "bridge"]
) }}

/*
   The API exposes this relationship from several directions.  A hearing may
   have an associated committee meeting, a meeting may list one or more
   hearing transcripts, and a GovInfo CHRG package may provide additional
   evidence for the same pair.  Keep one row per logical hearing/meeting pair
   while retaining every source link and rule that produced it.

   ``canonical_hearing_id`` uses the stable public identity for either source.
   GovInfo-only packages therefore remain usable relationship targets rather
   than being reduced to source-qualified internal keys.
*/
with source_links as (
    select
        link_id::text as source_link_id,
        source_system,
        source_id::text,
        target_system,
        target_id::text,
        relationship_type,
        congress,
        match_rule
    from {{ ref('int_hearing_committee_meeting_links') }}
    where relationship_type in (
        'congress_hearing_to_committee_meeting_associated',
        'committee_meeting_to_congress_hearing_child',
        'govinfo_hearing_to_committee_meeting',
        'govinfo_joint_hearing_to_committee_meeting'
    )
), normalized as (
    select
        source_link_id,
        relationship_type,
        congress,
        match_rule,
        case
            when relationship_type = 'committee_meeting_to_congress_hearing_child'
                then target_system
            else source_system
        end as hearing_source_system,
        case
            when relationship_type = 'committee_meeting_to_congress_hearing_child'
                then target_id
            else source_id
        end as hearing_source_id,
        case
            when relationship_type = 'committee_meeting_to_congress_hearing_child'
                then source_system
            else target_system
        end as meeting_source_system,
        case
            when relationship_type = 'committee_meeting_to_congress_hearing_child'
                then source_id
            else target_id
        end as committee_meeting_id
    from source_links
), resolved as (
    select
        normalized.*,
        case
            when normalized.hearing_source_system = 'congress'
                then congress_public.public_hearing_id
            else govinfo_public.hearing_id
        end as canonical_hearing_id,
        coalesce(
            congress_hearings.chamber,
            hearing_matches.chamber,
            case
                when normalized.hearing_source_id ~* '^(CHRG-[0-9]+)?HHRG[0-9]+' then 'house'
                when normalized.hearing_source_id ~* '^(CHRG-[0-9]+)?SHRG[0-9]+' then 'senate'
                when normalized.hearing_source_id ~* '^(CHRG-[0-9]+)?JHRG[0-9]+' then 'joint'
            end
        ) as chamber
    from normalized
    left join {{ ref('fct_hearing_matches') }} as hearing_matches
      on normalized.hearing_source_system = 'govinfo'
     and hearing_matches.govinfo_source_record_id = normalized.hearing_source_id
    left join {{ ref('int_congress_hearing_public_id_map') }} as congress_public
      on normalized.hearing_source_system = 'congress'
     and congress_public.source_hearing_id = normalized.hearing_source_id
    left join {{ ref('int_govinfo_hearing_identity_map') }} as govinfo_public
      on normalized.hearing_source_system = 'govinfo'
     and govinfo_public.package_id = normalized.hearing_source_id
    left join {{ ref('int_hearings__congress') }} as congress_hearings
      on congress_hearings.hearing_id = case
          when normalized.hearing_source_system = 'congress' then normalized.hearing_source_id
          else hearing_matches.congress_source_record_id
      end
), grouped as (
    select
        coalesce(
            canonical_hearing_id,
            hearing_source_system || ':' || hearing_source_id
        ) as hearing_key,
        max(canonical_hearing_id) as canonical_hearing_id,
        max(committee_meeting_id) as committee_meeting_id,
        max(meeting_source_system) as meeting_source_system,
        max(congress) as congress,
        max(chamber) as chamber,
        array_agg(distinct hearing_source_id order by hearing_source_id) as hearing_source_ids,
        array_agg(distinct hearing_source_system order by hearing_source_system)
            as hearing_source_systems,
        array_agg(distinct source_link_id order by source_link_id) as evidence_link_ids,
        array_agg(distinct relationship_type order by relationship_type)
            as evidence_relationship_types,
        array_agg(distinct match_rule order by match_rule) as evidence_match_rules,
        count(distinct source_link_id)::integer as evidence_count,
        bool_or(hearing_source_system = 'congress') as has_congress_hearing_evidence,
        bool_or(hearing_source_system = 'govinfo') as has_govinfo_hearing_evidence
    from resolved
    group by
        coalesce(
            canonical_hearing_id,
            hearing_source_system || ':' || hearing_source_id
        )
)
select
    {{ hash_to_uuid("hearing_key || '|' || committee_meeting_id") }} as bridge_id,
    hearing_key,
    canonical_hearing_id,
    committee_meeting_id,
    meeting_source_system,
    congress,
    chamber,
    hearing_source_ids,
    hearing_source_systems,
    evidence_link_ids,
    evidence_relationship_types,
    evidence_match_rules,
    evidence_count,
    has_congress_hearing_evidence,
    has_govinfo_hearing_evidence
from grouped
