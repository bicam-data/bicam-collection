{{ config(tags=["hearings", "committee_meetings", "reconciliation", "intermediate"]) }}

with congress_hearings as (
    select
        hearing_id,
        congress,
        hearing_jacketnumber,
        chamber,
        associated_meeting_id,
        case
            when associated_meeting_id is not null
            then associated_meeting_id || '-' || congress::text
        end as associated_meeting_key
    from {{ ref('int_hearings__congress') }}
),

hearing_associations as (
    select
        hearing_id as source_id,
        associated_meeting_key as target_id,
        congress,
        'congress_hearing_to_committee_meeting_associated' as relationship_type,
        'congress.hearing_detail.associatedMeeting.eventId' as match_rule
    from congress_hearings
    where associated_meeting_key is not null
),

meeting_children as (
    /*
       The meeting payload carries a route-shaped hearing URL.  For some
       joint hearings Congress.gov exposes that URL through the House/Senate
       route even though the hearing detail's canonical identity is JHRG.
       Prefer a unique canonical joint hearing at the same Congress and
       jacket number; retain the route-derived target only when no such
       hearing exists.  This preserves the source edge while preventing a
       single joint package from being represented as two chamber hearings.
    */
    with routed as (
        select distinct
            meeting_id as source_id,
            hearing_id as routed_target_id,
            hearing_jacketnumber,
            congress
        from {{ ref('stg_congress__committee_meeting_hearings') }}
    ),
    unique_joint as (
        select
            congress,
            hearing_jacketnumber,
            min(hearing_id) as hearing_id
        from congress_hearings
        where chamber = 'joint'
        group by congress, hearing_jacketnumber
        having count(*) = 1
    )
    select distinct
        routed.source_id,
        coalesce(unique_joint.hearing_id, routed.routed_target_id) as target_id,
        routed.congress,
        'committee_meeting_to_congress_hearing_child' as relationship_type,
        case
            when unique_joint.hearing_id is not null
                then 'congress.committee_meeting_detail.hearingTranscript -> canonical_joint_hearing'
            else 'congress.committee_meeting_detail.hearingTranscript'
        end as match_rule
    from routed
    left join unique_joint
      on unique_joint.congress = routed.congress
     and unique_joint.hearing_jacketnumber = routed.hearing_jacketnumber
),

govinfo_hearings as (
    select
        package_id,
        congress,
        lower(chamber) as chamber,
        package_id as source_id,
        regexp_match(
            lower(package_id),
            '^chrg-([0-9]+)([hsj]hrg)([0-9]+).*$'
        ) as package_parts,
        coalesce(
            nullif(
                ltrim(
                    (regexp_match(
                        lower(package_id),
                        '^chrg-([0-9]+)([hsj]hrg)([0-9]+).*$'
                    ))[3],
                    '0'
                ),
                ''
            ),
            '0'
        ) as jacket_number
    from {{ ref('stg_govinfo__hearings_packages') }}
),

govinfo_to_congress_exact as (
    select
        g.source_id,
        h.hearing_id as target_id,
        g.congress,
        'govinfo_hearing_to_congress_hearing' as relationship_type,
        'govinfo.package_id=congress.hearing_id' as match_rule
    from govinfo_hearings g
    join congress_hearings h
      on h.hearing_id = g.package_parts[2] || g.jacket_number || '-' || g.package_parts[1]

),

govinfo_to_congress_fallback_rows as (
    -- GovInfo's CHRG prefix records the Serial Set chamber, while the
    -- Congress.gov payload sometimes reports a different chamber (and older
    -- crawls used a ``nochamber`` route). The jacket number plus Congress is
    -- the stable cross-source identity in that case. ``joint`` is a canonical
    -- value of the hearing chamber field, not a separate hearing relation.
    -- A GovInfo JHRG package therefore resolves only to a joint hearing; an
    -- HHRG/SHRG package may resolve to a uniquely identified joint hearing
    -- when Congress.gov's committee evidence canonicalizes that same jacket
    -- number as joint. It is never copied into separate House/Senate rows.
    select distinct
        g.source_id,
        h.hearing_id as target_id,
        g.congress,
        g.package_parts[2] as package_kind
    from govinfo_hearings g
    join congress_hearings h
     on h.congress = g.package_parts[1]::integer
     and h.hearing_jacketnumber::text = g.jacket_number
     and (
          (g.package_parts[2] = 'hhrg' and h.chamber in ('house', 'joint'))
       or (g.package_parts[2] = 'shrg' and h.chamber in ('senate', 'joint'))
       or (g.package_parts[2] = 'jhrg' and h.chamber = 'joint')
     )
    where not exists (
        select 1
        from congress_hearings exact
        where exact.hearing_id = g.package_parts[2] || g.jacket_number || '-' || g.package_parts[1]
    )
      and g.package_parts[2] in ('hhrg', 'shrg')
),

govinfo_to_congress_fallback as (
    select
        source_id,
        target_id,
        congress,
        package_kind,
        count(*) over (partition by source_id) as candidate_match_count
    from govinfo_to_congress_fallback_rows
),

govinfo_to_congress as (
    select * from govinfo_to_congress_exact

    union all

    select
        source_id,
        target_id,
        congress,
        'govinfo_hearing_to_congress_hearing' as relationship_type,
        'govinfo.package_id=congress.congress+jacket_number_fallback' as match_rule
    from govinfo_to_congress_fallback
    where candidate_match_count = 1
),

govinfo_to_meeting as (
    select
        g.source_id,
        a.target_id,
        g.congress,
        'govinfo_hearing_to_committee_meeting' as relationship_type,
        'govinfo_hearing_to_congress_hearing -> associatedMeeting' as match_rule
    from govinfo_to_congress g
    join hearing_associations a
      on a.source_id = g.target_id

    union

    select
        g.source_id,
        m.source_id as target_id,
        g.congress,
        'govinfo_hearing_to_committee_meeting' as relationship_type,
        'govinfo_hearing_to_congress_hearing -> hearingTranscript' as match_rule
    from govinfo_to_congress g
    join meeting_children m
      on m.target_id = g.target_id
),

govinfo_joint_to_meeting as (
    select
        g.source_id,
        m.source_id as target_id,
        g.congress,
        'govinfo_joint_hearing_to_committee_meeting' as relationship_type,
        'govinfo.joint_package_id=committee_meeting.hearingTranscript' as match_rule
    from govinfo_hearings g
    join meeting_children m
      on g.package_parts[2] = 'jhrg'
     and m.target_id = g.package_parts[2] || g.jacket_number || '-' || g.package_parts[1]
),

all_links as (
    select * from hearing_associations
    union all
    select * from meeting_children
    union all
    select * from govinfo_to_congress
    union all
    select * from govinfo_to_meeting
    union all
    select * from govinfo_joint_to_meeting
)

select
    md5(concat_ws('|', relationship_type, source_id, target_id, match_rule)) as link_id,
    case
        when source_id like 'CHRG-%' then 'govinfo'
        else 'congress'
    end as source_system,
    source_id,
    case
        when target_id like 'CHRG-%' then 'govinfo'
        else 'congress'
    end as target_system,
    target_id,
    relationship_type,
    congress,
    match_rule
from all_links
