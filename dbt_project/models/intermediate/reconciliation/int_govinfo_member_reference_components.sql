{{ config(
    materialized="table",
    tags=["govinfo_bills", "govinfo_committee_reports", "govinfo_hearings", "reconciliation", "quality", "identity_review"]
) }}

/*
   A member object can contain a conjunction of legislators (most often in
   historical bill packages and SUBMITTEDBY report metadata).  The parent
   bridge deliberately does not assign one Bioguide to that object.  Expand
   it here so each named component can be linked independently while the
   original source key and ordinal remain traceable.
*/
with composite_refs as (
    select
        refs.*,
        regexp_replace(
            regexp_replace(
                regexp_replace(
                    coalesce(refs.parsed_name, refs.member_name_raw, refs.authority_fnf, refs.authority_lnf),
                    '^\s*(and|&)\s+',
                    '',
                    'i'
                ),
                '\([^)]*\)',
                '',
                'g'
            ),
            '\s+of\s+.*$',
            '',
            'i'
        ) as composite_name
    from {{ ref('bridge_govinfo__member_references') }} as refs
    where refs.multi_legislator_flag
), split_components as (
    select
        composite_refs.*,
        component.ordinality::integer as component_ordinal,
        trim(
            regexp_replace(
                regexp_replace(component.component_name, '^\s*(and|&)\s+', '', 'i'),
                '^(Mr|Ms|Mrs|Miss|Dr)\.?\s+',
                '',
                'i'
            )
        ) as component_name
    from composite_refs
    cross join lateral regexp_split_to_table(
        composite_refs.composite_name,
        '\s*,?\s+(and|&)\s+'
    ) with ordinality as component(component_name, ordinality)
), normalized as (
    select
        split_components.*,
        split_components.source_member_reference_key || ':'
            || split_components.component_ordinal::text as component_key,
        nullif(
            regexp_replace(lower(split_components.component_name), '[^a-z0-9 ]', '', 'g'),
            ''
        ) as component_name_clean,
        nullif(
            regexp_replace(
                lower(
                    case
                        when position(' ' in split_components.component_name) > 0
                            then regexp_replace(split_components.component_name, '^.*[[:space:]]', '')
                        else split_components.component_name
                    end
                ),
                '[^a-z0-9]',
                '',
                'g'
            ),
            ''
        ) as component_last_name_key
    from split_components
    where nullif(trim(split_components.component_name), '') is not null
), congress_members as (
    select distinct
        members.bioguide_id,
        terms.congress,
        upper(terms.state_code) as candidate_state,
        lower(terms.chamber) as candidate_chamber,
        regexp_replace(
            translate(
                lower(split_part(coalesce(members.inverted_order_name, members.direct_order_name), ',', 1)),
                'áéíóúñç',
                'aeiounc'
            ),
            '[^a-z0-9]',
            '',
            'g'
        ) as candidate_last_name_key,
        coalesce(members.inverted_order_name, members.direct_order_name, members.normalized_name)
            as candidate_name
    from {{ ref('stg_congress__member_detail') }} as members
    join {{ ref('stg_congress__member_terms') }} as terms
      on terms.bioguide_id = members.bioguide_id
    where members.bioguide_id is not null
), candidates as (
    select
        normalized.component_key,
        congress_members.bioguide_id as candidate_bioguide_id,
        congress_members.candidate_name,
        congress_members.candidate_state,
        congress_members.candidate_chamber,
        false as matches_source_bioguide
    from normalized
    join congress_members
      on congress_members.congress = normalized.congress
     and (
         normalized.chamber is null
         or (upper(trim(normalized.chamber)) = 'H'
             and congress_members.candidate_chamber = 'house of representatives')
         or (upper(trim(normalized.chamber)) = 'S'
             and congress_members.candidate_chamber = 'senate')
     )
     and (
         congress_members.candidate_last_name_key = normalized.component_last_name_key
         or congress_members.candidate_last_name_key = normalized.component_name_clean
       )
    union all
    /*
       Composite objects can carry one valid Bioguide for only one named
       component.  Attach that identifier only when the component surname
       matches the identified member in the same Congress/chamber; this keeps
       a Berman identifier from being copied onto a neighboring Hastings
       component in the same source object.
    */
    select
        normalized.component_key,
        source_member.bioguide_id as candidate_bioguide_id,
        source_member.candidate_name,
        source_member.candidate_state,
        source_member.candidate_chamber,
        true as matches_source_bioguide
    from normalized
    join congress_members as source_member
      on source_member.bioguide_id = upper(trim(normalized.bioguide_id_raw))
     and source_member.congress = normalized.congress
     and source_member.candidate_last_name_key = normalized.component_last_name_key
     and (
         normalized.chamber is null
         or (upper(trim(normalized.chamber)) = 'H'
             and source_member.candidate_chamber = 'house of representatives')
         or (upper(trim(normalized.chamber)) = 'S'
             and source_member.candidate_chamber = 'senate')
     )
    where normalized.bioguide_id_raw ~ '^[A-Za-z][0-9]{6}$'
), candidate_counts as (
    select
        component_key,
        count(distinct candidate_bioguide_id) as candidate_count,
        count(distinct candidate_bioguide_id)
            filter (where matches_source_bioguide) as source_bioguide_candidate_count
    from candidates
    group by component_key
)
select
    normalized.component_key,
    normalized.source_member_reference_key,
    normalized.raw_id,
    normalized.category,
    normalized.node_type,
    normalized.package_id,
    normalized.granule_id,
    normalized.report_id,
    normalized.congress,
    normalized.state,
    normalized.chamber,
    normalized.identity_context,
    normalized.component_ordinal,
    normalized.component_name,
    normalized.component_name_clean,
    normalized.component_last_name_key,
    candidates.candidate_bioguide_id,
    candidates.candidate_name,
    candidates.candidate_state,
    candidates.candidate_chamber,
    coalesce(candidate_counts.candidate_count, 0) as candidate_count,
    coalesce(candidate_counts.source_bioguide_candidate_count, 0)
        as source_bioguide_candidate_count,
    coalesce(candidates.matches_source_bioguide, false) as matches_source_bioguide,
    case
        when coalesce(candidate_counts.source_bioguide_candidate_count, 0) = 1
            then 'source_bioguide_evidence'
        when coalesce(candidate_counts.candidate_count, 0) = 1
            then 'unique_name_context_candidate'
        when coalesce(candidate_counts.candidate_count, 0) = 0
            then 'no_candidate'
        else 'ambiguous_candidate'
    end as resolution_status
from normalized
left join candidate_counts using (component_key)
left join candidates using (component_key)
