{{ config(
    materialized="table",
    tags=["govinfo_committee_reports", "reconciliation", "quality", "identity_review"]
) }}

/*
   GovInfo sometimes stores two legislators in one SUBMITTEDBY member object,
   for example "Mr. Hefley and Mr. Berman", while supplying at most one
   Bioguide ID. Keep the source object intact and expose one evidence row per
   named component. A component is only safe to infer when its Congress.gov
   last-name match is unique for that Congress; otherwise it remains an
   explicit review item.
*/
with report_metadata as (
    select
        report_id,
        max(congress) as report_congress
    from {{ ref('stg_govinfo__committeereports') }}
    where report_id is not null
    group by report_id
), source_members as (
    select
        source_member_key,
        report_id,
        report_set_id,
        package_id,
        granule_id,
        congress,
        report_metadata.report_congress,
        coalesce(report_metadata.report_congress, congress) as matching_congress,
        state,
        chamber,
        member_name_raw,
        parsed_name,
        bioguide_id_raw,
        coalesce(nullif(trim(parsed_name), ''), nullif(trim(member_name_raw), '')) as composite_name
    from {{ ref('stg_govinfo__committeereports_members') }}
    left join report_metadata using (report_id)
    where multi_legislator_flag
), split_components as (
    select
        source_members.*,
        component.ordinality::integer as component_ordinal,
        trim(regexp_replace(component.component_name, '^(Mr|Ms|Mrs|Miss|Dr)\\.?[[:space:]]+', '', 'i')) as component_name
    from source_members
    cross join lateral regexp_split_to_table(
        source_members.composite_name,
        '[[:space:]]+(and|&)[[:space:]]+'
    ) with ordinality as component(component_name, ordinality)
), normalized_components as (
    select
        split_components.*,
        source_member_key || ':' || component_ordinal::text as component_key,
        regexp_replace(
            lower(regexp_replace(component_name, '[^[:alnum:] -]', '', 'g')),
            '^.*[[:space:]]',
            ''
        ) as component_last_name_key
    from split_components
    where nullif(trim(component_name), '') is not null
), congress_members as (
    select distinct
        members.bioguide_id,
        terms.congress,
        upper(terms.state_code) as candidate_state,
        regexp_replace(
            lower(split_part(coalesce(members.inverted_order_name, members.direct_order_name), ',', 1)),
            '[^[:alnum:]-]',
            '',
            'g'
        ) as candidate_last_name_key,
        coalesce(members.inverted_order_name, members.direct_order_name, members.normalized_name) as candidate_name,
        terms.chamber as candidate_chamber
    from {{ ref('stg_congress__member_detail') }} as members
    join {{ ref('stg_congress__member_terms') }} as terms
      on terms.bioguide_id = members.bioguide_id
    where members.bioguide_id is not null
), candidates as (
    select
        components.component_key,
        congress_members.bioguide_id as candidate_bioguide_id,
        congress_members.candidate_name,
        congress_members.candidate_state,
        congress_members.candidate_chamber,
        (components.bioguide_id_raw is not null
            and upper(trim(components.bioguide_id_raw)) = congress_members.bioguide_id) as matches_source_bioguide,
        (components.state is not null
            and upper(trim(components.state)) = congress_members.candidate_state) as matches_source_state
    from normalized_components as components
    join congress_members
      on congress_members.congress = components.matching_congress
     and (
         components.chamber is null
         or (
             upper(trim(components.chamber)) = 'H'
             and lower(trim(congress_members.candidate_chamber)) = 'house of representatives'
         )
         or (
             upper(trim(components.chamber)) = 'S'
             and lower(trim(congress_members.candidate_chamber)) = 'senate'
         )
     )
     and congress_members.candidate_last_name_key = components.component_last_name_key
), candidate_counts as (
    select
        component_key,
        count(*) as candidate_count,
        count(*) filter (where matches_source_bioguide) as source_bioguide_candidate_count,
        count(*) filter (where matches_source_state) as state_candidate_count
    from candidates
    group by component_key
)
select
    components.source_member_key,
    components.component_key,
    components.report_id,
    components.report_set_id,
    components.package_id,
    components.granule_id,
    components.congress,
    components.report_congress,
    components.matching_congress,
    components.state,
    components.chamber,
    components.member_name_raw,
    components.parsed_name,
    components.bioguide_id_raw,
    components.component_ordinal,
    components.component_name,
    components.component_last_name_key,
    candidates.candidate_bioguide_id,
    candidates.candidate_name,
    candidates.candidate_state,
    candidates.candidate_chamber,
    coalesce(candidate_counts.candidate_count, 0) as candidate_count,
    coalesce(candidate_counts.source_bioguide_candidate_count, 0) as source_bioguide_candidate_count,
    coalesce(candidate_counts.state_candidate_count, 0) as state_candidate_count,
    coalesce(candidates.matches_source_bioguide, false) as matches_source_bioguide,
    coalesce(candidates.matches_source_state, false) as matches_source_state,
    case
        when coalesce(candidate_counts.source_bioguide_candidate_count, 0) = 1
            then 'source_bioguide_evidence'
        when coalesce(candidate_counts.candidate_count, 0) = 1
            then 'unique_name_congress_candidate'
        when coalesce(candidate_counts.candidate_count, 0) = 0
            then 'no_candidate'
        else 'ambiguous_candidate'
    end as resolution_status
from normalized_components as components
left join candidate_counts
  on candidate_counts.component_key = components.component_key
left join candidates
  on candidates.component_key = components.component_key
