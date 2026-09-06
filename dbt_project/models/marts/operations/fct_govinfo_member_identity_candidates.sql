{{ config(
    materialized="table",
    tags=["operations", "quality", "govinfo", "identity_review"]
) }}

/*
   Produce evidence for a reviewer, never an automatic identity decision.
   Congress.gov member terms provide the independent Congress/state context
   needed to investigate missing GovInfo Bioguide IDs. Exact normalized-name
   matches are preferred; when a source provides only a surname, a last-name
   plus Congress/state candidate is retained as weaker evidence. Blank or
   malformed GovInfo names remain manual work.
*/
with unresolved as (
    select *
    from {{ ref('fct_govinfo_member_identity_exceptions') }}
), unresolved_named as (
    select
        unresolved.*,
        coalesce(
            nullif(trim(unresolved.member_name_raw), ''),
            nullif(trim(unresolved.parsed_name), '')
        ) as source_member_name,
        regexp_replace(
            lower(coalesce(
                nullif(trim(unresolved.member_name_raw), ''),
                nullif(trim(unresolved.parsed_name), '')
            )),
            '[^a-z0-9]',
            '',
            'g'
        ) as source_name_key,
        regexp_replace(
            lower(
                regexp_replace(
                    regexp_replace(
                        coalesce(
                            nullif(trim(unresolved.member_name_raw), ''),
                            nullif(trim(unresolved.parsed_name), '')
                        ),
                        '^(Mr|Ms|Mrs|Miss|Dr)\\.?[[:space:]]+',
                        '',
                        'i'
                    ),
                    '[[:space:]]+of[[:space:]].*$',
                    '',
                    'i'
                )
            ),
            '[^a-z0-9 -]',
            '',
            'g'
        ) as source_name_clean
    from unresolved
), congress_members as (
    select
        members.bioguide_id,
        members.normalized_name,
        members.inverted_order_name,
        members.direct_order_name,
        terms.congress,
        upper(terms.state_code) as state_code,
        terms.chamber,
        regexp_replace(
            lower(coalesce(members.inverted_order_name, members.direct_order_name, members.normalized_name)),
            '[^a-z0-9]', '', 'g'
        ) as congress_name_key,
        regexp_replace(
            lower(case
                when members.inverted_order_name is not null
                    then split_part(members.inverted_order_name, ',', 1)
                else regexp_replace(
                    coalesce(members.direct_order_name, members.normalized_name),
                    '.*[[:space:]]',
                    ''
                )
            end),
            '[^a-z0-9-]', '', 'g'
        ) as congress_last_name_key
    from {{ ref('stg_congress__member_detail') }} as members
    join {{ ref('stg_congress__member_terms') }} as terms
      on terms.bioguide_id = members.bioguide_id
), exact_matches as (
    select
        unresolved.source_member_key,
        congress_members.bioguide_id as candidate_bioguide_id,
        congress_members.congress as candidate_congress,
        congress_members.state_code as candidate_state_code,
        congress_members.chamber as candidate_chamber,
        case
            when upper(unresolved.state) = congress_members.state_code
                then 'name_congress_state'
            else 'name_congress'
        end as candidate_match_basis,
        0 as match_priority
    from unresolved_named as unresolved
    join congress_members
      on congress_members.congress = unresolved.congress
     and (
         unresolved.state is null
         or congress_members.state_code = upper(unresolved.state)
     )
     and (
         unresolved.chamber is null
         or (
             upper(trim(unresolved.chamber)) = 'H'
             and lower(trim(congress_members.chamber)) = 'house of representatives'
         )
         or (
             upper(trim(unresolved.chamber)) = 'S'
             and lower(trim(congress_members.chamber)) = 'senate'
         )
     )
     and nullif(unresolved.source_name_key, '')
         is not null
     and nullif(unresolved.source_name_key, '')
         = congress_members.congress_name_key
), last_name_matches as (
    select
        unresolved.source_member_key,
        congress_members.bioguide_id as candidate_bioguide_id,
        congress_members.congress as candidate_congress,
        congress_members.state_code as candidate_state_code,
        congress_members.chamber as candidate_chamber,
        case
            when upper(unresolved.state) = congress_members.state_code
                then 'last_name_congress_state'
            else 'last_name_congress'
        end as candidate_match_basis,
        1 as match_priority
    from unresolved_named as unresolved
    join congress_members
      on congress_members.congress = unresolved.congress
     and congress_members.congress_last_name_key = regexp_replace(
            regexp_replace(unresolved.source_name_clean, '.*[[:space:]]', ''),
            '[^a-z0-9-]', '', 'g'
        )
     and (
         unresolved.state is null
         or congress_members.state_code = upper(unresolved.state)
     )
     and (
         unresolved.chamber is null
         or (
             upper(trim(unresolved.chamber)) = 'H'
             and lower(trim(congress_members.chamber)) = 'house of representatives'
         )
         or (
             upper(trim(unresolved.chamber)) = 'S'
             and lower(trim(congress_members.chamber)) = 'senate'
         )
     )
    where not unresolved.multi_legislator_flag
      and nullif(unresolved.source_name_clean, '') is not null
      and not exists (
          select 1
          from exact_matches as exact
          where exact.source_member_key = unresolved.source_member_key
      )
), candidate_matches as (
    select * from exact_matches
    union all
    select * from last_name_matches
), ranked_candidates as (
    select
        candidate_matches.*,
        row_number() over (
            partition by candidate_matches.source_member_key
            order by candidate_matches.match_priority,
                case when candidate_matches.candidate_match_basis like '%_state' then 0 else 1 end,
                candidate_matches.candidate_bioguide_id
        ) as candidate_rank
    from candidate_matches
), candidate_counts as (
    select source_member_key, count(*) as candidate_count
    from ranked_candidates
    group by source_member_key
)
select
    unresolved.source_member_key,
    unresolved.report_id,
    unresolved.report_set_id,
    unresolved.package_id,
    unresolved.granule_id,
    unresolved.congress,
    unresolved.state,
    unresolved.chamber,
    unresolved.member_name_raw,
    unresolved.bioguide_id_raw,
    unresolved.bioguide_resolution_status,
    unresolved.role,
    unresolved.identity_context,
    unresolved.committee_ids,
    unresolved.committee_names,
    coalesce(candidate_counts.candidate_count, 0) as candidate_count,
    ranked_candidates.candidate_rank,
    ranked_candidates.candidate_bioguide_id,
    ranked_candidates.candidate_congress,
    ranked_candidates.candidate_state_code,
    ranked_candidates.candidate_chamber,
    ranked_candidates.candidate_match_basis,
    case
        when ranked_candidates.candidate_bioguide_id is null then 'manual_review_no_candidate'
        when candidate_counts.candidate_count = 1 then 'manual_review_single_candidate'
        else 'manual_review_multiple_candidates'
    end as review_status
from unresolved
left join candidate_counts
  on candidate_counts.source_member_key = unresolved.source_member_key
left join ranked_candidates
  on ranked_candidates.source_member_key = unresolved.source_member_key
