{{ config(
    materialized="table",
    tags=["govinfo_bills", "govinfo_committee_reports", "govinfo_hearings", "reconciliation", "quality", "identity_review"]
) }}

/*
   Candidate evidence for GovInfo member references that omit Bioguide IDs.
   A unique full-name match is allowed to resolve at the person level when the
   source does not provide a usable temporal partition, but an explicit
   Congress value is a hard safety boundary: a candidate must have a
   Congress.gov term in that Congress.  This prevents a modern or otherwise
   unrelated person with the same surname from being assigned to a historical
   package merely because the name is unique globally.  The context match is
   retained so that source-shape errors remain visible.  Surname-only matches
   require matching Congress/state/chamber context and are never resolved when
   more than one person remains.  Exact display-name aliases observed on
   another GovInfo reference with a valid Bioguide are also evidence: this
   handles source records whose Congress/chamber partition is stale while
   retaining ambiguity whenever the same display name maps to multiple IDs.
   Abbreviated compound surnames (for example, "Beutler" for "Herrera
   Beutler") may use a suffix match, but only with Congress/state/chamber
   context and a unique resulting candidate.
*/
with unresolved as (
    select
        member_refs.*,
        regexp_replace(
            lower(
                regexp_replace(
                    regexp_replace(
                        regexp_replace(
                            coalesce(
                                member_refs.authority_fnf,
                                member_refs.authority_lnf,
                                member_refs.member_name_raw,
                                member_refs.parsed_name,
                                member_refs.authority_other
                            ),
                            '^\s*(and|or)\s+',
                            '',
                            'i'
                        ),
                        '^(The Honorable|The Hon|Honorable|Hon|Mr|Ms|Mrs|Miss|Dr|Lt)\.?[[:space:]]+',
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
    from {{ ref('int_govinfo_member_references') }} as member_refs
    where member_refs.bioguide_resolution_status <> 'provided_valid'
), normalized as (
    select
        unresolved.*,
        nullif(regexp_replace(unresolved.source_name_clean, '[^a-z0-9]', '', 'g'), '')
            as source_name_key,
        nullif(
            regexp_replace(
                case
                    when position(' ' in unresolved.source_name_clean) > 0
                        then regexp_replace(unresolved.source_name_clean, '^.*[[:space:]]', '')
                    else unresolved.source_name_clean
                end,
                '[^a-z0-9]',
                '',
                'g'
            ),
            ''
        ) as source_last_name_key,
        nullif(
            regexp_replace(
                regexp_replace(
                    regexp_replace(
                        regexp_replace(unresolved.source_name_clean, ' [a-z] ', ' ', 'g'),
                        '^[a-z] ',
                        ''
                    ),
                    ' [a-z]$',
                    ''
                ),
                '[^a-z0-9]',
                '',
                'g'
            ),
            ''
        ) as source_name_no_middle_key,
        nullif(
            regexp_replace(
                translate(unresolved.source_name_clean, 'áéíóúñç', 'aeiounc'),
                '[^a-z0-9]',
                '',
                'g'
            ),
            ''
        ) as source_name_ascii_key
    from unresolved
), congress_members as (
    select distinct
        members.bioguide_id,
        terms.congress,
        terms.start_year,
        terms.end_year,
        upper(terms.state_code) as candidate_state,
        lower(terms.chamber) as candidate_chamber,
        regexp_replace(
            lower(coalesce(members.inverted_order_name, members.direct_order_name, members.normalized_name)),
            '[^a-z0-9]',
            '',
            'g'
        ) as candidate_name_key,
        regexp_replace(
            lower(coalesce(members.direct_order_name, members.inverted_order_name, members.normalized_name)),
            '[^a-z0-9]',
            '',
            'g'
        ) as candidate_direct_name_key,
        regexp_replace(
            translate(
                lower(coalesce(members.inverted_order_name, members.direct_order_name, members.normalized_name)),
                'áéíóúñç',
                'aeiounc'
            ),
            '[^a-z0-9]',
            '',
            'g'
        ) as candidate_name_ascii_key,
        regexp_replace(
            regexp_replace(
                regexp_replace(
                    lower(coalesce(members.inverted_order_name, members.direct_order_name, members.normalized_name)),
                    '[^a-z0-9 ]',
                    '',
                    'g'
                ),
                ' [a-z] ',
                ' ',
                'g'
            ),
            ' ',
            '',
            'g'
        ) as candidate_name_no_middle_key,
        regexp_replace(
            lower(
                case
                    when members.inverted_order_name is not null
                        then split_part(members.inverted_order_name, ',', 1)
                    else regexp_replace(
                        coalesce(members.direct_order_name, members.normalized_name),
                        '.*[[:space:]]',
                        ''
                    )
                end
            ),
            '[^a-z0-9]',
            '',
            'g'
        ) as candidate_last_name_key,
        regexp_replace(
            translate(
                lower(
                    case
                        when members.inverted_order_name is not null
                            then split_part(members.inverted_order_name, ',', 1)
                        else regexp_replace(
                            coalesce(members.direct_order_name, members.normalized_name),
                            '.*[[:space:]]',
                            ''
                        )
                    end
                ),
                'áéíóúñç',
                'aeiounc'
            ),
            '[^a-z0-9]',
            '',
            'g'
        ) as candidate_last_name_ascii_key,
        coalesce(
            members.inverted_order_name,
            members.direct_order_name,
            members.normalized_name
        ) as candidate_name
    from {{ ref('stg_congress__member_detail') }} as members
    join {{ ref('stg_congress__member_terms') }} as terms
      on terms.bioguide_id = members.bioguide_id
    where members.bioguide_id is not null
), exact_context_matches as (
    select
        normalized.source_member_reference_key,
        congress_members.bioguide_id as candidate_bioguide_id,
        congress_members.candidate_name,
        congress_members.congress as candidate_congress,
        congress_members.candidate_state,
        congress_members.candidate_chamber,
        'exact_name_context' as candidate_match_basis,
        0 as match_priority
    from normalized
    join congress_members
      on (
          congress_members.candidate_name_key = normalized.source_name_key
          or congress_members.candidate_direct_name_key = normalized.source_name_key
          or congress_members.candidate_name_no_middle_key = normalized.source_name_no_middle_key
          or congress_members.candidate_last_name_ascii_key = normalized.source_name_ascii_key
          or congress_members.candidate_name_ascii_key = normalized.source_name_ascii_key
      )
     and (normalized.congress is null or congress_members.congress = normalized.congress)
     and (normalized.state is null or congress_members.candidate_state = normalized.state)
     and (
         normalized.chamber is null
         or (
             upper(trim(normalized.chamber)) = 'H'
             and congress_members.candidate_chamber = 'house of representatives'
         )
         or (
             upper(trim(normalized.chamber)) = 'S'
             and congress_members.candidate_chamber = 'senate'
         )
     )
    where normalized.source_name_key is not null
), exact_global_matches as (
    select
        normalized.source_member_reference_key,
        congress_members.bioguide_id as candidate_bioguide_id,
        congress_members.candidate_name,
        congress_members.congress as candidate_congress,
        congress_members.candidate_state,
        congress_members.candidate_chamber,
        'exact_name_global' as candidate_match_basis,
        1 as match_priority
    from normalized
    join congress_members
      on (
          congress_members.candidate_name_key = normalized.source_name_key
          or congress_members.candidate_direct_name_key = normalized.source_name_key
          or congress_members.candidate_name_no_middle_key = normalized.source_name_no_middle_key
          or congress_members.candidate_last_name_ascii_key = normalized.source_name_ascii_key
          or congress_members.candidate_name_ascii_key = normalized.source_name_ascii_key
      )
    where normalized.source_name_key is not null
      /* A supplied Congress is authoritative enough to reject a globally
         unique but temporally impossible name (for example a 119th-Congress
         member appearing in a 104th-Congress package).  If Congress is absent,
         require date or state evidence before allowing a global inference;
         otherwise leave the reference in the review queue. */
      and (
          (
              normalized.congress is not null
              and exists (
                  select 1
                  from congress_members as contextual_term
                  where contextual_term.bioguide_id = congress_members.bioguide_id
                    and contextual_term.congress = normalized.congress
              )
          )
          or (
              normalized.congress is null
              and normalized.source_issued_at is not null
              and exists (
                  select 1
                  from congress_members as active_term
                  where active_term.bioguide_id = congress_members.bioguide_id
                    and active_term.start_year
                        <= extract(year from normalized.source_issued_at)::integer
                    and (
                        active_term.end_year is null
                        or active_term.end_year
                            >= extract(year from normalized.source_issued_at)::integer
                    )
              )
          )
          or (
              normalized.congress is null
              and normalized.state is not null
              and exists (
                  select 1
                  from congress_members as state_term
                  where state_term.bioguide_id = congress_members.bioguide_id
                    and state_term.candidate_state = normalized.state
              )
          )
      )
      and not exists (
          select 1
          from exact_context_matches as context_matches
          where context_matches.source_member_reference_key
              = normalized.source_member_reference_key
      )
), last_name_context_matches as (
    select
        normalized.source_member_reference_key,
        congress_members.bioguide_id as candidate_bioguide_id,
        congress_members.candidate_name,
        congress_members.congress as candidate_congress,
        congress_members.candidate_state,
        congress_members.candidate_chamber,
        'last_name_context' as candidate_match_basis,
        2 as match_priority
    from normalized
    join congress_members
      on (
          congress_members.candidate_last_name_key = normalized.source_last_name_key
          -- GovInfo's older bill packages often store a compound surname
          -- (e.g. "McMorris Rodgers") without a given name.  The complete
          -- source token is a stronger surname key than its final word.
          or congress_members.candidate_last_name_key = normalized.source_name_key
          -- Some packages retain only the final token of a compound surname;
          -- context and the one-candidate rule below keep this conservative.
          or right(
              congress_members.candidate_last_name_key,
              length(normalized.source_last_name_key)
          ) = normalized.source_last_name_key
          or congress_members.candidate_last_name_ascii_key = normalized.source_name_ascii_key
      )
     and (normalized.congress is not null and congress_members.congress = normalized.congress)
     and (normalized.state is null or congress_members.candidate_state = normalized.state)
     and (
         normalized.chamber is null
         or (
             upper(trim(normalized.chamber)) = 'H'
             and congress_members.candidate_chamber = 'house of representatives'
         )
         or (
             upper(trim(normalized.chamber)) = 'S'
             and congress_members.candidate_chamber = 'senate'
         )
     )
    where normalized.source_last_name_key is not null
      and not exists (
          select 1
          from exact_context_matches as context_matches
          where context_matches.source_member_reference_key
              = normalized.source_member_reference_key
      )
      and not normalized.multi_legislator_flag
), name_candidate_pool as (
    select * from exact_context_matches
    union all select * from exact_global_matches
    union all select * from last_name_context_matches
), date_active_name_candidates as (
    /* Date evidence narrows a name match only when it identifies at least
       one active term. If a GovInfo date is stale or outside the term ledger,
       preserve the original candidates for explicit review. */
    select distinct
        names.source_member_reference_key,
        names.candidate_bioguide_id
    from name_candidate_pool as names
    join normalized
      on normalized.source_member_reference_key = names.source_member_reference_key
    join congress_members
      on congress_members.bioguide_id = names.candidate_bioguide_id
     and congress_members.congress = names.candidate_congress
    where normalized.source_issued_at is not null
      and congress_members.start_year <= extract(year from normalized.source_issued_at)::integer
      and (
          congress_members.end_year is null
          or congress_members.end_year >= extract(year from normalized.source_issued_at)::integer
      )
), state_name_candidates as (
    /* State metadata is another corroborating signal. As with dates, use it
       only when at least one candidate agrees; stale GovInfo state values do
       not erase otherwise valid candidates. */
    select distinct
        names.source_member_reference_key,
        names.candidate_bioguide_id
    from name_candidate_pool as names
    join normalized
      on normalized.source_member_reference_key = names.source_member_reference_key
    where normalized.state is not null
      and upper(trim(names.candidate_state)) = upper(trim(normalized.state))
), scoped_name_candidate_pool as (
    select names.*
    from name_candidate_pool as names
    where (
        not exists (
            select 1
            from date_active_name_candidates as active
            where active.source_member_reference_key = names.source_member_reference_key
        )
        or exists (
            select 1
            from date_active_name_candidates as active
            where active.source_member_reference_key = names.source_member_reference_key
              and active.candidate_bioguide_id = names.candidate_bioguide_id
        )
    )
    and (
        not exists (
            select 1
            from state_name_candidates as state_matches
            where state_matches.source_member_reference_key = names.source_member_reference_key
        )
        or exists (
            select 1
            from state_name_candidates as state_matches
            where state_matches.source_member_reference_key = names.source_member_reference_key
              and state_matches.candidate_bioguide_id = names.candidate_bioguide_id
        )
    )
), source_reference_aliases as (
    /*
       GovInfo frequently repeats a display name across packages, with the
       Bioguide present in some payloads and omitted in others. Reuse only
       exact parsed-name observations (and only valid IDs); do not infer from
       a surname or from a name that never carries a source identifier.
    */
    select distinct
        valid_refs.parsed_name as source_display_name,
        upper(trim(valid_refs.bioguide_id_raw)) as candidate_bioguide_id,
        valid_refs.parsed_name as candidate_name,
        valid_refs.congress as candidate_congress,
        upper(valid_refs.state) as candidate_state,
        valid_refs.chamber as candidate_chamber
    from {{ ref('int_govinfo_member_references') }} as valid_refs
    where valid_refs.bioguide_id_raw ~ '^[A-Z][0-9]{6}$'
      and valid_refs.parsed_name is not null
), source_reference_alias_counts as (
    select
        source_display_name,
        count(distinct candidate_bioguide_id) as candidate_id_count
    from source_reference_aliases
    group by source_display_name
), source_alias_matches as (
    select distinct
        normalized.source_member_reference_key,
        aliases.candidate_bioguide_id,
        aliases.candidate_name,
        aliases.candidate_congress,
        aliases.candidate_state,
        aliases.candidate_chamber,
        'source_reference_alias' as candidate_match_basis,
        -2 as match_priority
    from normalized
    join source_reference_aliases as aliases
      on lower(trim(aliases.source_display_name))
          = lower(trim(normalized.parsed_name))
    join source_reference_alias_counts as alias_counts
      on alias_counts.source_display_name = aliases.source_display_name
     and alias_counts.candidate_id_count = 1
     and (
         normalized.state is null
         or aliases.candidate_state is null
         or upper(trim(aliases.candidate_state)) = upper(trim(normalized.state))
     )
     and (
         normalized.chamber is null
         or aliases.candidate_chamber is null
         or (
             upper(trim(normalized.chamber)) = 'H'
             and (
                 lower(aliases.candidate_chamber) = 'house of representatives'
                 or upper(trim(aliases.candidate_chamber)) = 'H'
             )
         )
         or (
             upper(trim(normalized.chamber)) = 'S'
             and (
                 lower(aliases.candidate_chamber) = 'senate'
                 or upper(trim(aliases.candidate_chamber)) = 'S'
             )
         )
     )
    where normalized.source_name_key is not null
      and not normalized.multi_legislator_flag
      /* Do not let a stale cross-package alias override stronger contextual
         Congress.gov evidence that already produced a candidate. */
      and not exists (
          select 1
          from scoped_name_candidate_pool as existing_candidates
          where existing_candidates.source_member_reference_key
              = normalized.source_member_reference_key
      )
), known_member_name_aliases as (
    /*
       A bounded, reviewed alias set covers recurring GovInfo transcription
       and historical-name errors that cannot be recovered by exact matching
       alone (for example ``Yvette Clark`` for Yvette D. Clarke, or
       ``Rush D. Hunt`` for Rush D. Holt). The Congress range is deliberately
       explicit so these aliases cannot assign a person outside the period
       in which the member could have appeared. A curated alias is stronger
       than a generic surname candidate and therefore supersedes that pool;
       state/chamber are not hard filters here because these same GovInfo
       records are known to carry stale state and chamber values.
    */
    select *
    from (values
        ('AMATACOLEMANRADEWAGEN', 'R000600', 114, 119),
        ('CHARLESAGONZALES', 'G000544', 106, 112),
        ('DANMEUSER', 'M001204', 116, 119),
        ('DEANKELLER', 'H001041', 112, 114),
        ('DENNISHASTERT', 'H000323', 102, 111),
        ('FRENCHHILL', 'H001072', 114, 119),
        ('HERBCONWAY', 'C001136', 119, 119),
        ('JERRYMORGAN', 'M000934', 109, 115),
        ('JOHNDROCKFELLERIV', 'R000361', 99, 111),
        ('JOHNWWARNER', 'W000154', 96, 107),
        ('JONHUSTEDMORGAN', 'H001104', 119, 119),
        /* GovInfo spells Kay Bailey Hutchison's surname ``Hutchinson`` in
           one 108th-Congress Senate bill package; the Congress.gov term and
           source context uniquely identify H001016. */
        ('HUTCHINSON', 'H001016', 108, 108),
        ('JENNIFERMCCLELLAN', 'C001063', 117, 119),
        ('KARENHANDEL', 'H001078', 115, 115),
        ('KATHYMCMORRISRODGERS', 'M001159', 109, 118),
        ('MICHAELBENNET', 'B001267', 111, 119),
        ('MICHAELFBENNETT', 'B001267', 111, 119),
        ('RUSHDHUNT', 'H001032', 106, 114),
        ('YVETTECLARK', 'C001067', 110, 119)
    ) as aliases(source_name_key, candidate_bioguide_id, congress_min, congress_max)
), known_member_alias_matches as (
    select distinct
        normalized.source_member_reference_key,
        aliases.candidate_bioguide_id,
        members.candidate_name,
        members.congress as candidate_congress,
        members.candidate_state,
        members.candidate_chamber,
        'known_name_alias' as candidate_match_basis,
        -3 as match_priority
    from normalized
    join known_member_name_aliases as aliases
      on aliases.source_name_key = upper(normalized.source_name_key)
     and normalized.congress between aliases.congress_min and aliases.congress_max
    join congress_members as members
      on members.bioguide_id = aliases.candidate_bioguide_id
     and members.congress between aliases.congress_min and aliases.congress_max
    where not normalized.multi_legislator_flag
), bill_member_edges as (
    -- The reciprocity edge table is already materialized.  Reusing it avoids
    -- re-expanding the two multi-million-row Congress child views for every
    -- identity audit while retaining both sponsor and cosponsor evidence.
    select
        right_id as bill_id,
        left_id as bioguide_id,
        case
            when relationship_type = 'member_bill_sponsor' then 'SPONSOR'
            else 'COSPONSOR'
        end as edge_role
    from {{ ref('int_congress_internal_relationship_edges') }}
    where relationship_type in ('member_bill_sponsor', 'member_bill_cosponsor')
      and right_id is not null
      and left_id is not null
), bill_packages as (
    select
        replace(source_record_id, 'govinfo:', '') as package_id,
        canonical_bill_id
    from {{ ref('int_bills__govinfo') }}
    where source_record_id like 'govinfo:BILLS-%'
      and canonical_bill_id is not null
    union all
    /* Some older GovInfo package details were harvested through a child
       reference but no longer appear in the current package-list snapshot.
       Derive their canonical bill key directly from the immutable detail so
       member↔bill evidence is not lost merely because the list page rotated. */
    select distinct
        response->>'packageId' as package_id,
        {{ bill_canonical_id(
            "lower(nullif(response->>'billType', ''))",
            "nullif(response->>'billNumber', '')::integer",
            "nullif(response->>'congress', '')::integer"
        ) }} as canonical_bill_id
    from {{ ref('int_raw_responses__current') }}
    where source = 'govinfo'
      and category = 'govinfo-bills'
      and node_type = 'govinfo_bill_package_detail'
      and response->>'packageId' is not null
      and {{ bill_canonical_id(
          "lower(nullif(response->>'billType', ''))",
          "nullif(response->>'billNumber', '')::integer",
          "nullif(response->>'congress', '')::integer"
      ) }} is not null
), bill_candidate_matches as (
    -- Congress.gov's bill sponsor/cosponsor side is an additional identity
    -- corroboration for abbreviated GovInfo bill names.  It only narrows an
    -- existing name candidate; it never invents a member from bill context.
    -- GovInfo occasionally labels a sponsor as COSPONSOR (or vice versa), so
    -- the relationship role is deliberately not a hard filter here.
    select distinct
        names.source_member_reference_key,
        names.candidate_bioguide_id,
        names.candidate_name,
        names.candidate_congress,
        names.candidate_state,
        names.candidate_chamber,
        'bill_relationship' as candidate_match_basis,
        -1 as match_priority
    from scoped_name_candidate_pool as names
    join normalized
      on normalized.source_member_reference_key = names.source_member_reference_key
     and normalized.category = 'govinfo-bills'
    join bill_packages as packages
      on packages.package_id = normalized.package_id
    join bill_member_edges as edges
     on edges.bill_id = packages.canonical_bill_id
     and edges.bioguide_id = names.candidate_bioguide_id
), report_reference_bills as (
    /* Committee-report granules often cite the bill or resolution that they
       implement. Those citations provide independent, source-backed evidence
       for a name-only SUBMITTEDBY member without relying on fuzzy text. */
    select distinct
        report_id,
        bill_id as canonical_bill_id
    from {{ ref('stg_govinfo__committeereports_reference_bills') }}
    where report_id is not null
      and bill_id is not null
), report_bill_sponsor_candidate_matches as (
    select distinct
        names.source_member_reference_key,
        names.candidate_bioguide_id,
        names.candidate_name,
        names.candidate_congress,
        names.candidate_state,
        names.candidate_chamber,
        'report_bill_sponsor_relationship' as candidate_match_basis,
        -1 as match_priority
    from scoped_name_candidate_pool as names
    join normalized
      on normalized.source_member_reference_key = names.source_member_reference_key
     and normalized.category = 'govinfo-committee-reports'
    join report_reference_bills as report_refs
      on report_refs.report_id = normalized.report_id
    join bill_member_edges as edges
      on edges.bill_id = report_refs.canonical_bill_id
     and edges.bioguide_id = names.candidate_bioguide_id
     and edges.edge_role = 'SPONSOR'
), report_bill_cosponsor_candidate_matches as (
    select distinct
        names.source_member_reference_key,
        names.candidate_bioguide_id,
        names.candidate_name,
        names.candidate_congress,
        names.candidate_state,
        names.candidate_chamber,
        'report_bill_relationship' as candidate_match_basis,
        0 as match_priority
    from scoped_name_candidate_pool as names
    join normalized
      on normalized.source_member_reference_key = names.source_member_reference_key
     and normalized.category = 'govinfo-committee-reports'
    join report_reference_bills as report_refs
      on report_refs.report_id = normalized.report_id
    join bill_member_edges as edges
      on edges.bill_id = report_refs.canonical_bill_id
     and edges.bioguide_id = names.candidate_bioguide_id
     and edges.edge_role = 'COSPONSOR'
    where not exists (
        select 1
        from report_bill_sponsor_candidate_matches as sponsors
        where sponsors.source_member_reference_key
            = names.source_member_reference_key
    )
), known_alias_source_keys as (
    select distinct source_member_reference_key
    from known_member_alias_matches
), candidate_pool_raw as (
    select * from bill_candidate_matches
    union all
    select * from report_bill_sponsor_candidate_matches
    union all
    select * from report_bill_cosponsor_candidate_matches
    union all
    select * from known_member_alias_matches
    union all
    select * from source_alias_matches
    union all
    select names.*
    from scoped_name_candidate_pool as names
    where not exists (
        select 1
        from bill_candidate_matches as bill_matches
        where bill_matches.source_member_reference_key
            = names.source_member_reference_key
    )
    and not exists (
        select 1
        from report_bill_sponsor_candidate_matches as report_matches
        where report_matches.source_member_reference_key
            = names.source_member_reference_key
    )
    and not exists (
        select 1
        from report_bill_cosponsor_candidate_matches as report_matches
        where report_matches.source_member_reference_key
            = names.source_member_reference_key
    )
), candidate_pool as (
    /* A reviewed spelling alias is stronger than a generic surname/global
       candidate. Suppress the latter for that source reference so a known
       one-person correction is not reclassified as ambiguous merely because
       the misspelled surname also matched unrelated members. */
    select candidate_pool_raw.*
    from candidate_pool_raw
    where candidate_pool_raw.candidate_match_basis = 'known_name_alias'
       or not exists (
           select 1
           from known_alias_source_keys
           where known_alias_source_keys.source_member_reference_key
               = candidate_pool_raw.source_member_reference_key
       )
), deduped_candidates as (
    select distinct on (source_member_reference_key, candidate_bioguide_id)
        candidate_pool.*
    from candidate_pool
    order by source_member_reference_key, candidate_bioguide_id, match_priority
), counts as (
    select
        source_member_reference_key,
        count(*) as candidate_count,
        count(*) filter (where candidate_match_basis = 'exact_name_context')
            as exact_context_count,
        count(*) filter (where candidate_match_basis = 'exact_name_global')
            as exact_global_count
    from deduped_candidates
    group by source_member_reference_key
)
select
    normalized.source_member_reference_key,
    normalized.legacy_source_member_key,
    normalized.raw_id,
    normalized.source_issued_at,
    normalized.category,
    normalized.node_type,
    normalized.package_id,
    normalized.granule_id,
    normalized.report_id,
    normalized.member_ordinal,
    normalized.parsed_name,
    normalized.member_name_raw,
    normalized.authority_fnf,
    normalized.authority_lnf,
    normalized.state,
    normalized.chamber,
    normalized.congress,
    normalized.role,
    normalized.identity_context,
    normalized.multi_legislator_flag,
    normalized.bioguide_id_raw,
    coalesce(counts.candidate_count, 0) as candidate_count,
    deduped_candidates.candidate_rank,
    deduped_candidates.candidate_bioguide_id,
    deduped_candidates.candidate_name,
    deduped_candidates.candidate_congress,
    deduped_candidates.candidate_state,
    deduped_candidates.candidate_chamber,
    deduped_candidates.candidate_match_basis,
    case
        when deduped_candidates.candidate_bioguide_id is null then 'no_candidate'
        when counts.candidate_count = 1
            and deduped_candidates.candidate_match_basis in (
                 'exact_name_context', 'bill_relationship',
                 'report_bill_relationship', 'report_bill_sponsor_relationship',
                 'source_reference_alias',
                 'known_name_alias'
             )
            then 'unique_name_context_candidate'
        when counts.candidate_count = 1
             and deduped_candidates.candidate_match_basis = 'exact_name_global'
            then 'unique_name_global_candidate'
        when counts.candidate_count = 1
             and deduped_candidates.candidate_match_basis = 'last_name_context'
            then 'unique_last_name_context_candidate'
        else 'ambiguous_candidate'
    end as resolution_status
from normalized
left join counts
  on counts.source_member_reference_key = normalized.source_member_reference_key
left join (
    select
        deduped_candidates.*,
        row_number() over (
            partition by source_member_reference_key
            order by match_priority, candidate_bioguide_id
        ) as candidate_rank
    from deduped_candidates
) as deduped_candidates
  on deduped_candidates.source_member_reference_key
      = normalized.source_member_reference_key
