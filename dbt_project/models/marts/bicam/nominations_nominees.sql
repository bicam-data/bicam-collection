{{ config(schema='bicam', materialized='table', tags=['bicam_public', 'nominations'], indexes=[{'columns': ['nomination_id', 'ordinal']}, {'columns': ['nomination_id']}]) }}

/*
   ``ordinal`` is the position ordinal supplied by Congress.gov, not a
   person-level key. A single position can name many nominees, so the
   source-array ordinal is used only to reconcile repeated observations of
   the same child page. Every person in the newest applicable response is
   retained; only an older duplicate at the same source-array position is
   removed. This prevents overlapping incremental/full snapshots from
   inflating the public table without collapsing legitimate duplicate
   position ordinals.
*/
with observations as (
    select
        nominees.*,
        regexp_replace(
            regexp_replace(
                nominees.source_endpoint_url,
                '([?&])(fromDateTime|toDateTime|limit)=[^&]*&?',
                '\1',
                'g'
            ),
            '[?&]$',
            ''
        ) as source_logical_endpoint
    from {{ ref('int_congress_nominee_observations') }} as nominees
    where nomination_part_id is not null
      and nominee_ordinal is not null
), ranked as (
    select
        observations.*,
        row_number() over (
            partition by source_logical_endpoint, source_ordinal
            order by source_fetched_at desc nulls last,
                     source_raw_id desc
        ) as _rn
    from observations
)
select
    {{ bicam_public_nomination_id('nomination_part_id') }} as nomination_id,
    nominee_ordinal as ordinal,
    first_name,
    middle_name,
    last_name,
    prefix,
    suffix,
    state,
    effective_date::date as effective_date,
    predecessor_name,
    corps_code
from ranked
where _rn = 1
