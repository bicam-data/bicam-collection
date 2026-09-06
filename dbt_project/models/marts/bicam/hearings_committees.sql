{{ config(schema='bicam', materialized='table', tags=['bicam_public', 'hearings'], indexes=[{'columns': ['hearing_id', 'committee_code'], 'unique': true}, {'columns': ['committee_code']}]) }}

with congress_rows as (
    select
        public_ids.public_hearing_id as hearing_id,
        committees.committee_id as committee_code,
        committees.committee_name
    from {{ ref('stg_congress__hearing_committees') }} as committees
    join {{ ref('int_congress_hearing_public_id_map') }} as public_ids
      on public_ids.source_hearing_id = committees.hearing_id
    where committees.hearing_id is not null and committees.committee_id is not null
),
govinfo_observations as (
    select
        granules.hearing_id,
        lower(nullif(trim(committee.value #>> '{authorityId}'), '')) as source_committee_id,
        nullif(committee.value #>> '{committeeName}', '') as committee_name
    from {{ ref('int_govinfo_hearing_granules') }} as granules
    cross join lateral {{ unnest_array('granules.granule_json', 'committees', 'committee') }}
),
govinfo_rows as (
    select
        observations.hearing_id,
        committee_map.committee_id as committee_code,
        observations.committee_name
    from govinfo_observations as observations
    join {{ ref('int_govinfo_committee_id_map') }} as committee_map
      on committee_map.source_committee_id = observations.source_committee_id
    where committee_map.committee_id is not null
),
all_rows as (
    select * from congress_rows
    union all
    select * from govinfo_rows
),
ranked as (
    select
        all_rows.*,
        row_number() over (
            partition by hearing_id, committee_code
            order by (committee_name is not null) desc, committee_name
        ) as _rank
    from all_rows
)

select hearing_id, committee_code, committee_name
from ranked
where _rank = 1
