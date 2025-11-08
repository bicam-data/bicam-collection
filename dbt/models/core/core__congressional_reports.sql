{{ config(materialized='table', tags=['core','congressional','govinfo','reports']) }}

with cong as (
  select
    cr.*
  from {{ ref('int_congressional__committeereports') }} cr
),
gov_pkg as (
  select
    g.*
  from {{ ref('int_govinfo__congressionalreports') }} g
),
gov_gran as (
  select
    gg.*
  from {{ ref('int_govinfo__congressionalreports_granules') }} gg
),
parts as (
  -- union packages (part 0) and granules (part >= 1)
  select
    p.package_set_id,
    0::int as part_number,
    p.package_id,
    null::text as granule_id
  from gov_pkg p
  union all
  select
    g.package_set_id,
    g.part_number,
    g.package_id,
    g.granule_id
  from gov_gran g
),
joined as (
  select
    pr.package_set_id,
    pr.part_number,
    pr.package_id,
    pr.granule_id,
    c.report_id,
    c.report_set_id,
    c.title_clean as congressional_title,
    c.processed_at_ts as congressional_processed_at,
    -- core identifiers
    (pr.package_set_id || '-' || pr.part_number)::text as core_id
  from parts pr
  left join cong c
    on c.report_set_id = pr.package_set_id
)
select * from joined

