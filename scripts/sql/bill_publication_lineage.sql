-- Run only against a frozen build. Equal-valued contributing observations
-- remain many-to-many; this does not invent a unique winner for SQL ties.
with evidence as (
    select 'bills_versions'::text as relation, jsonb_build_array(p.govinfo_package_id) as public_key,
           to_jsonb(p) as public_row, v.govinfo_raw_response_version_id as version_id,
           'govinfo_package'::text as role, null::bigint as source_ordinal
    from bicam.bills_versions p join public.bridge_govinfo__bill_versions v on v.package_id=p.govinfo_package_id
    union all
    select 'bills_versions', jsonb_build_array(p.govinfo_package_id), to_jsonb(p),
           v.congress_raw_response_version_id, 'congress_identity', null::bigint
    from bicam.bills_versions p join public.bridge_govinfo__bill_versions v on v.package_id=p.govinfo_package_id
    where v.congress_raw_response_version_id is not null
    union all
    select 'bills_titles', jsonb_build_array(p.bill_id,p.title_type,p.title), to_jsonb(p),
           o.source_raw_response_version_id, 'value_equivalent_title_observation', o.source_ordinal
    from bicam.bills_titles p join public.int_congress_bill_title_observations o
      on p.bill_id=o.bill_id and p.title is not distinct from o.title and p.title_type is not distinct from o.title_type
     and p.bill_text_version_code is not distinct from o.bill_text_version_code
     and p.bill_text_version_name is not distinct from o.bill_text_version_name
     and p.chamber is not distinct from o.chamber and p.title_type_code is not distinct from o.title_type_code
    union all
    select 'bills_actions', jsonb_build_array(p.action_id), to_jsonb(p),
           o.source_raw_response_version_id, 'selected_action_observation', o.source_ordinal
    from bicam.bills_actions p join public.int_congress_bill_action_observations o
      on p.action_id=md5(o.bill_id || '|' || coalesce(o.action_json::text,''))::uuid::text
    union all
    select 'bills_texts', jsonb_build_array(p.bill_id,p.date,p.type), to_jsonb(p),
           o.raw_response_version_id, 'selected_rendition_observation', o.format_ordinal
    from bicam.bills_texts p join public.int_congress_bill_text_formats o
      on p.bill_id=o.bill_id
     and p.date is not distinct from (o.text_version_json->>'date')::timestamptz::date
     and p.type is not distinct from o.text_version_json->>'type'
     and case o.format_json->>'type'
         when 'Formatted Text' then p.formatted_text
         when 'PDF' then p.pdf when 'Formatted XML' then p.xml end = o.format_json->>'url'
)
select e.relation, e.public_key, e.public_row,
       jsonb_agg(jsonb_build_object('raw_version_id', e.version_id::text,
                 'role', e.role, 'source_ordinal', e.source_ordinal,
                 'endpoint', r.endpoint_url, 'first_seen_at', r.first_seen_at,
                 'last_seen_at', r.last_seen_at, 'source_response_hash', r.response_hash,
                 'resolved', r.id is not null)
                 order by e.role, e.version_id, e.source_ordinal) as evidence
from evidence e left join public.raw_response_versions r on r.id=e.version_id
group by e.relation,e.public_key,e.public_row order by e.relation,e.public_key,e.public_row
