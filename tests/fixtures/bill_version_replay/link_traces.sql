with child_keys as (
    select e.*,
           case source_table
             when 'bills_actions' then source_row->>'action_id'
             when 'bills_titles' then (source_row->>'bill_id') || '|' || coalesce(source_row->>'title_type','') || '|' || coalesce(source_row->>'title','') || '|' || coalesce(source_row->>'bill_text_version_code','')
             when 'bills_texts' then (source_row->>'bill_id') || '|' || coalesce(source_row->>'date','') || '|' || coalesce(source_row->>'type','')
           end as source_row_key
    from public.input_child_evidence e
)
select l.link_id, to_jsonb(l) as public_row,
       (select jsonb_agg(to_jsonb(e) order by ordinal) from child_keys e
        where e.source_table = l.source_table and e.source_row_key = l.source_row_key) as child_inputs,
       (select jsonb_agg(to_jsonb(t) order by title) from public.input_titles t
        where t.bill_id = l.bill_id and lower(trim(t.bill_text_version_name)) = lower(trim(l.source_version_name))) as title_name_evidence,
       l.govinfo_package_id as version_trace_key
from bicam.bills_version_links l order by l.link_id
