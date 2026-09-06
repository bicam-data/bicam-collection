/* A transient failed retry must not erase the last successful extraction from
   the stable content view used by public text marts. */
with latest_attempt as (
    select distinct on (url)
        url,
        id,
        extraction_status
    from public.html_text_versions
    order by url, fetched_at desc, id desc
), latest_success as (
    select distinct on (url)
        url,
        id
    from public.html_text_versions
    where extraction_status = 'success'
    order by url, fetched_at desc, id desc
)
select
    latest_attempt.url,
    latest_attempt.id as latest_attempt_id,
    latest_success.id as expected_success_id
from latest_attempt
join latest_success using (url)
left join public.html_text_successful_current as stable
  on stable.url = latest_success.url
 and stable.id = latest_success.id
where latest_attempt.extraction_status <> 'success'
  and stable.id is null
