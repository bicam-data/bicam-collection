/* Congressional Record is paused out of the main-11 text-extraction scope.
   Keep the source link in amendments_texts, but never surface extracted
   Congressional Record prose as amendment text. */
select amendment_id, html, raw_text
from {{ ref('amendments_texts') }}
where html ~* '/crec/'
  and raw_text is not null
