select url, extraction_status
from public.html_text_current
where extraction_status = 'success'
  and nullif(trim(text_content), '') is null
