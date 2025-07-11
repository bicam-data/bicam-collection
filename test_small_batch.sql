-- Test processing a small batch of unprocessed amendments
-- This will help identify specific errors

-- First, let's see what types of amendments are unprocessed
SELECT 
    payload->>'type' as amendment_type,
    COUNT(*) as count
FROM bicam_raw_congressional.amendments_raw 
WHERE source_doc_id NOT IN (SELECT amendment_id FROM bicam.amendments)
  AND payload ? 'type'
GROUP BY payload->>'type'
ORDER BY count DESC;

-- Sample some unprocessed amendments to see their structure
SELECT 
    source_doc_id,
    payload->>'type' as amendment_type,
    payload->>'number' as amendment_number,
    payload->>'congress' as congress,
    payload->>'chamber' as chamber,
    LEFT(payload::text, 200) as payload_preview
FROM bicam_raw_congressional.amendments_raw 
WHERE source_doc_id NOT IN (SELECT amendment_id FROM bicam.amendments)
LIMIT 5;

-- Test processing just 10 unprocessed amendments
SELECT * FROM process_amendments_from_raw_table(
    batch_size := 10,
    start_offset := 0,
    max_records := 10
); 