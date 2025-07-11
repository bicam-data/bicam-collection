-- Simple amendment processing statistics
-- This script checks the basic processing status without complex queries

-- 1. Check total counts
SELECT 
    'Raw amendments' as description,
    COUNT(*) as count
FROM bicam_raw_congressional.amendments_raw

UNION ALL

SELECT 
    'Processed amendments' as description,
    COUNT(*) as count
FROM bicam.amendments

UNION ALL

SELECT 
    'Unprocessed amendments' as description,
    COUNT(*) as count
FROM bicam_raw_congressional.amendments_raw 
WHERE source_doc_id NOT IN (SELECT amendment_id FROM bicam.amendments);

-- 2. Sample some unprocessed amendments
SELECT 
    source_doc_id,
    payload->>'type' as amendment_type,
    payload->>'number' as amendment_number,
    payload->>'congress' as congress
FROM bicam_raw_congressional.amendments_raw 
WHERE source_doc_id NOT IN (SELECT amendment_id FROM bicam.amendments)
LIMIT 10;

-- 3. Check amendment type distribution for unprocessed
SELECT 
    payload->>'type' as amendment_type,
    COUNT(*) as count
FROM bicam_raw_congressional.amendments_raw 
WHERE source_doc_id NOT IN (SELECT amendment_id FROM bicam.amendments)
  AND payload ? 'type'
GROUP BY payload->>'type'
ORDER BY count DESC
LIMIT 10; 