-- Debug Amendment Processing Errors
-- This script helps identify why amendment processing is failing

-- 1. Check the structure of the raw data
SELECT 
    COUNT(*) as total_records,
    COUNT(CASE WHEN payload IS NULL THEN 1 END) as null_payloads,
    COUNT(CASE WHEN source_doc_id IS NULL THEN 1 END) as null_source_doc_ids
FROM bicam_raw_congressional.amendments_raw;

-- 2. Sample some raw records to see the structure
SELECT 
    source_doc_id,
    LEFT(payload::text, 200) as payload_preview
FROM bicam_raw_congressional.amendments_raw 
LIMIT 5;

-- 3. Check for JSON parsing issues
SELECT 
    source_doc_id,
    CASE 
        WHEN payload::text IS NULL THEN 'NULL payload'
        WHEN jsonb_typeof(payload) != 'object' THEN 'Not an object: ' || jsonb_typeof(payload)
        ELSE 'Valid JSON'
    END as json_status
FROM bicam_raw_congressional.amendments_raw 
LIMIT 10;

-- 4. Check for missing required fields
SELECT 
    source_doc_id,
    CASE 
        WHEN NOT (payload ? 'type') THEN 'Missing type'
        WHEN NOT (payload ? 'number') THEN 'Missing number'
        WHEN NOT (payload ? 'congress') THEN 'Missing congress'
        WHEN NOT (payload ? 'chamber') THEN 'Missing chamber'
        ELSE 'All required fields present'
    END as missing_fields,
    payload->>'type' as amendment_type,
    payload->>'number' as amendment_number,
    payload->>'congress' as congress
FROM bicam_raw_congressional.amendments_raw 
WHERE NOT (payload ? 'type' AND payload ? 'number' AND payload ? 'congress')
LIMIT 10;

-- 5. Test ID generation for a few records
WITH sample_data AS (
    SELECT 
        source_doc_id,
        payload,
        payload->>'type' as amendment_type,
        payload->>'number' as amendment_number,
        payload->>'congress' as congress
    FROM bicam_raw_congressional.amendments_raw 
    WHERE payload ? 'type' AND payload ? 'number' AND payload ? 'congress'
    LIMIT 5
)
SELECT 
    source_doc_id,
    amendment_type,
    amendment_number,
    congress,
    -- Test the ID generation
    lower(amendment_type) || amendment_number || '-' || congress as generated_id,
    -- Check if this ID already exists in bicam.amendments
    CASE 
        WHEN EXISTS (SELECT 1 FROM bicam.amendments WHERE amendment_id = lower(amendment_type) || amendment_number || '-' || congress)
        THEN 'Already exists'
        ELSE 'New'
    END as status
FROM sample_data;

-- 6. Check for data type issues
SELECT 
    source_doc_id,
    payload->>'congress' as congress_raw,
    CASE 
        WHEN payload->>'congress' ~ '^[0-9]+$' THEN 'Valid integer'
        ELSE 'Invalid integer: ' || payload->>'congress'
    END as congress_validation,
    payload->>'number' as number_raw,
    CASE 
        WHEN payload->>'number' ~ '^[0-9]+$' THEN 'Valid integer'
        ELSE 'Invalid integer: ' || payload->>'number'
    END as number_validation
FROM bicam_raw_congressional.amendments_raw 
WHERE NOT (payload->>'congress' ~ '^[0-9]+$' AND payload->>'number' ~ '^[0-9]+$')
LIMIT 10;

-- 7. Check the difference between source_doc_id and generated amendment_id
SELECT 
    source_doc_id,
    payload->>'type' as amendment_type,
    payload->>'number' as amendment_number,
    payload->>'congress' as congress,
    lower(payload->>'type') || (payload->>'number')::TEXT || '-' || (payload->>'congress')::TEXT as generated_id,
    CASE 
        WHEN source_doc_id = lower(payload->>'type') || (payload->>'number')::TEXT || '-' || (payload->>'congress')::TEXT
        THEN 'Match'
        ELSE 'Mismatch'
    END as id_match
FROM bicam_raw_congressional.amendments_raw 
WHERE payload ? 'type' AND payload ? 'number' AND payload ? 'congress'
LIMIT 10;

-- 8. Count how many would be processed vs filtered out
SELECT 
    COUNT(*) as total_raw,
    COUNT(CASE WHEN source_doc_id NOT IN (SELECT amendment_id FROM bicam.amendments) THEN 1 END) as unprocessed,
    COUNT(CASE WHEN source_doc_id IN (SELECT amendment_id FROM bicam.amendments) THEN 1 END) as already_processed
FROM bicam_raw_congressional.amendments_raw;

-- 9. Check for specific error patterns in the data
SELECT 
    payload->>'type' as amendment_type,
    COUNT(*) as count
FROM bicam_raw_congressional.amendments_raw 
WHERE payload ? 'type'
GROUP BY payload->>'type'
ORDER BY count DESC; 