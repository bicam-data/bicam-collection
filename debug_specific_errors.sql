-- Debug specific amendment processing errors
-- This script examines the actual unprocessed records to identify issues

-- 1. Look at the first few unprocessed amendments in detail
WITH unprocessed_sample AS (
    SELECT 
        source_doc_id,
        payload,
        payload->>'type' as amendment_type,
        payload->>'number' as amendment_number,
        payload->>'congress' as congress,
        payload->>'chamber' as chamber,
        payload->>'purpose' as purpose,
        payload->>'updateDate' as update_date
    FROM bicam_raw_congressional.amendments_raw 
    WHERE source_doc_id NOT IN (SELECT amendment_id FROM bicam.amendments)
    LIMIT 5
)
SELECT 
    source_doc_id,
    amendment_type,
    amendment_number,
    congress,
    chamber,
    purpose,
    update_date,
    -- Test the ID generation
    lower(amendment_type) || amendment_number || '-' || congress as generated_id,
    -- Check if the generated ID matches source_doc_id
    CASE 
        WHEN source_doc_id = lower(amendment_type) || amendment_number || '-' || congress
        THEN 'Match'
        ELSE 'Mismatch - Source: ' || source_doc_id || ', Generated: ' || lower(amendment_type) || amendment_number || '-' || congress
    END as id_comparison
FROM unprocessed_sample;

-- 2. Check for data type issues in unprocessed records
SELECT 
    source_doc_id,
    payload->>'congress' as congress_raw,
    payload->>'number' as number_raw,
    CASE 
        WHEN payload->>'congress' ~ '^[0-9]+$' THEN 'Valid'
        ELSE 'Invalid: ' || COALESCE(payload->>'congress', 'NULL')
    END as congress_valid,
    CASE 
        WHEN payload->>'number' ~ '^[0-9]+$' THEN 'Valid'
        ELSE 'Invalid: ' || COALESCE(payload->>'number', 'NULL')
    END as number_valid
FROM bicam_raw_congressional.amendments_raw 
WHERE source_doc_id NOT IN (SELECT amendment_id FROM bicam.amendments)
  AND (NOT (payload->>'congress' ~ '^[0-9]+$') OR NOT (payload->>'number' ~ '^[0-9]+$'))
LIMIT 10;

-- 3. Check for missing required fields in unprocessed records
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
    payload->>'congress' as congress,
    payload->>'chamber' as chamber
FROM bicam_raw_congressional.amendments_raw 
WHERE source_doc_id NOT IN (SELECT amendment_id FROM bicam.amendments)
  AND NOT (payload ? 'type' AND payload ? 'number' AND payload ? 'congress')
LIMIT 10;

-- 4. Test the extract_amendment_data function on a problematic record
-- Let's try to manually extract data from the first unprocessed record
WITH test_record AS (
    SELECT payload
    FROM bicam_raw_congressional.amendments_raw 
    WHERE source_doc_id NOT IN (SELECT amendment_id FROM bicam.amendments)
    LIMIT 1
)
SELECT 
    -- Test the ID generation
    lower(payload->>'type') || (payload->>'number')::TEXT || '-' || (payload->>'congress')::TEXT as test_amendment_id,
    -- Test other extractions
    payload->>'type' as test_type,
    payload->>'number' as test_number,
    payload->>'congress' as test_congress,
    payload->>'chamber' as test_chamber,
    payload->>'purpose' as test_purpose,
    -- Test timestamp parsing
    CASE 
        WHEN payload->>'proposedDate' IS NOT NULL 
        THEN (payload->>'proposedDate')::TIMESTAMP WITH TIME ZONE
        ELSE NULL
    END as test_proposed_date,
    -- Test boolean logic
    CASE WHEN payload ? 'amendedBill' THEN TRUE ELSE FALSE END as test_is_bill_amendment
FROM test_record; 