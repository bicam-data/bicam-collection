-- Convert JSON Amendment Data to SQL Tables
-- This script processes JSON payloads and inserts data into the staging_congressional schema tables
-- Reads from bicam_raw_congressional.amendments_raw

-- Function to safely extract JSON values with proper type casting
CREATE OR REPLACE FUNCTION extract_amendment_data(json_data JSONB)
RETURNS TABLE (
    amendment_id TEXT,
    amendment_type TEXT,
    amendment_number TEXT,
    congress INTEGER,
    chamber TEXT,
    purpose TEXT,
    description TEXT,
    proposed_at TIMESTAMP WITH TIME ZONE,
    submitted_at TIMESTAMP WITH TIME ZONE,
    is_bill_amendment BOOLEAN,
    is_treaty_amendment BOOLEAN,
    is_amendment_amendment BOOLEAN,
    notes TEXT,
    actions_count INTEGER,
    cosponsors_count INTEGER,
    amendments_to_amendment_count INTEGER,
    updated_at TIMESTAMP WITH TIME ZONE
) AS $$
BEGIN
    RETURN QUERY
    SELECT
                -- Generate amendment_id according to format: {type}{number}-{congress}
        -- Examples: SAMDT584-112, HAMDT96-116
        lower(json_data->>'type') || (json_data->>'number')::TEXT || '-' || (json_data->>'congress')::TEXT AS amendment_id,

        lower(json_data->>'type') AS amendment_type,
        json_data->>'number' AS amendment_number,
        (json_data->>'congress')::INTEGER AS congress,
        CASE
            WHEN json_data->>'chamber' LIKE 'S%' THEN 'senate'
            WHEN json_data->>'chamber' LIKE 'H%' THEN 'house'
            ELSE NULL
        END AS chamber,
        json_data->>'purpose' AS purpose,
        COALESCE(json_data->>'description', json_data->>'purpose') AS description,
        CASE 
            WHEN json_data->>'proposedDate' IS NOT NULL
            THEN (json_data->>'proposedDate')::TIMESTAMP WITH TIME ZONE
            ELSE NULL
        END AS proposed_at,
        CASE 
            WHEN json_data->>'submittedDate' IS NOT NULL 
            THEN (json_data->>'submittedDate')::TIMESTAMP WITH TIME ZONE
            ELSE NULL
        END AS submitted_at,

        -- Determine amendment types based on presence of related objects
        CASE WHEN json_data ? 'amendedBill' THEN TRUE ELSE FALSE END AS is_bill_amendment,
        CASE WHEN json_data ? 'amendedTreaty' THEN TRUE ELSE FALSE END AS is_treaty_amendment,
        CASE WHEN json_data ? 'amendedAmendment' THEN TRUE ELSE FALSE END AS is_amendment_amendment,

        COALESCE(json_data->>'notes', 
                CASE 
                    WHEN json_data ? 'latestAction' AND json_data->'latestAction' ? 'text'
                    THEN json_data->'latestAction'->>'text'
                    ELSE NULL
                END) AS notes,

        -- Extract counts from nested objects
        CASE
            WHEN json_data ? 'actions' AND json_data->'actions' ? 'count'
            THEN (json_data->'actions'->>'count')::INTEGER
            ELSE 0
        END AS actions_count,

        CASE
            WHEN json_data ? 'cosponsors' AND json_data->'cosponsors' ? 'count'
            THEN (json_data->'cosponsors'->>'count')::INTEGER
            ELSE 0
        END AS cosponsors_count,

        CASE
            WHEN json_data ? 'amendmentsToAmendment' AND json_data->'amendmentsToAmendment' ? 'count'
            THEN (json_data->'amendmentsToAmendment'->>'count')::INTEGER
            ELSE 0
        END AS amendments_to_amendment_count,

        CASE
            WHEN json_data->>'updateDate' IS NOT NULL
            THEN (json_data->>'updateDate')::TIMESTAMP WITH TIME ZONE
            ELSE CURRENT_TIMESTAMP
        END AS updated_at;
END;
$$ LANGUAGE plpgsql;

-- Function to extract sponsor data
CREATE OR REPLACE FUNCTION extract_amendment_sponsors(json_data JSONB)
RETURNS TABLE (
    amendment_id TEXT,
    bioguide_id TEXT
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        -- Generate amendment_id according to format: {type}{number}-{congress}
        lower(json_data->>'type') || (json_data->>'number')::TEXT || '-' || (json_data->>'congress')::TEXT AS amendment_id,
        sponsor->>'bioguideId' AS bioguide_id
    FROM jsonb_array_elements(json_data->'sponsors') AS sponsor
    WHERE sponsor ? 'bioguideId' AND sponsor->>'bioguideId' IS NOT NULL;
END;
$$ LANGUAGE plpgsql;

-- Function to extract amended bill data
CREATE OR REPLACE FUNCTION extract_amended_bills(json_data JSONB)
RETURNS TABLE (
    amendment_id TEXT,
    bill_id TEXT
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        -- amendment_id: {type}{number}-{congress}
        lower(json_data->>'type') || (json_data->>'number')::TEXT || '-' || (json_data->>'congress')::TEXT AS amendment_id,
        -- bill_id: {type}{number}-{congress}
        lower(json_data->'amendedBill'->>'type') || (json_data->'amendedBill'->>'number')::TEXT || '-' || (json_data->'amendedBill'->>'congress')::TEXT AS bill_id
    WHERE json_data ? 'amendedBill' 
      AND json_data->'amendedBill' ? 'congress' 
      AND json_data->'amendedBill' ? 'type' 
      AND json_data->'amendedBill' ? 'number';
END;
$$ LANGUAGE plpgsql;

-- Function to extract amended amendment data
CREATE OR REPLACE FUNCTION extract_amended_amendments(json_data JSONB)
RETURNS TABLE (
    amendment_id TEXT,
    amended_amendment_id TEXT
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        -- amendment_id: {type}{number}-{congress}
        lower(json_data->>'type') || (json_data->>'number')::TEXT || '-' || (json_data->>'congress')::TEXT AS amendment_id,
        -- amended_amendment_id: {type}{number}-{congress}
        lower(json_data->'amendedAmendment'->>'type') || (json_data->'amendedAmendment'->>'number')::TEXT || '-' || (json_data->'amendedAmendment'->>'congress')::TEXT AS amended_amendment_id
    WHERE json_data ? 'amendedAmendment'
      AND json_data->'amendedAmendment' ? 'congress' 
      AND json_data->'amendedAmendment' ? 'type' 
      AND json_data->'amendedAmendment' ? 'number';
END;
$$ LANGUAGE plpgsql;

-- Function to extract amended treaty data
CREATE OR REPLACE FUNCTION extract_amended_treaties(json_data JSONB)
RETURNS TABLE (
    amendment_id TEXT,
    treaty_id TEXT
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        -- amendment_id: {type}{number}-{congress}
        lower(json_data->>'type') || (json_data->>'number')::TEXT || '-' || (json_data->>'congress')::TEXT AS amendment_id,
        -- treaty_id: td{congressreceived}-{number}
        'td' || (json_data->'amendedTreaty'->>'congressReceived') || '-' || (json_data->'amendedTreaty'->>'number') AS treaty_id
    WHERE json_data ? 'amendedTreaty' AND json_data->'amendedTreaty' ? 'treatyId';
END;
$$ LANGUAGE plpgsql;

-- Function to extract amendment action data
CREATE OR REPLACE FUNCTION extract_amendment_action_data(json_data JSONB)
RETURNS TABLE (
    amendment_id TEXT,
    action_id TEXT,
    action_text TEXT,
    action_type TEXT,
    action_code TEXT,
    action_date DATE,
    action_time TIME,
    source_system_code INTEGER,
    source_system_name TEXT
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        -- amendment_id from the payload
        json_data->>'amendments_id' AS amendment_id,
        -- Generate deterministic action_id based on action date, text, code, and current timestamp
        encode(
            digest(
                COALESCE(json_data->>'actionDate', '') || '|' || 
                COALESCE(json_data->>'text', '') || '|' || 
                COALESCE(json_data->>'actionCode', '') || '|' ||
                EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)::TEXT,
                'sha256'
            ),
            'hex'
        ) AS action_id,
        json_data->>'text' AS action_text,
        json_data->>'type' AS action_type,
        json_data->>'actionCode' AS action_code,
        CASE 
            WHEN json_data->>'actionDate' IS NOT NULL 
            THEN (json_data->>'actionDate')::DATE
            ELSE NULL
        END AS action_date,
        CASE 
            WHEN json_data->>'actionTime' IS NOT NULL 
            THEN (json_data->>'actionTime')::TIME
            ELSE NULL
        END AS action_time,
        CASE 
            WHEN json_data ? 'sourceSystem' AND json_data->'sourceSystem' ? 'code'
            THEN (json_data->'sourceSystem'->>'code')::INTEGER
            ELSE NULL
        END AS source_system_code,
        CASE 
            WHEN json_data ? 'sourceSystem' AND json_data->'sourceSystem' ? 'name'
            THEN json_data->'sourceSystem'->>'name'
            ELSE NULL
        END AS source_system_name;
END;
$$ LANGUAGE plpgsql;

-- Function to extract amendment action recorded votes (if any)
CREATE OR REPLACE FUNCTION extract_amendment_action_recorded_votes(json_data JSONB)
RETURNS TABLE (
    action_id TEXT,
    amendment_id TEXT,
    chamber TEXT,
    congress INTEGER,
    date TIMESTAMP WITH TIME ZONE,
    roll_number INTEGER,
    session INTEGER,
    url TEXT
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        -- Generate the same deterministic action_id as in extract_amendment_action_data
        encode(
            digest(
                COALESCE(json_data->>'actionDate', '') || '|' || 
                COALESCE(json_data->>'text', '') || '|' || 
                COALESCE(json_data->>'actionCode', '') || '|' ||
                EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)::TEXT,
                'sha256'
            ),
            'hex'
        ) AS action_id,
        json_data->>'amendments_id' AS amendment_id,
        recorded_vote->>'chamber' AS chamber,
        CASE 
            WHEN recorded_vote->>'congress' IS NOT NULL 
            THEN (recorded_vote->>'congress')::INTEGER
            ELSE NULL
        END AS congress,
        CASE 
            WHEN recorded_vote->>'date' IS NOT NULL 
            THEN (recorded_vote->>'date')::TIMESTAMP WITH TIME ZONE
            ELSE NULL
        END AS date,
        CASE 
            WHEN recorded_vote->>'rollNumber' IS NOT NULL 
            THEN (recorded_vote->>'rollNumber')::INTEGER
            ELSE NULL
        END AS roll_number,
        CASE 
            WHEN recorded_vote->>'sessionNumber' IS NOT NULL 
            THEN (recorded_vote->>'sessionNumber')::INTEGER
            ELSE NULL
        END AS session,
        recorded_vote->>'url' AS url
    FROM jsonb_array_elements(json_data->'recordedVotes') AS recorded_vote
    WHERE json_data ? 'recordedVotes' AND json_data->'recordedVotes' IS NOT NULL;
END;
$$ LANGUAGE plpgsql;

-- Main conversion procedure
CREATE OR REPLACE PROCEDURE convert_amendment_json_to_sql(json_payloads JSONB[])
LANGUAGE plpgsql
AS $$
DECLARE
    json_payload JSONB;
    amendment_record RECORD;
    sponsor_record RECORD;
    bill_record RECORD;
    amendment_amendment_record RECORD;
    treaty_record RECORD;
BEGIN
    -- Process each JSON payload
    FOREACH json_payload IN ARRAY json_payloads
    LOOP
        -- Extract main amendment data
        SELECT * INTO amendment_record 
        FROM extract_amendment_data(json_payload);

        -- Insert into main amendments table
        INSERT INTO staging_congressional.amendments (
            amendment_id,
            amendment_type,
            amendment_number,
            congress,
            chamber,
            purpose,
            description,
            proposed_at,
            submitted_at,
            is_bill_amendment,
            is_treaty_amendment,
            is_amendment_amendment,
            notes,
            actions_count,
            cosponsors_count,
            amendments_to_amendment_count,
            updated_at
        ) VALUES (
            amendment_record.amendment_id,
            amendment_record.amendment_type,
            amendment_record.amendment_number,
            amendment_record.congress,
            amendment_record.chamber,
            amendment_record.purpose,
            amendment_record.description,
            amendment_record.proposed_at,
            amendment_record.submitted_at,
            amendment_record.is_bill_amendment,
            amendment_record.is_treaty_amendment,
            amendment_record.is_amendment_amendment,
            amendment_record.notes,
            amendment_record.actions_count,
            amendment_record.cosponsors_count,
            amendment_record.amendments_to_amendment_count,
            amendment_record.updated_at
        )
        ON CONFLICT (amendment_id) DO UPDATE SET
            amendment_type = EXCLUDED.amendment_type,
            amendment_number = EXCLUDED.amendment_number,
            congress = EXCLUDED.congress,
            chamber = EXCLUDED.chamber,
            purpose = EXCLUDED.purpose,
            description = EXCLUDED.description,
            proposed_at = EXCLUDED.proposed_at,
            submitted_at = EXCLUDED.submitted_at,
            is_bill_amendment = EXCLUDED.is_bill_amendment,
            is_treaty_amendment = EXCLUDED.is_treaty_amendment,
            is_amendment_amendment = EXCLUDED.is_amendment_amendment,
            notes = EXCLUDED.notes,
            actions_count = EXCLUDED.actions_count,
            cosponsors_count = EXCLUDED.cosponsors_count,
            amendments_to_amendment_count = EXCLUDED.amendments_to_amendment_count,
            updated_at = EXCLUDED.updated_at;
        
        -- Insert sponsors
        FOR sponsor_record IN 
            SELECT * FROM extract_amendment_sponsors(json_payload)
        LOOP
            INSERT INTO staging_congressional.amendments_sponsors (
                amendment_id,
                bioguide_id
            ) VALUES (
                sponsor_record.amendment_id,
                sponsor_record.bioguide_id
            )
            ON CONFLICT (amendment_id, bioguide_id) DO NOTHING;
        END LOOP;
        
        -- Insert amended bills
        FOR bill_record IN 
            SELECT * FROM extract_amended_bills(json_payload)
        LOOP
            INSERT INTO staging_congressional.amendments_amended_bills (
                amendment_id,
                bill_id
            ) VALUES (
                bill_record.amendment_id,
                bill_record.bill_id
            )
            ON CONFLICT (amendment_id, bill_id) DO NOTHING;
        END LOOP;
        
        -- Insert amended amendments
        FOR amendment_amendment_record IN 
            SELECT * FROM extract_amended_amendments(json_payload)
        LOOP
            INSERT INTO staging_congressional.amendments_amended_amendments (
                amendment_id,
                amended_amendment_id
            ) VALUES (
                amendment_amendment_record.amendment_id,
                amendment_amendment_record.amended_amendment_id
            )
            ON CONFLICT (amendment_id, amended_amendment_id) DO NOTHING;
        END LOOP;
        
        -- Insert amended treaties
        FOR treaty_record IN 
            SELECT * FROM extract_amended_treaties(json_payload)
        LOOP
            INSERT INTO staging_congressional.amendments_amended_treaties (
                amendment_id,
                treaty_id
            ) VALUES (
                treaty_record.amendment_id,
                treaty_record.treaty_id
            )
            ON CONFLICT (amendment_id, treaty_id) DO NOTHING;
        END LOOP;
        
    END LOOP;
END;
$$;

-- Example usage with the provided JSON data
-- You would call this with your actual JSON data:

/*
-- Example call with the provided JSON payloads:
CALL convert_amendment_json_to_sql(ARRAY[
    '{"type": "SAMDT", "number": "584", "actions": {"url": "https://api.congress.gov/v3/amendment/112/samdt/584/actions?format=json", "count": 3}, "chamber": "Senate", "purpose": "Of a perfecting nature.", "congress": 112, "sponsors": [{"url": "https://api.congress.gov/v3/member/R000146?format=json", "party": "D", "state": "NV", "fullName": "Sen. Reid, Harry [D-NV]", "lastName": "REID", "firstName": "HARRY", "bioguideId": "R000146", "middleName": "M."}], "updateDate": "2025-07-02T21:32:29Z", "amendedBill": {"url": "https://api.congress.gov/v3/bill/112/s/1323?format=json", "type": "S", "title": "A bill to express the sense of the Senate on shared sacrifice in resolving the budget deficit.", "number": "1323", "congress": 112, "originChamber": "Senate", "originChamberCode": "S", "updateDateIncludingText": "2025-07-02"}, "latestAction": {"text": "Amendment SA 584 proposed by Senator Reid to Amendment SA 583, the instructions of the motion to commit. (consideration: CR S4866; text: CR S4866) Of a perfecting nature.", "links": [{"url": "https://www.congress.gov/congressional-record/volume-157/senate-section/page/S4866", "name": "S4866"}, {"url": "https://www.congress.gov/amendment/112th-congress/senate-amendment/583", "name": "SA 583"}, {"url": "https://www.congress.gov/amendment/112th-congress/senate-amendment/584", "name": "SA 584"}], "actionDate": "2011-07-25"}, "proposedDate": "2011-07-25T04:00:00Z", "textVersions": {"url": "https://api.congress.gov/v3/amendment/112/samdt/584/text?format=json", "count": 2}, "submittedDate": "2011-07-25T04:00:00Z", "amendedAmendment": {"url": "https://api.congress.gov/v3/amendment/112/samdt/583?format=json", "type": "SAMDT", "number": "583", "purpose": "To change the enactment date.", "congress": 112, "updateDate": "2025-07-02T21:32:29Z"}, "amendmentsToAmendment": {"url": "https://api.congress.gov/v3/amendment/112/samdt/584/amendments?format=json", "count": 1}}'::JSONB,
    '{"type": "SAMDT", "number": "96", "actions": {"url": "https://api.congress.gov/v3/amendment/116/samdt/96/actions?format=json", "count": 6}, "chamber": "Senate", "purpose": "To clarify that the amendment shall not be construed as a declaration of war or an authorization of the use of military force.", "congress": 116, "sponsors": [{"url": "https://api.congress.gov/v3/member/M000639?format=json", "party": "D", "state": "NJ", "fullName": "Sen. Menendez, Robert [D-NJ]", "lastName": "Menendez", "firstName": "Robert", "bioguideId": "M000639"}], "cosponsors": {"url": "https://api.congress.gov/v3/amendment/116/samdt/96/cosponsors?format=json", "count": 1, "countIncludingWithdrawnCosponsors": 1}, "updateDate": "2022-02-08T23:22:08Z", "amendedBill": {"url": "https://api.congress.gov/v3/bill/116/s/1?format=json", "type": "S", "title": "Strengthening America's Security in the Middle East Act of 2019", "number": "1", "congress": 116, "originChamber": "Senate", "originChamberCode": "S", "updateDateIncludingText": "2025-05-28"}, "latestAction": {"text": "Amendment SA 96 agreed to in Senate by Voice Vote. ", "links": [{"url": "https://www.congress.gov/amendment/116th-congress/senate-amendment/96", "name": "SA 96"}], "actionDate": "2019-02-04"}, "proposedDate": "2019-01-31T05:00:00Z", "textVersions": {"url": "https://api.congress.gov/v3/amendment/116/samdt/96/text?format=json", "count": 2}, "submittedDate": "2019-01-31T05:00:00Z", "amendedAmendment": {"url": "https://api.congress.gov/v3/amendment/116/samdt/65?format=json", "type": "SAMDT", "number": "65", "purpose": "To express the sense of the Senate that the United States faces continuing threats from terrorist groups operating in Syria and Afghanistan and that the precipitous withdrawal of United States forces from either country could put at risk hard-won gains and United States national security.", "congress": 116, "updateDate": "2022-02-08T23:22:07Z"}}'::JSONB,
    '{"type": "SAMDT", "number": "1445", "actions": {"url": "https://api.congress.gov/v3/amendment/117/samdt/1445/actions?format=json", "count": 9}, "chamber": "Senate", "purpose": "To improve the bill.", "congress": 117, "sponsors": [{"url": "https://api.congress.gov/v3/member/H001042?format=json", "party": "D", "state": "HI", "fullName": "Sen. Hirono, Mazie K. [D-HI]", "lastName": "Hirono", "firstName": "Mazie", "bioguideId": "H001042", "middleName": "K."}], "cosponsors": {"url": "https://api.congress.gov/v3/amendment/117/samdt/1445/cosponsors?format=json", "count": 3, "countIncludingWithdrawnCosponsors": 3}, "updateDate": "2022-02-09T12:39:28Z", "amendedBill": {"url": "https://api.congress.gov/v3/bill/117/s/937?format=json", "type": "S", "title": "COVID-19 Hate Crimes Act", "number": "937", "congress": 117, "originChamber": "Senate", "originChamberCode": "S", "updateDateIncludingText": "2025-05-28"}, "latestAction": {"text": "Amendment SA 1445 agreed to in Senate by Unanimous Consent. ", "links": [{"url": "https://www.congress.gov/amendment/117th-congress/senate-amendment/1445", "name": "SA 1445"}], "actionDate": "2021-04-22"}, "proposedDate": "2021-04-19T04:00:00Z", "textVersions": {"url": "https://api.congress.gov/v3/amendment/117/samdt/1445/text?format=json", "count": 1}, "submittedDate": "2021-04-19T04:00:00Z", "amendmentsToAmendment": {"url": "https://api.congress.gov/v3/amendment/117/samdt/1445/amendments?format=json", "count": 7}}'::JSONB
]);
*/

-- Utility function to convert a single JSON string to JSONB
CREATE OR REPLACE FUNCTION json_string_to_jsonb(json_string TEXT)
RETURNS JSONB AS $$
BEGIN
    RETURN json_string::JSONB;
EXCEPTION
    WHEN OTHERS THEN
        RAISE EXCEPTION 'Invalid JSON string: %', json_string;
END;
$$ LANGUAGE plpgsql;

-- Function to process amendments from a JSON file or table
CREATE OR REPLACE FUNCTION process_amendment_json_batch(json_payloads TEXT[])
RETURNS VOID AS $$
DECLARE
    json_payload TEXT;
    jsonb_array JSONB[] := ARRAY[]::JSONB[];
BEGIN
    -- Convert TEXT array to JSONB array
    FOREACH json_payload IN ARRAY json_payloads
    LOOP
        jsonb_array := array_append(jsonb_array, json_string_to_jsonb(json_payload));
    END LOOP;
    
    -- Process the batch
    CALL convert_amendment_json_to_sql(jsonb_array);
END;
$$ LANGUAGE plpgsql;

-- Cleanup function (optional - for development/testing)
CREATE OR REPLACE FUNCTION cleanup_amendment_test_data()
RETURNS VOID AS $$
BEGIN
    DELETE FROM staging_congressional.amendments_amended_treaties;
    DELETE FROM staging_congressional.amendments_amended_amendments;
    DELETE FROM staging_congressional.amendments_amended_bills;
    DELETE FROM staging_congressional.amendments_sponsors;
    DELETE FROM staging_congressional.amendments;
END;
$$ LANGUAGE plpgsql;

-- Main function to process amendments from the raw table
CREATE OR REPLACE FUNCTION process_amendments_from_raw_table(
    batch_size INTEGER DEFAULT 1000,
    start_offset INTEGER DEFAULT 0,
    max_records INTEGER DEFAULT NULL
)
RETURNS TABLE(
    processed_count BIGINT,
    error_count BIGINT,
    total_records BIGINT
) AS $$
DECLARE
    json_payload JSONB;
    processed_count BIGINT := 0;
    error_count BIGINT := 0;
    total_records BIGINT := 0;
    record_count BIGINT := 0;
    batch_array JSONB[] := ARRAY[]::JSONB[];
    raw_record RECORD;
    query_text TEXT;
BEGIN
    -- Get total count of records with updateDate after 2024-11-15T12:08:16Z
    SELECT COUNT(*) INTO total_records 
    FROM bicam_raw_congressional.amendments_raw
    WHERE (payload->>'updateDate')::TIMESTAMP WITH TIME ZONE > '2024-11-15T12:08:16Z'::TIMESTAMP WITH TIME ZONE;
    
    -- Build dynamic query with optional limits and filtering for amendments with recent updates
    query_text := 'SELECT payload FROM bicam_raw_congressional.amendments_raw WHERE (payload->>''updateDate'')::TIMESTAMP WITH TIME ZONE > ''2024-11-15T12:08:16Z''::TIMESTAMP WITH TIME ZONE';
    
    IF max_records IS NOT NULL THEN
        query_text := query_text || ' LIMIT ' || max_records;
    END IF;
    
    IF start_offset > 0 THEN
        query_text := query_text || ' OFFSET ' || start_offset;
    END IF;
    
    -- Process records in batches
    FOR raw_record IN EXECUTE query_text
    LOOP
        BEGIN
            -- Convert payload to JSONB
            json_payload := raw_record.payload::JSONB;
            
            -- Add to batch
            batch_array := array_append(batch_array, json_payload);
            record_count := record_count + 1;
            
            -- Process batch when it reaches the batch size
            IF array_length(batch_array, 1) >= batch_size THEN
                CALL convert_amendment_json_to_sql(batch_array);
                processed_count := processed_count + array_length(batch_array, 1);
                batch_array := ARRAY[]::JSONB[];

                RAISE NOTICE 'Processed batch: % records, Total processed: %', 
                    array_length(batch_array, 1), processed_count;
            END IF;
            
        EXCEPTION WHEN OTHERS THEN
            error_count := error_count + 1;
            RAISE WARNING 'Error processing record %: %', record_count, SQLERRM;
            -- Continue processing other records
        END;
    END LOOP;
    
    -- Process remaining records in the last batch
    IF array_length(batch_array, 1) > 0 THEN
        BEGIN
            CALL convert_amendment_json_to_sql(batch_array);
            processed_count := processed_count + array_length(batch_array, 1);
        EXCEPTION WHEN OTHERS THEN
            error_count := error_count + array_length(batch_array, 1);
            RAISE WARNING 'Error processing final batch: %', SQLERRM;
        END;
    END IF;
    
    RETURN QUERY SELECT processed_count, error_count, total_records;
END;
$$ LANGUAGE plpgsql;

-- Function to process all amendments (convenience function)
CREATE OR REPLACE FUNCTION process_all_amendments()
RETURNS TABLE(
    processed_count BIGINT,
    error_count BIGINT,
    total_records BIGINT
) AS $$
BEGIN
    RETURN QUERY SELECT * FROM process_amendments_from_raw_table(1000, 0, NULL);
END;
$$ LANGUAGE plpgsql;

-- Function to get statistics about amendments with recent updates
DROP FUNCTION IF EXISTS get_unprocessed_amendment_stats();
CREATE OR REPLACE FUNCTION get_unprocessed_amendment_stats()
RETURNS TABLE(
    total_raw_amendments BIGINT,
    already_processed BIGINT,
    unprocessed_amendments BIGINT,
    processing_status TEXT
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        (SELECT COUNT(*) FROM bicam_raw_congressional.amendments_raw) as total_raw_amendments,
        (SELECT COUNT(*) FROM bicam.amendments) as already_processed,
        (SELECT COUNT(*) FROM bicam_raw_congressional.amendments_raw 
         WHERE (payload->>'updateDate')::TIMESTAMP WITH TIME ZONE > '2024-11-15T12:08:16Z'::TIMESTAMP WITH TIME ZONE) as unprocessed_amendments,
        CASE 
            WHEN (SELECT COUNT(*) FROM bicam_raw_congressional.amendments_raw 
                  WHERE (payload->>'updateDate')::TIMESTAMP WITH TIME ZONE > '2024-11-15T12:08:16Z'::TIMESTAMP WITH TIME ZONE) = 0
            THEN 'No amendments with recent updates found'
            ELSE 'Amendments with recent updates available for processing'
        END as processing_status;
END;
$$ LANGUAGE plpgsql;

-- Procedure to process amendment actions from raw table
CREATE OR REPLACE PROCEDURE process_amendment_actions_from_raw_table(
    batch_size INTEGER DEFAULT 1000,
    start_offset INTEGER DEFAULT 0,
    max_records INTEGER DEFAULT NULL
)
LANGUAGE plpgsql
AS $$
DECLARE
    action_record RECORD;
    processed_count INTEGER := 0;
    error_count INTEGER := 0;
    record_count INTEGER := 0;
    query_text TEXT;
BEGIN
    -- Build dynamic query with optional limits and filtering for actions related to amendments with recent updates
    query_text := 'SELECT a.payload FROM bicam_raw_congressional.amendments_actions_raw a INNER JOIN bicam_raw_congressional.amendments_raw am ON a.source_doc_id = am.source_doc_id WHERE (am.payload->>''updateDate'')::TIMESTAMP WITH TIME ZONE > ''2024-11-15T12:08:16Z''::TIMESTAMP WITH TIME ZONE';
    
    IF max_records IS NOT NULL THEN
        query_text := query_text || ' LIMIT ' || max_records;
    END IF;
    
    IF start_offset > 0 THEN
        query_text := query_text || ' OFFSET ' || start_offset;
    END IF;
    
    -- Process records
    FOR action_record IN EXECUTE query_text
    LOOP
        BEGIN
            record_count := record_count + 1;

            RAISE NOTICE 'Inserting action: %', action_record.payload;

            -- Insert main action record
            INSERT INTO staging_congressional.amendments_actions (
                action_id,
                amendment_id,
                action_code,
                action_date,
                text,
                action_type,
                source_system,
                source_system_code
            )
            SELECT 
                action_id,
                amendment_id,
                action_code,
                action_date,
                action_text,
                action_type,
                source_system_name,
                source_system_code
            FROM extract_amendment_action_data(action_record.payload::JSONB);

            -- Insert recorded votes if any
            INSERT INTO staging_congressional.amendments_actions_recorded_votes (
                action_id,
                amendment_id,
                chamber,
                congress,
                date,
                roll_number,
                session,
                url
            )
            SELECT 
                action_id,
                amendment_id,
                chamber,
                congress,
                date,
                roll_number,
                session,
                url
            FROM extract_amendment_action_recorded_votes(action_record.payload::JSONB)
            ON CONFLICT (action_id, amendment_id, url) DO NOTHING;
            
            processed_count := processed_count + 1;
            
            -- Log progress every batch_size records
            IF processed_count % batch_size = 0 THEN
                RAISE NOTICE 'Processed % action records', processed_count;
            END IF;
            
        EXCEPTION WHEN OTHERS THEN
            error_count := error_count + 1;
            RAISE WARNING 'Error processing action record %: %', record_count, SQLERRM;
            -- Continue processing other records
        END;
    END LOOP;
    
    RAISE NOTICE 'Processing complete. Processed: %, Errors: %, Total: %', 
        processed_count, error_count, record_count;
END;
$$;

-- Function to get statistics about amendment actions
DROP FUNCTION IF EXISTS get_amendment_actions_stats();
CREATE OR REPLACE FUNCTION get_amendment_actions_stats()
RETURNS TABLE(
    total_raw_actions BIGINT,
    amendment_related_actions BIGINT,
    processed_actions BIGINT,
    unprocessed_amendment_actions BIGINT,
    processing_status TEXT
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        (SELECT COUNT(*) FROM bicam_raw_congressional.amendments_actions_raw) as total_raw_actions,
        (SELECT COUNT(*) FROM bicam_raw_congressional.amendments_actions_raw a 
         INNER JOIN bicam_raw_congressional.amendments_raw am ON a.source_doc_id = am.source_doc_id 
         WHERE (am.payload->>'updateDate')::TIMESTAMP WITH TIME ZONE > '2024-11-15T12:08:16Z'::TIMESTAMP WITH TIME ZONE) as amendment_related_actions,
        (SELECT COUNT(*) FROM staging_congressional.amendments_actions) as processed_actions,
        (SELECT COUNT(*) FROM bicam_raw_congressional.amendments_actions_raw a 
         INNER JOIN bicam_raw_congressional.amendments_raw am ON a.source_doc_id = am.source_doc_id 
         WHERE (am.payload->>'updateDate')::TIMESTAMP WITH TIME ZONE > '2024-11-15T12:08:16Z'::TIMESTAMP WITH TIME ZONE
         AND a.source_doc_id NOT IN (SELECT amendment_id FROM staging_congressional.amendments_actions)) as unprocessed_amendment_actions,
        CASE 
            WHEN (SELECT COUNT(*) FROM bicam_raw_congressional.amendments_actions_raw a 
                  INNER JOIN bicam_raw_congressional.amendments_raw am ON a.source_doc_id = am.source_doc_id 
                  WHERE (am.payload->>'updateDate')::TIMESTAMP WITH TIME ZONE > '2024-11-15T12:08:16Z'::TIMESTAMP WITH TIME ZONE) = 0
            THEN 'No amendment actions for recently updated amendments'
            ELSE 'Amendment actions for recently updated amendments available for processing'
        END as processing_status;
END;
$$ LANGUAGE plpgsql; 