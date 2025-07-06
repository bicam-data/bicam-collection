-- Create raw staging schema for Congressional API payloads
-- Each table stores the unaltered JSON payload in a flexible layout
-- -----------------------------------------------------------------

BEGIN;

-- 1. Schema ----------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS bicam_raw_congressional;

-- 2. Baseline table definition --------------------------------------------
-- All raw-tables share the same structure.  We create one helper domain so
-- the DDL is easy to maintain via a single 
-- template that is repeated for every <parent>_<relation>_raw table.

-- Table template (commented for reference)
--   id_uuid        TEXT PRIMARY KEY                -- random surrogate PK
--   fetched_at     TIMESTAMPTZ NOT NULL            -- time of ingestion
--   payload        JSONB NOT NULL                  -- full raw JSON
--   endpoint       TEXT                            -- canonical endpoint
--   source_doc_id  TEXT                            -- natural key when known
--   etl_batch_id   TEXT                            -- batch tag from ETL run

-- Helper macro: executes a CREATE TABLE IF NOT EXISTS with the common cols
DO $$
DECLARE
    table_name TEXT;
    table_names TEXT[] := ARRAY[
        'bills_list_raw',
        'bills_full_raw', 
        'bills_texts_raw',
        'bills_summaries_raw',
        'bills_subjects_raw',
        'bills_actions_raw',
        'bills_amendments_raw',
        'bills_cosponsors_raw',
        'bills_committees_raw',
        'bills_related_raw',
        'bills_titles_raw',
        'amendments_list_raw',
        'amendments_full_raw',
        'amendments_actions_raw',
        'amendments_cosponsors_raw', 
        'committees_list_raw',
        'committees_full_raw',
        'committeemeetings_list_raw',
        'committeemeetings_full_raw',
        'committeeprints_list_raw',
        'committeeprints_full_raw',
        'committeereports_list_raw',
        'committeereports_full_raw',
        'congresses_list_raw',
        'congresses_full_raw',
        'hearings_list_raw',
        'hearings_full_raw',
        'members_list_raw',
        'members_full_raw',
        'nominations_list_raw',
        'nominations_full_raw',
        'treaties_list_raw',
        'treaties_full_raw'
    ];
BEGIN
    FOREACH table_name IN ARRAY table_names LOOP
        EXECUTE format('
            CREATE TABLE IF NOT EXISTS bicam_raw_congressional.%I (
                id_uuid TEXT PRIMARY KEY,
                url TEXT,
                batch_id TEXT,
                scraped_at TIMESTAMPTZ,
                payload JSONB NOT NULL,
                endpoint TEXT,
                source_doc_id TEXT,
                etl_batch_id TEXT
            )', table_name);
    END LOOP;
END
$$;

-- 3. Indexes for performance -------------------------------------------
DO $$
DECLARE
    table_name TEXT;
    table_names TEXT[] := ARRAY[
        'bills_list_raw',
        'bills_full_raw', 
        'bills_texts_raw',
        'bills_summaries_raw',
        'bills_subjects_raw',
        'bills_actions_raw',
        'bills_amendments_raw',
        'bills_cosponsors_raw',
        'bills_committees_raw',
        'bills_related_raw',
        'bills_titles_raw',
        'amendments_list_raw',
        'amendments_full_raw',
        'amendments_actions_raw',
        'amendments_cosponsors_raw', 
        'committees_list_raw',
        'committees_full_raw',
        'committeemeetings_list_raw',
        'committeemeetings_full_raw',
        'committeeprints_list_raw',
        'committeeprints_full_raw',
        'committeereports_list_raw',
        'committeereports_full_raw',
        'congresses_list_raw',
        'congresses_full_raw',
        'hearings_list_raw',
        'hearings_full_raw',
        'members_list_raw',
        'members_full_raw',
        'nominations_list_raw',
        'nominations_full_raw',
        'treaties_list_raw',
        'treaties_full_raw'
    ];
BEGIN
    FOREACH table_name IN ARRAY table_names LOOP
        EXECUTE format('CREATE INDEX IF NOT EXISTS idx_%I_scraped_at ON bicam_raw_congressional.%I(scraped_at)', table_name, table_name);
        EXECUTE format('CREATE INDEX IF NOT EXISTS idx_%I_batch_id ON bicam_raw_congressional.%I(batch_id)', table_name, table_name);
        EXECUTE format('CREATE INDEX IF NOT EXISTS idx_%I_payload_gin ON bicam_raw_congressional.%I USING GIN(payload)', table_name, table_name);
    END LOOP;
END
$$;

COMMIT; 