-- Create raw staging schema for GovInfo API payloads
-- Each table stores the unaltered JSON payload in a flexible layout
-- -----------------------------------------------------------------

BEGIN;

-- 1. Schema ----------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS bicam_raw_govinfo;

-- 2. Baseline table definition --------------------------------------------
-- All raw-tables share the same structure.  We create one helper domain so
-- the DDL is easy to maintain via a single 
-- template that is repeated for every <parent>_<relation>_raw table.

-- Table template (commented for reference)
--   id_uuid        TEXT PRIMARY KEY                -- random surrogate PK
--   package_link   TEXT                            -- package URL
--   granule_link   TEXT                            -- granule URL
--   batch_id       TEXT                            -- batch identifier
--   scraped_at     TIMESTAMPTZ NOT NULL            -- time of ingestion
--   payload        JSONB NOT NULL                  -- full raw JSON
--   endpoint       TEXT                            -- canonical endpoint
--   source_doc_id  TEXT                            -- natural key when known
--   etl_batch_id   TEXT                            -- batch tag from ETL run

-- Helper macro: executes a CREATE TABLE IF NOT EXISTS with the common cols
DO $$
DECLARE
    table_name TEXT;
    table_names TEXT[] := ARRAY[
        'bills_collection_list_raw',
        'bills_collection_package_raw',
        'bills_collection_granules_raw',
        'bills_collection_granule_raw',
        'congressional_reports_list_raw',
        'congressional_reports_package_raw',
        'congressional_reports_granules_raw',
        'congressional_reports_granule_raw',
        'congressional_directories_list_raw',
        'congressional_directories_package_raw',
        'congressional_directories_granules_raw',
        'congressional_directories_granule_raw',
        'hearing_packages_list_raw',
        'hearing_packages_package_raw',
        'hearing_packages_granules_raw',
        'hearing_packages_granule_raw',
        'print_packages_list_raw',
        'print_packages_package_raw',
        'print_packages_granules_raw',
        'print_packages_granule_raw',
        'treaty_docs_list_raw',
        'treaty_docs_package_raw',
        'treaty_docs_granules_raw',
        'treaty_docs_granule_raw'
    ];
BEGIN
    FOREACH table_name IN ARRAY table_names LOOP
        EXECUTE format('
            CREATE TABLE IF NOT EXISTS bicam_raw_govinfo.%I (
                id_uuid TEXT PRIMARY KEY,
                package_link TEXT,
                granule_link TEXT,
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
        'bills_collection_list_raw',
        'bills_collection_package_raw',
        'bills_collection_granules_raw',
        'bills_collection_granule_raw',
        'congressional_reports_list_raw',
        'congressional_reports_package_raw',
        'congressional_reports_granules_raw',
        'congressional_reports_granule_raw',
        'congressional_directories_list_raw',
        'congressional_directories_package_raw',
        'congressional_directories_granules_raw',
        'congressional_directories_granule_raw',
        'hearing_packages_list_raw',
        'hearing_packages_package_raw',
        'hearing_packages_granules_raw',
        'hearing_packages_granule_raw',
        'print_packages_list_raw',
        'print_packages_package_raw',
        'print_packages_granules_raw',
        'print_packages_granule_raw',
        'treaty_docs_list_raw',
        'treaty_docs_package_raw',
        'treaty_docs_granules_raw',
        'treaty_docs_granule_raw'
    ];
BEGIN
    FOREACH table_name IN ARRAY table_names LOOP
        EXECUTE format('CREATE INDEX IF NOT EXISTS idx_%I_scraped_at ON bicam_raw_govinfo.%I(scraped_at)', table_name, table_name);
        EXECUTE format('CREATE INDEX IF NOT EXISTS idx_%I_batch_id ON bicam_raw_govinfo.%I(batch_id)', table_name, table_name);
        EXECUTE format('CREATE INDEX IF NOT EXISTS idx_%I_package_link ON bicam_raw_govinfo.%I(package_link)', table_name, table_name);
        EXECUTE format('CREATE INDEX IF NOT EXISTS idx_%I_payload_gin ON bicam_raw_govinfo.%I USING GIN(payload)', table_name, table_name);
    END LOOP;
END
$$;

COMMIT; 