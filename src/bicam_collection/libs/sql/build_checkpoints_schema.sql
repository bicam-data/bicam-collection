BEGIN;

-- Create checkpoint schema
CREATE SCHEMA IF NOT EXISTS bicam_checkpoints;

-- ------------------------------------------------------------------
--  Main table holding high-level checkpoint rows
-- ------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bicam_checkpoints.checkpoints (
    id                  BIGSERIAL PRIMARY KEY,
    scraper_type        TEXT        NOT NULL,
    data_type           TEXT        NOT NULL,

    last_processed_id   TEXT,
    last_processed_date TIMESTAMPTZ,

    total_items         INTEGER     DEFAULT 0,
    processed_items     INTEGER     DEFAULT 0,
    failed_items        INTEGER     DEFAULT 0,

    status              TEXT        DEFAULT 'pending',

    created_at          TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    -- JSONB blobs for hierarchical state / metadata
    processing_state    JSONB       DEFAULT '{}'::jsonb,
    api_params          JSONB       DEFAULT '{}'::jsonb,
    last_exported_item  JSONB       DEFAULT '{}'::jsonb,
    batch_info          JSONB       DEFAULT '{}'::jsonb,
    metadata            JSONB       DEFAULT '{}'::jsonb,

    UNIQUE (scraper_type, data_type)
);

-- ------------------------------------------------------------------
--  Table storing every processed item (idempotent via PK)
-- ------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bicam_checkpoints.processed_items (
    checkpoint_id     BIGINT REFERENCES bicam_checkpoints.checkpoints(id) ON DELETE CASCADE,
    item_id           TEXT        NOT NULL,
    processing_phase  TEXT        NOT NULL,
    field_name        TEXT,
    relation_type     TEXT,
    processed_at      TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (checkpoint_id, item_id, processing_phase, field_name, relation_type)
);

-- ------------------------------------------------------------------
--  Table capturing error details when an item fails
-- ------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bicam_checkpoints.checkpoint_errors (
    id               BIGSERIAL PRIMARY KEY,
    checkpoint_id    BIGINT REFERENCES bicam_checkpoints.checkpoints(id) ON DELETE CASCADE,
    item_id          TEXT,
    error_message    TEXT,
    error_details    TEXT,
    processing_phase TEXT,
    field_name       TEXT,
    relation_type    TEXT,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- ------------------------------------------------------------------
--  Indexes & housekeeping triggers
-- ------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_checkpoints_status
    ON bicam_checkpoints.checkpoints(status);

CREATE INDEX IF NOT EXISTS idx_processed_items_checkpoint
    ON bicam_checkpoints.processed_items(checkpoint_id);

CREATE INDEX IF NOT EXISTS idx_checkpoint_errors_checkpoint
    ON bicam_checkpoints.checkpoint_errors(checkpoint_id);

-- Trigger to keep updated_at current
CREATE OR REPLACE FUNCTION bicam_checkpoints_set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS set_checkpoints_updated_at ON bicam_checkpoints.checkpoints;

CREATE TRIGGER set_checkpoints_updated_at
    BEFORE UPDATE ON bicam_checkpoints.checkpoints
    FOR EACH ROW EXECUTE FUNCTION bicam_checkpoints_set_updated_at();

-- ------------------------------------------------------------------
--  Permissions (adjust role as appropriate)
-- ------------------------------------------------------------------
GRANT USAGE ON SCHEMA bicam_checkpoints TO bicam_pipeline;
GRANT ALL PRIVILEGES ON ALL TABLES    IN SCHEMA bicam_checkpoints TO bicam_pipeline;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA bicam_checkpoints TO bicam_pipeline;

COMMIT;