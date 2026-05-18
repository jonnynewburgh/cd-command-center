-- =====================================================================
-- etl_load_log — Shared ETL Provenance Table (REVIEW DRAFT)
-- =====================================================================
-- Promoted to its own migration because this is shared infrastructure
-- across all state pipelines (TN, GA, CA, ...) and any other ETL that
-- writes to a fact table. Every fact-table row FKs to a load_id, enabling
-- traceability ("which file produced this row?") and clean rollback
-- ("DELETE WHERE source_load_id = X").
--
-- This migration must run before any state-accountability migration
-- (tn_*, ga_*, ca_*) and before tn_school_crosswalk.
-- =====================================================================


CREATE TABLE etl_load_log (
    load_id           bigserial    PRIMARY KEY,
    load_ts           timestamptz  NOT NULL DEFAULT now(),
    source_file       text         NOT NULL,
    source_file_hash  text,
    table_name        text         NOT NULL,
    row_count         integer      NOT NULL CHECK (row_count >= 0),
    status            text         NOT NULL CHECK (status IN ('success', 'failed', 'partial')),
    notes             text,
    parent_load_id    bigint       REFERENCES etl_load_log(load_id)
);

CREATE INDEX idx_etl_load_log_table_ts     ON etl_load_log(table_name, load_ts DESC);
CREATE INDEX idx_etl_load_log_source_file  ON etl_load_log(source_file);

COMMENT ON TABLE etl_load_log IS
  'Shared infrastructure: records every load of a source file into any fact table across all pipelines. Every fact-table row FKs to a load_id, enabling traceability and clean rollback. parent_load_id chains supersedes-relationships when reloading a corrected file.';
COMMENT ON COLUMN etl_load_log.source_file_hash IS
  'sha256 of the source file bytes. Detects "did the publisher silently republish this file?" or "is this the same file we already loaded?"';
COMMENT ON COLUMN etl_load_log.parent_load_id IS
  'When this load supersedes an earlier load (e.g. a corrected file), points to the load it replaces. NULL for first-time loads.';
