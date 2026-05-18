-- =====================================================================
-- TN School Crosswalk Schema — REVIEW DRAFT (standalone)
-- =====================================================================
-- Maps TDOE state-native IDs (system_id, school_id) to NCES school IDs.
-- Standalone pipeline output, independent of accountability tables.
-- After approval, convert to its own Alembic migration. Future unified
-- state_school_crosswalk view will UNION ALL this with ga_, ca_ equivalents.
--
-- Depends on: etl_load_log (defined in etl_load_log_review.sql — shared
-- infrastructure across all state pipelines; that migration must run first).
-- =====================================================================


CREATE TABLE tn_school_crosswalk (
    tdoe_system_id    integer  NOT NULL,
    tdoe_school_id    integer  NOT NULL,
    ncessch           text     NOT NULL,                       -- NCES 12-digit school ID
    year_valid_start  integer  NOT NULL CHECK (year_valid_start BETWEEN 2000 AND 2100),
    year_valid_end    integer           CHECK (year_valid_end IS NULL OR year_valid_end >= year_valid_start),
    source_load_id    bigint   NOT NULL REFERENCES etl_load_log(load_id),
    PRIMARY KEY (tdoe_system_id, tdoe_school_id, year_valid_start)
);

CREATE INDEX idx_tn_crosswalk_ncessch ON tn_school_crosswalk(ncessch);

COMMENT ON TABLE tn_school_crosswalk IS
  'Maps TDOE state-native IDs (system + school) to NCES school IDs. Year-banded because schools occasionally get re-numbered. Built by its own ETL from the official TDOE crosswalk file; never embedded in accountability loaders. Joined softly (no FK) from fact tables — a school can appear in A-F or TVAAS before the crosswalk catches up.';
COMMENT ON COLUMN tn_school_crosswalk.year_valid_start IS
  'First school-year-end-year in which this mapping is valid. Uses end-year convention (SY 2022-23 → 2023). See tn_letter_grade.year COMMENT for full convention.';
COMMENT ON COLUMN tn_school_crosswalk.year_valid_end IS
  'NULL means "currently valid". Set to the last end-year the mapping held when a school is renumbered.';
