"""etl_load_log: shared ETL provenance and rollback substrate

Revision ID: f1a2b3c4d5e6
Revises: b4e7c2a1f8d3
Create Date: 2026-05-17

Shared infrastructure across all state pipelines (TN, GA, CA, ...) and any
other ETL that writes to a fact table. Every fact-table row FKs to a load_id,
enabling traceability ("which file produced this row?") and clean rollback
("DELETE WHERE source_load_id = X"). parent_load_id chains supersedes-
relationships when reloading a corrected file.

Postgres-only (project standardized on Postgres 2026-04-08).
"""
from typing import Sequence, Union

from alembic import op


revision: str = "f1a2b3c4d5e6"
down_revision: Union[str, Sequence[str], None] = "b4e7c2a1f8d3"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE IF NOT EXISTS etl_load_log (
            load_id           bigserial    PRIMARY KEY,
            load_ts           timestamptz  NOT NULL DEFAULT now(),
            source_file       text         NOT NULL,
            source_file_hash  text,
            table_name        text         NOT NULL,
            row_count         integer      NOT NULL CHECK (row_count >= 0),
            status            text         NOT NULL CHECK (status IN ('success', 'failed', 'partial')),
            notes             text,
            parent_load_id    bigint       REFERENCES etl_load_log(load_id)
        )
    """)
    op.execute("CREATE INDEX IF NOT EXISTS idx_etl_load_log_table_ts    ON etl_load_log(table_name, load_ts DESC)")
    op.execute("CREATE INDEX IF NOT EXISTS idx_etl_load_log_source_file ON etl_load_log(source_file)")

    op.execute("""
        COMMENT ON TABLE etl_load_log IS
          'Shared infrastructure: records every load of a source file into any fact table across all pipelines. Every fact-table row FKs to a load_id, enabling traceability and clean rollback. parent_load_id chains supersedes-relationships when reloading a corrected file.'
    """)
    op.execute("""
        COMMENT ON COLUMN etl_load_log.source_file_hash IS
          'sha256 of the source file bytes. Detects "did the publisher silently republish this file?" or "is this the same file we already loaded?"'
    """)
    op.execute("""
        COMMENT ON COLUMN etl_load_log.parent_load_id IS
          'When this load supersedes an earlier load (e.g. a corrected file), points to the load it replaces. NULL for first-time loads.'
    """)


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS etl_load_log")
