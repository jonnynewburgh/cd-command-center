"""etl_load_log_fixup: rename table_name → source_file_kind, add row_counts_by_table, extend status enum

Revision ID: f4b5c6d7e8f9
Revises: f3a4b5c6d7e8
Create Date: 2026-05-17

Three changes to etl_load_log, bundled because they're all driven by the
multi-table-loader architecture decisions made before writing any loader code:

1. Rename column `table_name` → `source_file_kind`. The previous name implied
   one log row per destination table; the new name reflects one log row per
   source file (which may write to multiple destination tables, as the A-F
   handler does). Values match the parser's FileType enum.

2. Add column `row_counts_by_table jsonb`. For multi-table loads, the total
   row_count is ambiguous (e.g. 1905 letter_grade + 87432 metric = 89337,
   which is meaningless). This column holds per-table counts as JSON, e.g.
   {"tn_letter_grade": 1905, "tn_letter_grade_metric": 87432}. row_count
   remains the total; analytics queries that need per-table counts extract
   from jsonb via ->> or ->.

3. Extend status CHECK enum from ('success','failed','partial') to also
   allow 'skipped'. Skip-handler invocations (e.g. district TVAAS files
   for which no destination table exists) write a status='skipped' log row
   so "did we touch every file in raw/" audits work.

The index idx_etl_load_log_table_ts is renamed to idx_etl_load_log_kind_ts
to match the new column name (the index continues to function after a
column rename, but renaming keeps the index name honest).

Postgres-only.
"""
from typing import Sequence, Union

from alembic import op


revision: str = "f4b5c6d7e8f9"
down_revision: Union[str, Sequence[str], None] = "f3a4b5c6d7e8"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # 1. Rename column. Postgres automatically updates dependent objects
    # (indexes, constraints, comments) to reference the new name.
    op.execute("ALTER TABLE etl_load_log RENAME COLUMN table_name TO source_file_kind")

    # 2. Rename the dependent index to match.
    op.execute("ALTER INDEX idx_etl_load_log_table_ts RENAME TO idx_etl_load_log_kind_ts")

    # 3. Add row_counts_by_table jsonb column.
    op.execute("ALTER TABLE etl_load_log ADD COLUMN row_counts_by_table jsonb")

    # 4. Extend the status CHECK enum to include 'skipped'.
    # Postgres requires DROP + ADD; can't ALTER a CHECK in place.
    op.execute("ALTER TABLE etl_load_log DROP CONSTRAINT etl_load_log_status_check")
    op.execute("""
        ALTER TABLE etl_load_log
        ADD CONSTRAINT etl_load_log_status_check
        CHECK (status IN ('success', 'failed', 'partial', 'skipped'))
    """)

    # 5. Update comments to reflect new shape.
    op.execute("""
        COMMENT ON COLUMN etl_load_log.source_file_kind IS
          'File type per the parser FileType enum (tvaas_school_composite, tvaas_school_subject, tvaas_district_composite, tvaas_district_subject, letter_grade). Renamed from table_name to support multi-table loads where one source file writes to multiple destination tables.'
    """)
    op.execute("""
        COMMENT ON COLUMN etl_load_log.row_counts_by_table IS
          'Per-destination-table row counts as JSON object. Keys are table names, values are row counts. Example: {"tn_letter_grade": 1905, "tn_letter_grade_metric": 87432}. The total across all tables is in row_count. For single-table loads, this is {table_name: row_count}; the redundancy is acceptable for query uniformity.'
    """)
    op.execute("""
        COMMENT ON COLUMN etl_load_log.status IS
          'Load outcome: success (committed), failed (rolled back, attempt logged separately), partial (deprecated — never written by the current loader, kept for back-compat), skipped (file was recognized by the parser but the handler is a no-op skip, e.g. district TVAAS files for which no destination table exists).'
    """)


def downgrade() -> None:
    # Reverse in reverse order. Note: any rows with status='skipped' will
    # cause the CHECK reinstall to fail; downgrade assumes a clean slate.
    op.execute("ALTER TABLE etl_load_log DROP CONSTRAINT etl_load_log_status_check")
    op.execute("""
        ALTER TABLE etl_load_log
        ADD CONSTRAINT etl_load_log_status_check
        CHECK (status IN ('success', 'failed', 'partial'))
    """)
    op.execute("ALTER TABLE etl_load_log DROP COLUMN row_counts_by_table")
    op.execute("ALTER INDEX idx_etl_load_log_kind_ts RENAME TO idx_etl_load_log_table_ts")
    op.execute("ALTER TABLE etl_load_log RENAME COLUMN source_file_kind TO table_name")
