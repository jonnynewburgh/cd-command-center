"""tn_school_crosswalk: TDOE ↔ NCES school ID mapping

Revision ID: f2a3b4c5d6e7
Revises: f1a2b3c4d5e6
Create Date: 2026-05-17

Standalone pipeline output independent of accountability tables. Maps
TDOE state-native IDs (system_id, school_id) to NCES 12-digit school IDs.
Year-banded because schools occasionally get re-numbered. Joined softly
(no FK) from fact tables — a school can appear in A-F or TVAAS before
the crosswalk catches up.

Postgres-only.
"""
from typing import Sequence, Union

from alembic import op


revision: str = "f2a3b4c5d6e7"
down_revision: Union[str, Sequence[str], None] = "f1a2b3c4d5e6"
branch_labels: Union[str, Sequence[str], None] = None
# Explicit dependency on etl_load_log (FK target). Belt-and-suspenders
# with down_revision so partial downgrades can't drop etl_load_log out
# from under this table while it's still referenced.
depends_on: Union[str, Sequence[str], None] = "f1a2b3c4d5e6"


def upgrade() -> None:
    op.execute("""
        CREATE TABLE IF NOT EXISTS tn_school_crosswalk (
            tdoe_system_id    integer  NOT NULL,
            tdoe_school_id    integer  NOT NULL,
            ncessch           text     NOT NULL,
            year_valid_start  integer  NOT NULL CHECK (year_valid_start BETWEEN 2000 AND 2100),
            year_valid_end    integer           CHECK (year_valid_end IS NULL OR year_valid_end >= year_valid_start),
            source_load_id    bigint   NOT NULL REFERENCES etl_load_log(load_id),
            PRIMARY KEY (tdoe_system_id, tdoe_school_id, year_valid_start)
        )
    """)
    op.execute("CREATE INDEX IF NOT EXISTS idx_tn_crosswalk_ncessch ON tn_school_crosswalk(ncessch)")

    op.execute("""
        COMMENT ON TABLE tn_school_crosswalk IS
          'Maps TDOE state-native IDs (system + school) to NCES school IDs. Year-banded because schools occasionally get re-numbered. Built by its own ETL from the official TDOE crosswalk file; never embedded in accountability loaders. Joined softly (no FK) from fact tables — a school can appear in A-F or TVAAS before the crosswalk catches up.'
    """)
    op.execute("""
        COMMENT ON COLUMN tn_school_crosswalk.year_valid_start IS
          'First school-year-end-year in which this mapping is valid. Uses end-year convention (SY 2022-23 → 2023). See tn_letter_grade.year COMMENT for full convention.'
    """)
    op.execute("""
        COMMENT ON COLUMN tn_school_crosswalk.year_valid_end IS
          'NULL means "currently valid". Set to the last end-year the mapping held when a school is renumbered.'
    """)


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS tn_school_crosswalk")
