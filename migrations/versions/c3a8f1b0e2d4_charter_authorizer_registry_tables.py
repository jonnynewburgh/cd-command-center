"""charter authorizer registry: statute snapshot + operational tables

Revision ID: c3a8f1b0e2d4
Revises: 9e3b590fa748
Create Date: 2026-05-04

Adds:
  statutory_charter_authorizer_policy — one row per state/DC from the NACSA
    statutory landscape seed CSV (types permitted under law; not individual agencies).

  authorizers — named charter authorizing entities (state DOE / commission / LEA / HEI
    rows loaded over time from open data + manual entry).

  school_authorizer — bridge: NCES school ↔ authorizer for a given school_year,
    with provenance in source_system.

SQLite and PostgreSQL DDL branches match patterns used elsewhere (Alembic runs against
the active DATABASE_URL).
"""
from typing import Sequence, Union

from alembic import op


revision: str = "c3a8f1b0e2d4"
down_revision: Union[str, Sequence[str], None] = "9e3b590fa748"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    bind = op.get_bind()
    is_pg = bind.dialect.name == "postgresql"

    op.execute(
        """
        CREATE TABLE IF NOT EXISTS statutory_charter_authorizer_policy (
            state_usps TEXT NOT NULL,
            state_name TEXT NOT NULL,
            nacsa_col_1 TEXT,
            nacsa_col_2 TEXT,
            nacsa_col_3 TEXT,
            nacsa_col_4 TEXT,
            source_url TEXT,
            retrieved TEXT,
            PRIMARY KEY (state_usps)
        )
        """
    )

    if is_pg:
        op.execute(
            """
            CREATE TABLE IF NOT EXISTS authorizers (
                id SERIAL PRIMARY KEY,
                state TEXT NOT NULL,
                name TEXT NOT NULL,
                authorizer_kind TEXT,
                nces_lea_id TEXT,
                state_authorizer_id TEXT,
                source_system TEXT,
                source_url TEXT,
                notes TEXT,
                is_active INTEGER DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE (state, name)
            )
            """
        )
        op.execute(
            """
            CREATE TABLE IF NOT EXISTS school_authorizer (
                id SERIAL PRIMARY KEY,
                nces_school_id TEXT NOT NULL,
                authorizer_id INTEGER NOT NULL REFERENCES authorizers(id) ON DELETE CASCADE,
                school_year TEXT NOT NULL,
                relationship TEXT,
                source_system TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE (nces_school_id, authorizer_id, school_year)
            )
            """
        )
    else:
        op.execute(
            """
            CREATE TABLE IF NOT EXISTS authorizers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                state TEXT NOT NULL,
                name TEXT NOT NULL,
                authorizer_kind TEXT,
                nces_lea_id TEXT,
                state_authorizer_id TEXT,
                source_system TEXT,
                source_url TEXT,
                notes TEXT,
                is_active INTEGER DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE (state, name)
            )
            """
        )
        op.execute(
            """
            CREATE TABLE IF NOT EXISTS school_authorizer (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                nces_school_id TEXT NOT NULL,
                authorizer_id INTEGER NOT NULL REFERENCES authorizers(id) ON DELETE CASCADE,
                school_year TEXT NOT NULL,
                relationship TEXT,
                source_system TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE (nces_school_id, authorizer_id, school_year)
            )
            """
        )

    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_authorizers_state ON authorizers(state)"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_authorizers_nces_lea ON authorizers(nces_lea_id)"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_authorizers_state_agency_id ON authorizers(state_authorizer_id)"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_school_authorizer_school ON school_authorizer(nces_school_id)"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_school_authorizer_authorizer ON school_authorizer(authorizer_id)"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_school_authorizer_year ON school_authorizer(school_year)"
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS school_authorizer")
    op.execute("DROP TABLE IF EXISTS authorizers")
    op.execute("DROP TABLE IF EXISTS statutory_charter_authorizer_policy")
