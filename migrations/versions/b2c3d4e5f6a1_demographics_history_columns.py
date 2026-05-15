"""add asian/multiracial/ELL/SPED columns to enrollment_history

The table already had pct_black/pct_hispanic/pct_white but was missing the
remaining race buckets and the ELL/SPED rates that fetch_nces_charter_schools.py
already pulls. Storing all of them per (nces_id, school_year) lets us hold a
multi-year demographic history alongside enrollment trends.

Revision ID: b2c3d4e5f6a1
Revises: a1b2c3d4e5f6
Create Date: 2026-05-09
"""
from typing import Sequence, Union

from alembic import op


revision: str = "b2c3d4e5f6a1"
down_revision: Union[str, Sequence[str], None] = "a1b2c3d4e5f6"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("ALTER TABLE enrollment_history ADD COLUMN IF NOT EXISTS pct_asian REAL")
    op.execute("ALTER TABLE enrollment_history ADD COLUMN IF NOT EXISTS pct_multiracial REAL")
    op.execute("ALTER TABLE enrollment_history ADD COLUMN IF NOT EXISTS pct_ell REAL")
    op.execute("ALTER TABLE enrollment_history ADD COLUMN IF NOT EXISTS pct_sped REAL")


def downgrade() -> None:
    op.execute("ALTER TABLE enrollment_history DROP COLUMN IF EXISTS pct_sped")
    op.execute("ALTER TABLE enrollment_history DROP COLUMN IF EXISTS pct_ell")
    op.execute("ALTER TABLE enrollment_history DROP COLUMN IF EXISTS pct_multiracial")
    op.execute("ALTER TABLE enrollment_history DROP COLUMN IF EXISTS pct_asian")
