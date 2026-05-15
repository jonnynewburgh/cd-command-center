"""index schools.ein and fqhc.ein

Both columns were added via ALTER TABLE in db.init_db() and never indexed.
get_operator_schools / get_operator_fqhc full-scan on every org page load.

Revision ID: a1b2c3d4e5f6
Revises: c7554b2253ea
Create Date: 2026-05-09
"""
from typing import Sequence, Union

from alembic import op


revision: str = "a1b2c3d4e5f6"
down_revision: Union[str, Sequence[str], None] = "c7554b2253ea"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("CREATE INDEX IF NOT EXISTS idx_schools_ein ON schools(ein)")
    op.execute("CREATE INDEX IF NOT EXISTS idx_fqhc_ein ON fqhc(ein)")


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS idx_fqhc_ein")
    op.execute("DROP INDEX IF EXISTS idx_schools_ein")
