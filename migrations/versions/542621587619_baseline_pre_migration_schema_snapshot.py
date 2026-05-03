"""baseline: pre-migration schema snapshot

Revision ID: 542621587619
Revises:
Create Date: 2026-05-03

This baseline is intentionally empty. db.init_db() is still the source
of truth for the existing schema; this revision just declares "every
table created by init_db() up to commit a855cf3 / e2676d8 is already
applied" so future schema changes can be added as proper migrations
that build on top.

Migration workflow from here on
-------------------------------
- New schema changes (CREATE TABLE, ALTER TABLE, indexes that aren't
  already in init_db) get a new alembic revision file. Run
  `alembic revision -m "short description"` and fill in upgrade() /
  downgrade() with op.execute() / op.create_table() calls.
- Existing developers / production: `alembic stamp head` once to mark
  the current DB as already at this baseline. After that
  `alembic upgrade head` will apply only the new revisions on top.
- Fresh empty DB: continue running `db.init_db()` for now (it knows
  every table); it remains the bootstrap path until init_db is
  retired in favor of versioned migrations end-to-end. Then
  `alembic stamp head` to align.

CODEX P1 #8 (audit 2026-04-26).
"""
from typing import Sequence, Union

# Imports kept on hand so future revisions copy the standard preamble.
from alembic import op  # noqa: F401
import sqlalchemy as sa  # noqa: F401


revision: str = "542621587619"
down_revision: Union[str, Sequence[str], None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """No-op: the baseline schema is whatever db.init_db() produced."""
    pass


def downgrade() -> None:
    """No-op: there is nothing under the baseline to downgrade to."""
    pass
