"""capture existing init_db schema as baseline

Revision ID: f19ded25b983
Revises: 542621587619
Create Date: 2026-05-03

Pins `db.init_db()`'s output as Alembic revision f19ded25b983.

Workflow on top of this revision
--------------------------------
- Fresh DB: `alembic upgrade head` is now sufficient — this revision
  delegates to `db.init_db()`, which is idempotent (every CREATE
  uses IF NOT EXISTS) and creates every table the app needs.
- Existing DB stamped at 542621587619 (the empty baseline): a plain
  `alembic upgrade head` is also safe — `db.init_db()`'s IF NOT EXISTS
  guards make it a no-op when the tables already exist.
- New schema changes: add a brand-new revision with op.create_table()
  / op.execute(). Do NOT add new statements to init_db — this
  revision is the freeze point for init_db's contents.

Rationale
---------
The audit's P1 #8 followup wanted "init_db retired in favor of
versioned migrations end-to-end". Two paths to get there:

  (a) translate every CREATE in init_db (~30 tables, ~70 indexes,
      ALTER TABLE patches) into op.create_table() / op.create_index()
      calls — multi-session refactor, lots of room for translation
      bugs;
  (b) wrap a `db.init_db()` call in a single migration, freeze
      init_db at this snapshot, write all future schema changes as
      proper migrations.

(b) gets the workflow benefit (single command for fresh DBs, ordered
chain of changes from here on) without spending a session on
mechanical translation. The downside is that this one revision is
opaque (it doesn't list each table) — acceptable because init_db
already exists in version control as the readable form, and 0003+
revisions WILL be readable op.create_table() calls.

The transactional caveat: db.init_db() opens its own DBAPI connection
and commits internally, so this revision is not atomic with whatever
runs after it. That's fine for an idempotent CREATE-IF-NOT-EXISTS
baseline; if a downstream revision fails, re-running upgrade head
just no-ops this one and retries the failing one.
"""
from typing import Sequence, Union

from alembic import op  # noqa: F401
import sqlalchemy as sa  # noqa: F401


revision: str = "f19ded25b983"
down_revision: Union[str, Sequence[str], None] = "542621587619"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Apply db.init_db() to materialize the baseline schema."""
    import sys
    import os
    # Make repo root importable when alembic is invoked from a non-root cwd.
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
    import db
    db.init_db()


def downgrade() -> None:
    """No-op: rolling back to the empty baseline would require dropping
    every table — destructive and never the intended action."""
    pass
