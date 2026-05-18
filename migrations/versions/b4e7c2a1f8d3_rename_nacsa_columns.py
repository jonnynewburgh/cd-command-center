"""rename statutory_charter_authorizer_policy nacsa_col_* to semantic names

Revision ID: b4e7c2a1f8d3
Revises: a8c3f5d9e2b1
Create Date: 2026-05-17

Renames nacsa_col_1..4 on statutory_charter_authorizer_policy to match the actual
NACSA source headers, and adds has_charter_law as a STORED generated column.

The table is TRUNCATEd at the top of upgrade because the existing rows were
loaded via pack-left transcription (values landed in nacsa_col_2 that should
have been in nacsa_col_4, etc.). The renamed columns would therefore hold
mis-aligned data the moment the rename completed. The seed file at
data/seed/authorizers/nacsa_statutory_landscape.csv has been re-transcribed with
column positions matching NACSA's table; rerun
etl/load_statutory_charter_authorizer_policy.py after this migration.

Column mapping:
  nacsa_col_1 -> allowed_by_law          (NACSA "AUTHORIZERS ALLOWED BY LAW")
  nacsa_col_2 -> appeal_only             (NACSA "ON APPEAL ONLY")
  nacsa_col_3 -> limited_jurisdiction    (NACSA "LIMITED JURISDICTION")
  nacsa_col_4 -> allowed_not_operating   (NACSA "ALLOWED BUT NOT CURRENTLY IN OPERATION")
"""
from typing import Sequence, Union

from alembic import op


revision: str = "b4e7c2a1f8d3"
down_revision: Union[str, Sequence[str], None] = "a8c3f5d9e2b1"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("TRUNCATE TABLE statutory_charter_authorizer_policy")

    op.execute(
        "ALTER TABLE statutory_charter_authorizer_policy "
        "RENAME COLUMN nacsa_col_1 TO allowed_by_law"
    )
    op.execute(
        "ALTER TABLE statutory_charter_authorizer_policy "
        "RENAME COLUMN nacsa_col_2 TO appeal_only"
    )
    op.execute(
        "ALTER TABLE statutory_charter_authorizer_policy "
        "RENAME COLUMN nacsa_col_3 TO limited_jurisdiction"
    )
    op.execute(
        "ALTER TABLE statutory_charter_authorizer_policy "
        "RENAME COLUMN nacsa_col_4 TO allowed_not_operating"
    )

    op.execute(
        """
        ALTER TABLE statutory_charter_authorizer_policy
        ADD COLUMN has_charter_law BOOLEAN
        GENERATED ALWAYS AS (allowed_by_law IS DISTINCT FROM 'No Charter Law') STORED
        """
    )


def downgrade() -> None:
    op.execute("ALTER TABLE statutory_charter_authorizer_policy DROP COLUMN has_charter_law")

    op.execute(
        "ALTER TABLE statutory_charter_authorizer_policy "
        "RENAME COLUMN allowed_not_operating TO nacsa_col_4"
    )
    op.execute(
        "ALTER TABLE statutory_charter_authorizer_policy "
        "RENAME COLUMN limited_jurisdiction TO nacsa_col_3"
    )
    op.execute(
        "ALTER TABLE statutory_charter_authorizer_policy "
        "RENAME COLUMN appeal_only TO nacsa_col_2"
    )
    op.execute(
        "ALTER TABLE statutory_charter_authorizer_policy "
        "RENAME COLUMN allowed_by_law TO nacsa_col_1"
    )

    op.execute("TRUNCATE TABLE statutory_charter_authorizer_policy")
