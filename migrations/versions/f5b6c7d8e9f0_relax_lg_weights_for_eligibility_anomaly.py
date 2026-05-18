"""relax lg_weights_sum CHECK to admit the eligibility-anomaly state

Revision ID: f5b6c7d8e9f0
Revises: f4b5c6d7e8f9
Create Date: 2026-05-18

Discovered during A-F handler implementation: the original lg_weights_sum
CHECK is too strict for actual TDOE data. The reality observed across all
three A-F files (2022-23, 2023-24, 2024-25):

  Pattern                       Pool  Count/yr  Sum
  ---------------------------  ----  --------  ----
  (0.5, 0.3, 0.1, 0.1)         HS    ~350      1.0
  (0.5, 0.4, NaN, 0.1)         HS    ~7        1.0 (growth25 absent)
  (0.5, 0.4, 0.1, NaN)         K8    ~1293     1.0 (ccr absent)
  (0.5, 0.5, NaN, NaN)         K8    ~42       1.0 (growth25+ccr absent)
  (NaN, NaN, NaN, NaN)         either ~3 (0 in 24-25)  N/A  ← anomaly

Partially-missing weights (rows 2-4) are handled by the loader: it coerces
absent weights to 0.0 for eligible rows that have at least one non-NaN
weight. The result satisfies the original CHECK.

The all-NULL row pattern is the eligibility anomaly (lg_ineligible=false
but no scores AND no weights published). 6 lifetime rows; trending toward
zero. validate_eligibility_score_consistency catches these as WARNING.

This migration adds a third branch to lg_weights_sum: allow all four
weights AND all four scores to be NULL together (the anomaly state).
The validator then logs a warning per row but the load proceeds.

Postgres-only.
"""
from typing import Sequence, Union

from alembic import op


revision: str = "f5b6c7d8e9f0"
down_revision: Union[str, Sequence[str], None] = "f4b5c6d7e8f9"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("ALTER TABLE tn_letter_grade DROP CONSTRAINT lg_weights_sum")
    op.execute("""
        ALTER TABLE tn_letter_grade ADD CONSTRAINT lg_weights_sum CHECK (
            lg_ineligible = true
            OR (
                -- Normal eligible row: all weights present and sum to 1.0
                ach_weight       IS NOT NULL
                AND growth_weight    IS NOT NULL
                AND growth25_weight  IS NOT NULL
                AND ccr_weight       IS NOT NULL
                AND abs((ach_weight + growth_weight + growth25_weight + ccr_weight) - 1.0) < 0.001
            )
            OR (
                -- Eligibility anomaly: TDOE marked eligible but published no
                -- scores or weights. 6 lifetime rows (2022-23 + 2023-24);
                -- 0 in 2024-25. validate_eligibility_score_consistency
                -- catches as WARNING per load.
                ach_weight       IS NULL
                AND growth_weight    IS NULL
                AND growth25_weight  IS NULL
                AND ccr_weight       IS NULL
                AND ach_score        IS NULL
                AND growth_score     IS NULL
                AND growth25_score   IS NULL
                AND ccr_score        IS NULL
            )
        )
    """)


def downgrade() -> None:
    op.execute("ALTER TABLE tn_letter_grade DROP CONSTRAINT lg_weights_sum")
    op.execute("""
        ALTER TABLE tn_letter_grade ADD CONSTRAINT lg_weights_sum CHECK (
            lg_ineligible = true
            OR (
                ach_weight       IS NOT NULL
                AND growth_weight    IS NOT NULL
                AND growth25_weight  IS NOT NULL
                AND ccr_weight       IS NOT NULL
                AND abs((ach_weight + growth_weight + growth25_weight + ccr_weight) - 1.0) < 0.001
            )
        )
    """)
