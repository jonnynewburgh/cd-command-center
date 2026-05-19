"""rename accounts_payable to accts_payable_accrued

Revision ID: a8c3f5d9e2b1
Revises: e1f2a3b4c5d6
Create Date: 2026-05-16

Form 990 Part X line 17 is a SINGLE combined line — "Accounts payable and
accrued expenses" — and the XML group `AccountsPayableAccrExpnssGrp/EOYAmt`
is that single value. The column has been named `accounts_payable` since
the schema was first written, but every value it contains is actually
AP + accrued. Rename to `accts_payable_accrued` so future readers don't
treat it as just accounts payable.

Affects three tables that all derive from the same 990 source:
  - irs_990
  - irs_990_history
  - financial_ratios

The companion `accrued_expenses` column is left in place — it stays NULL
for 990-source rows by design and is reserved for future audit-PDF flows
that may populate it separately.
"""
from typing import Sequence, Union

from alembic import op


revision: str = 'a8c3f5d9e2b1'
down_revision: Union[str, Sequence[str], None] = 'e1f2a3b4c5d6'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


_TABLES = ("irs_990", "irs_990_history", "financial_ratios")


def upgrade() -> None:
    """Rename accounts_payable -> accts_payable_accrued in three tables."""
    for tbl in _TABLES:
        op.alter_column(tbl, "accounts_payable", new_column_name="accts_payable_accrued")


def downgrade() -> None:
    """Reverse the rename."""
    for tbl in _TABLES:
        op.alter_column(tbl, "accts_payable_accrued", new_column_name="accounts_payable")
