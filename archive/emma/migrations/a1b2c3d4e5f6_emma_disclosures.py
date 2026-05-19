"""emma continuing-disclosure tables: emma_issuers, emma_disclosures

Revision ID: a1b2c3d4e5f6
Revises: f5b6c7d8e9f0
Create Date: 2026-05-18

Phase 1 of the EMMA ETL (see docs/emma_etl_brief.md). Adds two tables that
hold nonprofit conduit-borrower disclosures pulled from emma.msrb.org:

  emma_issuers     — one row per obligated person (the borrower behind the
                     bonds). EIN is populated by the BMF matcher after
                     enumeration. Many obligors will not match (project-LLCs,
                     governmental entities, hospital parents != borrower).
  emma_disclosures — one row per audited-financials / annual-financial PDF
                     pulled from EMMA, with on-disk path + SHA256.

cusip6_list / cusips are JSON-encoded TEXT (not Postgres text[]). Reason:
the rest of the schema is portable across SQLite (dev) and Postgres (prod);
keeping these columns TEXT avoids introducing a dialect-specific column type
just for these two tables. Read accessors json.loads() the values.

CREATE TABLE IF NOT EXISTS is idempotent — safe to re-run.
"""
from typing import Sequence, Union

from alembic import op


revision: str = "a1b2c3d4e5f6"
down_revision: Union[str, Sequence[str], None] = "f5b6c7d8e9f0"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    bind = op.get_bind()
    is_pg = bind.dialect.name == "postgresql"
    tstz = "TIMESTAMPTZ" if is_pg else "TIMESTAMP"

    # emma_issuers — one row per obligated person
    op.execute(f"""
        CREATE TABLE IF NOT EXISTS emma_issuers (
            obligor_id TEXT PRIMARY KEY,
            obligor_name TEXT NOT NULL,
            obligor_name_normalized TEXT,
            state TEXT,
            sector TEXT,
            cusip6_list TEXT,
            ein TEXT,
            ein_match_confidence REAL,
            ein_match_method TEXT,
            first_seen DATE,
            last_seen DATE,
            fetched_at {tstz} DEFAULT CURRENT_TIMESTAMP
        )
    """)
    op.execute("CREATE INDEX IF NOT EXISTS idx_emma_issuers_ein     ON emma_issuers(ein)")
    op.execute("CREATE INDEX IF NOT EXISTS idx_emma_issuers_state   ON emma_issuers(state)")
    op.execute("CREATE INDEX IF NOT EXISTS idx_emma_issuers_norm    ON emma_issuers(obligor_name_normalized, state)")

    # emma_disclosures — one row per continuing-disclosure document
    op.execute(f"""
        CREATE TABLE IF NOT EXISTS emma_disclosures (
            emma_doc_id TEXT PRIMARY KEY,
            obligor_id TEXT,
            filing_date DATE,
            period_end_date DATE,
            document_category TEXT,
            document_subcategory TEXT,
            document_title TEXT,
            cusips TEXT,
            source_url TEXT,
            pdf_path TEXT,
            pdf_sha256 TEXT,
            pdf_size_bytes BIGINT,
            download_status TEXT,
            fetched_at {tstz} DEFAULT CURRENT_TIMESTAMP
        )
    """)
    op.execute("CREATE INDEX IF NOT EXISTS idx_emma_disc_obligor ON emma_disclosures(obligor_id)")
    op.execute("CREATE INDEX IF NOT EXISTS idx_emma_disc_status  ON emma_disclosures(download_status)")
    op.execute("CREATE INDEX IF NOT EXISTS idx_emma_disc_date    ON emma_disclosures(filing_date)")


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS emma_disclosures")
    op.execute("DROP TABLE IF EXISTS emma_issuers")
