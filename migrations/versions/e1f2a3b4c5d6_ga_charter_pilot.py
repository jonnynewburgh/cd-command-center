"""ga_charter_pilot: GA local charter pilot accountability snapshot

Revision ID: e1f2a3b4c5d6
Revises: d1e2f3a4b5c6
Create Date: 2026-05-15

GA's CCRPI (College and Career Ready Performance Index) is the state DOE
accountability framework for individual schools. The SCSC CPF data in
scsc_cpf only covers SCSC-authorized charters; LEA-authorized charters
need a separate place for their CCRPI scores and renewal status.

This table holds one snapshot row per school per school_year, mirroring
the columns of data/raw/charter accountability/GA/local_charter_dataset.csv:
identity + authorizer, school metadata (year_opened, grade span, CMO),
renewal status (charter_end, years_to_renewal), CCRPI scores (ccrpi_24,
ccrpi_23, ccrpi_avg, ccrpi_desig), per-pupil expenditure (ppe_avg),
financial efficiency stars (fesr_stars), and pre-computed composites
(acad_proxy, fin_proxy, risk_score).
"""
from typing import Sequence, Union

from alembic import op


revision: str = "e1f2a3b4c5d6"
down_revision: Union[str, Sequence[str], None] = "d1e2f3a4b5c6"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    bind = op.get_bind()
    is_pg = bind.dialect.name == "postgresql"
    pk = "SERIAL PRIMARY KEY" if is_pg else "INTEGER PRIMARY KEY AUTOINCREMENT"

    op.execute(f"""
        CREATE TABLE IF NOT EXISTS ga_charter_pilot (
            id {pk},
            nces_id TEXT,
            school_name TEXT NOT NULL,
            school_year TEXT NOT NULL,
            authorizer TEXT,
            year_opened INTEGER,
            grade_span TEXT,
            curriculum_type TEXT,
            location_type TEXT,
            has_cmo INTEGER,
            cmo_name TEXT,
            is_conversion INTEGER,
            charter_end INTEGER,
            years_to_renewal INTEGER,
            ccrpi_24 REAL,
            ccrpi_23 REAL,
            ccrpi_avg REAL,
            ppe_avg REAL,
            fesr_stars REAL,
            acad_proxy REAL,
            fin_proxy REAL,
            risk_score REAL,
            ccrpi_desig TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE (school_name, school_year)
        )
    """)
    op.execute("CREATE INDEX IF NOT EXISTS idx_ga_charter_pilot_nces      ON ga_charter_pilot(nces_id)")
    op.execute("CREATE INDEX IF NOT EXISTS idx_ga_charter_pilot_year      ON ga_charter_pilot(school_year)")
    op.execute("CREATE INDEX IF NOT EXISTS idx_ga_charter_pilot_authorizer ON ga_charter_pilot(authorizer)")
    op.execute("CREATE INDEX IF NOT EXISTS idx_ga_charter_pilot_desig     ON ga_charter_pilot(ccrpi_desig)")


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS ga_charter_pilot")
