"""fqhc UDS pipeline: org-level keys + fqhc_uds_reports

Revision ID: 9e3b590fa748
Revises: f19ded25b983
Create Date: 2026-05-04

Adds the org-level join keys to `fqhc` (so UDS reports can roll up
across an org's many sites) and creates `fqhc_uds_reports`, the
grantee-per-year wide table populated by `etl/load_fqhc_uds.py` from
HRSA's H80 / Look-Alike FOIA xlsx releases.

Source columns:
- `org_bhcmis_id`            — HRSA "BHCMIS Organization Identification
                              Number". 1,525 distinct values across the
                              ~18.8K sites in `fqhc`.
- `health_center_grant_number` — HRSA "Health Center Number" (H80 /
                              H80CS / LAL series). Same cardinality as
                              org_bhcmis_id; this is the canonical join
                              key UDS reports use.

UDS payer-mix / encounters / FTE / quality / financials are reported
once per grantee per data_year. Schema is curated wide format with the
underwriting-relevant ~50 measures pulled out as columns; everything
else is preserved in `raw_metrics_json` so future analyses don't need
a new migration to read additional UDS fields.
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision: str = '9e3b590fa748'
down_revision: Union[str, Sequence[str], None] = 'f19ded25b983'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("ALTER TABLE fqhc ADD COLUMN IF NOT EXISTS org_bhcmis_id TEXT")
    op.execute("ALTER TABLE fqhc ADD COLUMN IF NOT EXISTS health_center_grant_number TEXT")
    op.execute("CREATE INDEX IF NOT EXISTS ix_fqhc_org_bhcmis_id ON fqhc (org_bhcmis_id)")
    op.execute("CREATE INDEX IF NOT EXISTS ix_fqhc_grant_number ON fqhc (health_center_grant_number)")

    op.execute("""
        CREATE TABLE IF NOT EXISTS fqhc_uds_reports (
            id                              SERIAL PRIMARY KEY,
            grant_number                    TEXT NOT NULL,
            org_bhcmis_id                   TEXT,
            health_center_name              TEXT,
            state                           TEXT,
            data_year                       INTEGER NOT NULL,
            grantee_type                    TEXT,

            total_patients                  INTEGER,
            total_visits                    INTEGER,
            medical_visits                  INTEGER,
            dental_visits                   INTEGER,
            mental_health_visits            INTEGER,
            substance_use_visits            INTEGER,
            vision_visits                   INTEGER,
            enabling_services_visits        INTEGER,

            patients_under_18               INTEGER,
            patients_65_plus                INTEGER,

            pct_medicaid                    REAL,
            pct_medicare                    REAL,
            pct_private_insurance           REAL,
            pct_uninsured                   REAL,
            pct_other_public                REAL,

            pct_below_100pct_poverty        REAL,
            pct_100_to_200_poverty          REAL,

            patients_homeless               INTEGER,
            patients_agricultural           INTEGER,
            patients_public_housing         INTEGER,
            patients_school_based           INTEGER,
            patients_veterans               INTEGER,

            pct_hispanic                    REAL,
            pct_black                       REAL,
            pct_white                       REAL,
            pct_asian                       REAL,
            pct_aian                        REAL,
            pct_nhpi                        REAL,
            pct_best_served_other_lang      REAL,

            physicians_fte                  REAL,
            np_pa_cnm_fte                   REAL,
            nurses_fte                      REAL,
            dentists_fte                    REAL,
            bh_providers_fte                REAL,
            total_clinical_fte              REAL,
            total_fte                       REAL,

            diabetes_a1c_poor_control_pct   REAL,
            hypertension_control_pct        REAL,
            breast_cancer_screening_pct     REAL,
            cervical_cancer_screening_pct   REAL,
            colorectal_cancer_screening_pct REAL,
            depression_screening_pct        REAL,
            tobacco_screening_pct           REAL,

            total_costs                     BIGINT,
            total_revenue                   BIGINT,
            patient_service_revenue         BIGINT,
            bphc_grant_revenue              BIGINT,
            other_federal_revenue           BIGINT,
            state_local_revenue             BIGINT,
            private_grant_revenue           BIGINT,
            self_pay_revenue                BIGINT,

            raw_metrics_json                JSONB,
            source_file                     TEXT,
            loaded_at                       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

            CONSTRAINT uq_fqhc_uds_grant_year UNIQUE (grant_number, data_year)
        )
    """)
    op.execute("CREATE INDEX IF NOT EXISTS ix_fqhc_uds_org_bhcmis_id ON fqhc_uds_reports (org_bhcmis_id)")
    op.execute("CREATE INDEX IF NOT EXISTS ix_fqhc_uds_state_year ON fqhc_uds_reports (state, data_year)")


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS fqhc_uds_reports")
    op.execute("DROP INDEX IF EXISTS ix_fqhc_grant_number")
    op.execute("DROP INDEX IF EXISTS ix_fqhc_org_bhcmis_id")
    op.execute("ALTER TABLE fqhc DROP COLUMN IF EXISTS health_center_grant_number")
    op.execute("ALTER TABLE fqhc DROP COLUMN IF EXISTS org_bhcmis_id")
