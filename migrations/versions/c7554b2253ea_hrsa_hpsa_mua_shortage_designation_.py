"""HRSA HPSA + MUA shortage designation registries

Revision ID: c7554b2253ea
Revises: c3a8f1b0e2d4
Create Date: 2026-05-04

Source files (`data/raw/fqhcs/`):
- BCD_HPSA_FCT_DET_PC.xlsx — Primary Care HPSAs (~78K component rows)
- BCD_HPSA_FCT_DET_MH.xlsx — Mental Health HPSAs
- BCD_HPSA_FCT_DET_DH.xlsx — Dental HPSAs
- MUA_DET.xlsx — Medically Underserved Areas / Populations

HRSA publishes one HPSA designation as multiple "component" rows
(one row per geographic component of the designation — e.g., a HPSA
covering 3 counties has 3 rows). Same for MUA. We preserve component
granularity so callers can query by county FIPS or census tract
without spatial joins to the HPSA polygon shapefiles.
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = 'c7554b2253ea'
down_revision: Union[str, Sequence[str], None] = 'c3a8f1b0e2d4'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE IF NOT EXISTS hrsa_hpsa_designations (
            id                          SERIAL PRIMARY KEY,
            discipline                  TEXT NOT NULL,         -- 'PC', 'MH', 'DH'
            hpsa_id                     TEXT NOT NULL,
            hpsa_name                   TEXT,
            designation_type            TEXT,
            hpsa_score                  REAL,
            hpsa_status                 TEXT,                  -- Designated / Withdrawn / Proposed For Withdrawal
            designation_date            DATE,
            last_update_date            DATE,
            withdrawn_date              DATE,

            -- Geography (component-level)
            state_abbr                  TEXT,
            state_fips                  TEXT,
            county_fips                 TEXT,                  -- 5-digit (state+county)
            county_name                 TEXT,
            postal_code                 TEXT,
            metro_indicator             TEXT,
            rural_status                TEXT,
            latitude                    REAL,
            longitude                   REAL,

            -- Population & shortage metrics
            hpsa_geo_id                 TEXT,
            designation_population      INTEGER,
            served_population           INTEGER,
            underserved_population      INTEGER,
            resident_civilian_pop       INTEGER,
            pct_below_100pct_poverty    REAL,
            formal_ratio                TEXT,
            hpsa_fte                    REAL,
            hpsa_shortage               REAL,
            provider_ratio_goal         TEXT,
            degree_of_shortage          TEXT,

            -- Component / facility linkage
            component_name              TEXT,
            component_type              TEXT,
            component_source_id         TEXT,
            bhcmis_org_id               TEXT,                  -- when HPSA is auto-designated for an FQHC

            source_file                 TEXT,
            loaded_at                   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    op.execute("CREATE INDEX IF NOT EXISTS ix_hpsa_county_fips ON hrsa_hpsa_designations (county_fips)")
    op.execute("CREATE INDEX IF NOT EXISTS ix_hpsa_state_status_disc ON hrsa_hpsa_designations (state_abbr, hpsa_status, discipline)")
    op.execute("CREATE INDEX IF NOT EXISTS ix_hpsa_hpsa_id ON hrsa_hpsa_designations (hpsa_id)")
    op.execute("CREATE INDEX IF NOT EXISTS ix_hpsa_bhcmis_org_id ON hrsa_hpsa_designations (bhcmis_org_id)")

    op.execute("""
        CREATE TABLE IF NOT EXISTS hrsa_mua_designations (
            id                          SERIAL PRIMARY KEY,
            mua_id                      TEXT NOT NULL,
            mua_name                    TEXT,
            designation_type            TEXT,                  -- MUA / MUP / Governor Exception
            mua_status                  TEXT,
            imu_score                   REAL,                  -- Index of Medical Underservice (0-100, lower = worse)
            designation_date            DATE,
            update_date                 DATE,
            withdrawal_date             DATE,
            break_in_designation        TEXT,

            population_type             TEXT,
            metro_indicator             TEXT,

            -- Geography
            state_abbr                  TEXT,
            state_fips                  TEXT,
            county_fips                 TEXT,                  -- 5-digit
            county_name                 TEXT,
            county_subdivision_name     TEXT,
            census_tract                TEXT,
            rural_status                TEXT,

            -- Component
            component_name              TEXT,
            component_type              TEXT,

            -- Underservice metrics (used to compute IMU)
            pct_below_100pct_poverty    REAL,
            pct_age_65_plus             REAL,
            infant_mortality_rate       REAL,
            providers_per_1000          REAL,
            designation_population      INTEGER,
            total_population            INTEGER,

            source_file                 TEXT,
            loaded_at                   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    op.execute("CREATE INDEX IF NOT EXISTS ix_mua_county_fips ON hrsa_mua_designations (county_fips)")
    op.execute("CREATE INDEX IF NOT EXISTS ix_mua_census_tract ON hrsa_mua_designations (census_tract)")
    op.execute("CREATE INDEX IF NOT EXISTS ix_mua_state_status ON hrsa_mua_designations (state_abbr, mua_status)")
    op.execute("CREATE INDEX IF NOT EXISTS ix_mua_mua_id ON hrsa_mua_designations (mua_id)")


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS hrsa_mua_designations")
    op.execute("DROP TABLE IF EXISTS hrsa_hpsa_designations")
