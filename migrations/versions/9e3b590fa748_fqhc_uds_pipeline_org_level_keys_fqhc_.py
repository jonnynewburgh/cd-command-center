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
from sqlalchemy import inspect


revision: str = '9e3b590fa748'
down_revision: Union[str, Sequence[str], None] = 'f19ded25b983'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    bind = op.get_bind()
    inspector = inspect(bind)

    fqhc_cols = {c['name'] for c in inspector.get_columns('fqhc')}
    if 'org_bhcmis_id' not in fqhc_cols:
        op.add_column('fqhc', sa.Column('org_bhcmis_id', sa.Text(), nullable=True))
    if 'health_center_grant_number' not in fqhc_cols:
        op.add_column('fqhc', sa.Column('health_center_grant_number', sa.Text(), nullable=True))

    existing_indexes = {ix['name'] for ix in inspector.get_indexes('fqhc')}
    if 'ix_fqhc_org_bhcmis_id' not in existing_indexes:
        op.create_index('ix_fqhc_org_bhcmis_id', 'fqhc', ['org_bhcmis_id'])
    if 'ix_fqhc_grant_number' not in existing_indexes:
        op.create_index('ix_fqhc_grant_number', 'fqhc', ['health_center_grant_number'])

    if 'fqhc_uds_reports' not in inspector.get_table_names():
        op.create_table(
            'fqhc_uds_reports',
            sa.Column('id', sa.Integer(), primary_key=True, autoincrement=True),
            sa.Column('grant_number', sa.Text(), nullable=False),
            sa.Column('org_bhcmis_id', sa.Text()),
            sa.Column('health_center_name', sa.Text()),
            sa.Column('state', sa.Text()),
            sa.Column('data_year', sa.Integer(), nullable=False),
            sa.Column('grantee_type', sa.Text()),

            sa.Column('total_patients', sa.Integer()),
            sa.Column('total_visits', sa.Integer()),
            sa.Column('medical_visits', sa.Integer()),
            sa.Column('dental_visits', sa.Integer()),
            sa.Column('mental_health_visits', sa.Integer()),
            sa.Column('substance_use_visits', sa.Integer()),
            sa.Column('vision_visits', sa.Integer()),
            sa.Column('enabling_services_visits', sa.Integer()),

            sa.Column('patients_under_18', sa.Integer()),
            sa.Column('patients_65_plus', sa.Integer()),

            sa.Column('pct_medicaid', sa.Float()),
            sa.Column('pct_medicare', sa.Float()),
            sa.Column('pct_private_insurance', sa.Float()),
            sa.Column('pct_uninsured', sa.Float()),
            sa.Column('pct_other_public', sa.Float()),

            sa.Column('pct_below_100pct_poverty', sa.Float()),
            sa.Column('pct_100_to_200_poverty', sa.Float()),

            sa.Column('patients_homeless', sa.Integer()),
            sa.Column('patients_agricultural', sa.Integer()),
            sa.Column('patients_public_housing', sa.Integer()),
            sa.Column('patients_school_based', sa.Integer()),
            sa.Column('patients_veterans', sa.Integer()),

            sa.Column('pct_hispanic', sa.Float()),
            sa.Column('pct_black', sa.Float()),
            sa.Column('pct_white', sa.Float()),
            sa.Column('pct_asian', sa.Float()),
            sa.Column('pct_aian', sa.Float()),
            sa.Column('pct_nhpi', sa.Float()),
            sa.Column('pct_best_served_other_lang', sa.Float()),

            sa.Column('physicians_fte', sa.Float()),
            sa.Column('np_pa_cnm_fte', sa.Float()),
            sa.Column('nurses_fte', sa.Float()),
            sa.Column('dentists_fte', sa.Float()),
            sa.Column('bh_providers_fte', sa.Float()),
            sa.Column('total_clinical_fte', sa.Float()),
            sa.Column('total_fte', sa.Float()),

            sa.Column('diabetes_a1c_poor_control_pct', sa.Float()),
            sa.Column('hypertension_control_pct', sa.Float()),
            sa.Column('breast_cancer_screening_pct', sa.Float()),
            sa.Column('cervical_cancer_screening_pct', sa.Float()),
            sa.Column('colorectal_cancer_screening_pct', sa.Float()),
            sa.Column('depression_screening_pct', sa.Float()),
            sa.Column('tobacco_screening_pct', sa.Float()),

            sa.Column('total_costs', sa.BigInteger()),
            sa.Column('total_revenue', sa.BigInteger()),
            sa.Column('patient_service_revenue', sa.BigInteger()),
            sa.Column('bphc_grant_revenue', sa.BigInteger()),
            sa.Column('other_federal_revenue', sa.BigInteger()),
            sa.Column('state_local_revenue', sa.BigInteger()),
            sa.Column('private_grant_revenue', sa.BigInteger()),
            sa.Column('self_pay_revenue', sa.BigInteger()),

            sa.Column('raw_metrics_json', sa.JSON()),
            sa.Column('source_file', sa.Text()),
            sa.Column('loaded_at', sa.TIMESTAMP(), server_default=sa.func.current_timestamp()),

            sa.UniqueConstraint('grant_number', 'data_year', name='uq_fqhc_uds_grant_year'),
        )

    uds_indexes = {ix['name'] for ix in inspector.get_indexes('fqhc_uds_reports')} \
        if 'fqhc_uds_reports' in inspector.get_table_names() else set()
    # Re-inspect after possible create_table above
    inspector = inspect(bind)
    uds_indexes = {ix['name'] for ix in inspector.get_indexes('fqhc_uds_reports')}
    if 'ix_fqhc_uds_org_bhcmis_id' not in uds_indexes:
        op.create_index('ix_fqhc_uds_org_bhcmis_id', 'fqhc_uds_reports', ['org_bhcmis_id'])
    if 'ix_fqhc_uds_state_year' not in uds_indexes:
        op.create_index('ix_fqhc_uds_state_year', 'fqhc_uds_reports', ['state', 'data_year'])


def downgrade() -> None:
    # SQLite did not support DROP COLUMN until 3.35; use batch_alter_table for portability.
    bind = op.get_bind()
    inspector = inspect(bind)

    if 'fqhc_uds_reports' in inspector.get_table_names():
        op.drop_table('fqhc_uds_reports')

    existing_indexes = {ix['name'] for ix in inspector.get_indexes('fqhc')}
    if 'ix_fqhc_grant_number' in existing_indexes:
        op.drop_index('ix_fqhc_grant_number', table_name='fqhc')
    if 'ix_fqhc_org_bhcmis_id' in existing_indexes:
        op.drop_index('ix_fqhc_org_bhcmis_id', table_name='fqhc')

    fqhc_cols = {c['name'] for c in inspector.get_columns('fqhc')}
    with op.batch_alter_table('fqhc') as batch_op:
        if 'health_center_grant_number' in fqhc_cols:
            batch_op.drop_column('health_center_grant_number')
        if 'org_bhcmis_id' in fqhc_cols:
            batch_op.drop_column('org_bhcmis_id')
