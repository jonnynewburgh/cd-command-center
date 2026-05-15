"""post-baseline ETL tables: scsc_cpf, federal_audits, federal_audit_programs, headstart_programs

Revision ID: d1e2f3a4b5c6
Revises: b2c3d4e5f6a1
Create Date: 2026-05-10

These tables exist in db.init_db() but had no Alembic revision, so a fresh
PostgreSQL DB initialized via `alembic upgrade head` was missing them.
Per CLAUDE.md, db.init_db() is FROZEN at f19ded25b983; new tables belong
in migrations. This revision back-fills the four post-baseline tables.

CREATE TABLE IF NOT EXISTS makes this idempotent against any local SQLite
database that already ran init_db() and has these tables.
"""
from typing import Sequence, Union

from alembic import op


revision: str = "d1e2f3a4b5c6"
down_revision: Union[str, Sequence[str], None] = "b2c3d4e5f6a1"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    bind = op.get_bind()
    is_pg = bind.dialect.name == "postgresql"

    pk_autoincrement = "SERIAL PRIMARY KEY" if is_pg else "INTEGER PRIMARY KEY AUTOINCREMENT"

    # scsc_cpf — SCSC CPF accountability scores for GA charter schools
    op.execute(f"""
        CREATE TABLE IF NOT EXISTS scsc_cpf (
            id {pk_autoincrement},
            nces_id TEXT,
            school_name TEXT NOT NULL,
            school_year TEXT NOT NULL,
            academic_designation TEXT,
            financial_designation TEXT,
            financial_indicator_1 REAL,
            financial_indicator_2 REAL,
            operations_score REAL,
            operations_designation TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(school_name, school_year)
        )
    """)
    op.execute("CREATE INDEX IF NOT EXISTS idx_scsc_nces ON scsc_cpf(nces_id)")
    op.execute("CREATE INDEX IF NOT EXISTS idx_scsc_year ON scsc_cpf(school_year)")

    # federal_audits — one row per Single Audit submission (FAC / api.fac.gov)
    op.execute("""
        CREATE TABLE IF NOT EXISTS federal_audits (
            report_id TEXT PRIMARY KEY,
            auditee_ein TEXT,
            auditee_uei TEXT,
            auditee_name TEXT NOT NULL,
            entity_type TEXT,
            is_multiple_eins BOOLEAN,
            auditee_address_line_1 TEXT,
            auditee_city TEXT,
            auditee_state TEXT,
            auditee_zip TEXT,
            auditee_contact_name TEXT,
            auditee_contact_title TEXT,
            auditee_email TEXT,
            auditee_phone TEXT,
            auditee_certify_name TEXT,
            auditee_certify_title TEXT,
            auditee_certified_date DATE,
            audit_year INTEGER,
            fy_start_date DATE,
            fy_end_date DATE,
            audit_period_covered TEXT,
            audit_type TEXT,
            total_amount_expended BIGINT,
            dollar_threshold INTEGER,
            gaap_results TEXT,
            is_going_concern BOOLEAN,
            is_material_weakness BOOLEAN,
            is_significant_deficiency BOOLEAN,
            is_material_noncompliance BOOLEAN,
            is_low_risk_auditee BOOLEAN,
            agencies_with_prior_findings TEXT,
            cognizant_agency TEXT,
            oversight_agency TEXT,
            auditor_firm_name TEXT,
            auditor_ein TEXT,
            auditor_state TEXT,
            auditor_city TEXT,
            auditor_zip TEXT,
            auditor_address_line_1 TEXT,
            auditor_country TEXT,
            auditor_contact_name TEXT,
            auditor_contact_title TEXT,
            auditor_email TEXT,
            auditor_phone TEXT,
            auditor_certify_name TEXT,
            auditor_certify_title TEXT,
            auditor_certified_date DATE,
            submitted_date DATE,
            fac_accepted_date DATE,
            resubmission_version INTEGER,
            fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    op.execute("CREATE INDEX IF NOT EXISTS idx_federal_audits_ein        ON federal_audits(auditee_ein)")
    op.execute("CREATE INDEX IF NOT EXISTS idx_federal_audits_uei        ON federal_audits(auditee_uei)")
    op.execute("CREATE INDEX IF NOT EXISTS idx_federal_audits_state_year ON federal_audits(auditee_state, audit_year)")
    op.execute("CREATE INDEX IF NOT EXISTS idx_federal_audits_entity     ON federal_audits(entity_type)")

    # federal_audit_programs — per-ALN line items
    op.execute("""
        CREATE TABLE IF NOT EXISTS federal_audit_programs (
            report_id TEXT NOT NULL,
            award_reference TEXT NOT NULL,
            aln TEXT NOT NULL,
            federal_agency_prefix TEXT,
            federal_award_extension TEXT,
            federal_program_name TEXT,
            amount_expended BIGINT,
            federal_program_total BIGINT,
            is_loan BOOLEAN,
            loan_balance BIGINT,
            is_direct BOOLEAN,
            is_passthrough_award BOOLEAN,
            passthrough_amount BIGINT,
            is_major BOOLEAN,
            cluster_name TEXT,
            other_cluster_name TEXT,
            state_cluster_name TEXT,
            cluster_total BIGINT,
            findings_count INTEGER,
            audit_report_type TEXT,
            PRIMARY KEY (report_id, award_reference)
        )
    """)
    op.execute("CREATE INDEX IF NOT EXISTS idx_fap_aln       ON federal_audit_programs(aln)")
    op.execute("CREATE INDEX IF NOT EXISTS idx_fap_report_id ON federal_audit_programs(report_id)")
    op.execute("CREATE INDEX IF NOT EXISTS idx_fap_agency    ON federal_audit_programs(federal_agency_prefix)")

    # headstart_programs — Head Start PIR program-level data
    op.execute("""
        CREATE TABLE IF NOT EXISTS headstart_programs (
            grant_number TEXT NOT NULL,
            program_number TEXT NOT NULL,
            pir_year INTEGER NOT NULL,
            region TEXT,
            state TEXT,
            program_type TEXT,
            grantee_name TEXT,
            program_name TEXT,
            agency_type TEXT,
            agency_description TEXT,
            address TEXT,
            city TEXT,
            zip_code TEXT,
            phone TEXT,
            email TEXT,
            latitude REAL,
            longitude REAL,
            census_tract_id TEXT,
            funded_enrollment INTEGER,
            non_acf_enrollment INTEGER,
            total_cumulative_enrollment INTEGER,
            total_slots_center_based INTEGER,
            slots_at_child_care_partner INTEGER,
            total_classes INTEGER,
            home_based_slots INTEGER,
            family_child_care_slots INTEGER,
            children_lt1 INTEGER,
            children_1yr INTEGER,
            children_2yr INTEGER,
            children_3yr INTEGER,
            children_4yr INTEGER,
            children_5plus INTEGER,
            pregnant_women INTEGER,
            eligible_income INTEGER,
            eligible_public_assist INTEGER,
            eligible_foster INTEGER,
            eligible_homeless INTEGER,
            children_left_program INTEGER,
            children_end_of_year INTEGER,
            dual_language_learners INTEGER,
            children_transported INTEGER,
            children_with_subsidy INTEGER,
            total_staff INTEGER,
            total_contracted_staff INTEGER,
            classroom_teachers INTEGER,
            assistant_teachers INTEGER,
            teachers_ba_or_higher INTEGER,
            volunteers INTEGER,
            children_with_insurance_start INTEGER,
            children_with_insurance_end INTEGER,
            children_medicaid_start INTEGER,
            children_no_insurance_start INTEGER,
            children_with_medical_home_start INTEGER,
            children_at_fqhc_start INTEGER,
            child_care_partners INTEGER,
            leas_in_service_area INTEGER,
            data_source TEXT DEFAULT 'HSES PIR Export',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (grant_number, program_number, pir_year)
        )
    """)
    op.execute("CREATE INDEX IF NOT EXISTS idx_hs_state   ON headstart_programs(state)")
    op.execute("CREATE INDEX IF NOT EXISTS idx_hs_type    ON headstart_programs(program_type)")
    op.execute("CREATE INDEX IF NOT EXISTS idx_hs_grantee ON headstart_programs(grantee_name)")
    op.execute("CREATE INDEX IF NOT EXISTS idx_hs_tract   ON headstart_programs(census_tract_id)")
    op.execute("CREATE INDEX IF NOT EXISTS idx_hs_zip     ON headstart_programs(zip_code)")


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS headstart_programs")
    op.execute("DROP TABLE IF EXISTS federal_audit_programs")
    op.execute("DROP TABLE IF EXISTS federal_audits")
    op.execute("DROP TABLE IF EXISTS scsc_cpf")
