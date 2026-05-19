"""admit 'Grade 2' as a Test value in tn_tvaas_school_subject

Revision ID: f6c7d8e9f0a1
Revises: f5b6c7d8e9f0
Create Date: 2026-05-18

Discovered during canonical reload: the 2017 TVAAS school-subject file
publishes a third Test category, 'Grade 2', alongside 'Grades 3-8' and
'EOC'. TDOE reported grade-2 assessment separately that year (K-2 is
outside TCAP). 'Grade 2' does not appear in 2018+ subject files but is
real, valid, schema-violating data — relax the CHECK to admit it.
"""
from alembic import op


revision = "f6c7d8e9f0a1"
down_revision = "f5b6c7d8e9f0"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("ALTER TABLE tn_tvaas_school_subject DROP CONSTRAINT tn_tvaas_school_subject_test_check")
    op.execute(
        "ALTER TABLE tn_tvaas_school_subject "
        "ADD CONSTRAINT tn_tvaas_school_subject_test_check "
        "CHECK (test IN ('Grades 3-8', 'EOC', 'ACT', 'Grade 2'))"
    )


def downgrade() -> None:
    op.execute("ALTER TABLE tn_tvaas_school_subject DROP CONSTRAINT tn_tvaas_school_subject_test_check")
    op.execute(
        "ALTER TABLE tn_tvaas_school_subject "
        "ADD CONSTRAINT tn_tvaas_school_subject_test_check "
        "CHECK (test IN ('Grades 3-8', 'EOC', 'ACT'))"
    )
