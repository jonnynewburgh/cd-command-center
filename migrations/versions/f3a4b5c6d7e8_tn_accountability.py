"""tn_accountability: TN charter accountability (TVAAS + A-F letter grade)

Revision ID: f3a4b5c6d7e8
Revises: f2a3b4c5d6e7
Create Date: 2026-05-17

Four tables comprising the TN charter accountability schema. All rise/fall
together as one logical schema:

  1. tn_tvaas_school_composite — annual school-level TVAAS composite scores
                                  (1-5 levels for overall + each subject)
  2. tn_tvaas_school_subject   — TVAAS by subject × grade × test × year
                                  (long form; growth_measure, index, level)
  3. tn_letter_grade           — A-F letter grade headline (wide, ~15 cols)
  4. tn_letter_grade_metric    — long companion with promoted breakdown
                                  dimensions (subgroup, subject, grade_band,
                                  ccr_component as first-class columns with
                                  'all' sentinel for "not broken out on this dim")

Year semantics: all `year` columns use the END year of the school year
(SY 2022-23 → 2023). Matches TDOE convention and the filename parser output.

Source-of-truth convention for letter-grade tables: tn_letter_grade_metric
holds every breakdown row from source including all-overall rows
(overall_success_rate_all_students, ccr_rate); tn_letter_grade holds the
headline-only fields (growth_score, weights, component letter grades) plus
denormalized copies of ach_score and ccr_score. See table COMMENTs for full
relationship.

Postgres-only.
"""
from typing import Sequence, Union

from alembic import op


revision: str = "f3a4b5c6d7e8"
down_revision: Union[str, Sequence[str], None] = "f2a3b4c5d6e7"
branch_labels: Union[str, Sequence[str], None] = None
# Explicit dependency on etl_load_log (FK target on every fact table here).
# Independent of tn_school_crosswalk (no FK between them — soft join only).
depends_on: Union[str, Sequence[str], None] = "f1a2b3c4d5e6"


def upgrade() -> None:
    # -----------------------------------------------------------------
    # 1. tn_tvaas_school_composite
    # -----------------------------------------------------------------
    op.execute("""
        CREATE TABLE IF NOT EXISTS tn_tvaas_school_composite (
            tdoe_system_id              integer   NOT NULL,
            tdoe_school_id              integer   NOT NULL,
            year                        integer   NOT NULL CHECK (year BETWEEN 2010 AND 2100),
            district_name               text,
            school_name                 text,
            overall_composite           smallint  CHECK (overall_composite           BETWEEN 1 AND 5),
            literacy_composite          smallint  CHECK (literacy_composite          BETWEEN 1 AND 5),
            numeracy_composite          smallint  CHECK (numeracy_composite          BETWEEN 1 AND 5),
            literacy_numeracy_composite smallint  CHECK (literacy_numeracy_composite BETWEEN 1 AND 5),
            science_composite           smallint  CHECK (science_composite           BETWEEN 1 AND 5),
            social_studies_composite    smallint  CHECK (social_studies_composite    BETWEEN 1 AND 5),
            source_load_id              bigint    NOT NULL REFERENCES etl_load_log(load_id),
            PRIMARY KEY (tdoe_system_id, tdoe_school_id, year)
        )
    """)
    op.execute("CREATE INDEX IF NOT EXISTS idx_tvaas_comp_year   ON tn_tvaas_school_composite(year)")
    op.execute("CREATE INDEX IF NOT EXISTS idx_tvaas_comp_school ON tn_tvaas_school_composite(tdoe_system_id, tdoe_school_id)")

    op.execute("""
        COMMENT ON TABLE tn_tvaas_school_composite IS
          'TVAAS composite scores per school per year. Levels are TDOE 1-5 (1=lowest growth, 5=highest). Coverage: 2015-2025 except 2019 (never published) and 2021 (COVID).'
    """)
    op.execute("""
        COMMENT ON COLUMN tn_tvaas_school_composite.year IS
          'School-year end year (SY 2022-23 → 2023). Convention shared across all TN fact tables.'
    """)
    op.execute("""
        COMMENT ON COLUMN tn_tvaas_school_composite.literacy_numeracy_composite IS
          'TDOE-published combined L&N composite; not a simple average of literacy_composite and numeracy_composite.'
    """)

    # -----------------------------------------------------------------
    # 2. tn_tvaas_school_subject
    # -----------------------------------------------------------------
    op.execute("""
        CREATE TABLE IF NOT EXISTS tn_tvaas_school_subject (
            tdoe_system_id   integer       NOT NULL,
            tdoe_school_id   integer       NOT NULL,
            year             integer       NOT NULL CHECK (year BETWEEN 2010 AND 2100),
            test             text          NOT NULL CHECK (test IN ('Grades 3-8', 'EOC', 'ACT')),
            subject          text          NOT NULL,
            grade            text          NOT NULL,
            growth_measure   numeric(5,1),
            standard_error   numeric(4,1)  CHECK (standard_error IS NULL OR standard_error >= 0),
            tvaas_index      numeric(6,2),
            level            smallint      CHECK (level BETWEEN 1 AND 5),
            n_students       integer       CHECK (n_students IS NULL OR n_students > 0),
            source_load_id   bigint        NOT NULL REFERENCES etl_load_log(load_id),
            PRIMARY KEY (tdoe_system_id, tdoe_school_id, year, test, subject, grade)
        )
    """)
    op.execute("CREATE INDEX IF NOT EXISTS idx_tvaas_subj_year    ON tn_tvaas_school_subject(year)")
    op.execute("CREATE INDEX IF NOT EXISTS idx_tvaas_subj_school  ON tn_tvaas_school_subject(tdoe_system_id, tdoe_school_id)")
    op.execute("CREATE INDEX IF NOT EXISTS idx_tvaas_subj_subject ON tn_tvaas_school_subject(subject, year)")

    op.execute("""
        COMMENT ON TABLE tn_tvaas_school_subject IS
          'TVAAS by school × test × subject × grade × year. Test=Grades 3-8 has per-grade rows; Test=EOC reports per course (grade=''Course'' sentinel); Test=ACT reports composite (grade=''Composite'' sentinel). Source data has NULL Grade for ACT/EOC — coalesced at load to keep PK NOT NULL.'
    """)
    op.execute("""
        COMMENT ON COLUMN tn_tvaas_school_subject.year IS
          'School-year end year (SY 2022-23 → 2023). Convention shared across all TN fact tables.'
    """)
    op.execute("""
        COMMENT ON COLUMN tn_tvaas_school_subject.grade IS
          'Grade as text. Values: "3"-"8" or "Cumulative Grades" (test=Grades 3-8); "Composite" (test=ACT); "Course" (test=EOC — End-Of-Course exams are tied to courses like Algebra I, not to a grade level).'
    """)
    op.execute("""
        COMMENT ON COLUMN tn_tvaas_school_subject.tvaas_index IS
          'TVAAS growth index ≈ growth_measure / standard_error. Renamed from "Index" in source (avoids Postgres reserved-ish identifier).'
    """)

    # -----------------------------------------------------------------
    # 3. tn_letter_grade
    # -----------------------------------------------------------------
    op.execute("""
        CREATE TABLE IF NOT EXISTS tn_letter_grade (
            tdoe_system_id    integer       NOT NULL,
            tdoe_school_id    integer       NOT NULL,
            year              integer       NOT NULL CHECK (year BETWEEN 2023 AND 2100),
            system_name       text,
            school_name       text,
            school_pool       text          NOT NULL CHECK (school_pool IN ('HS', 'K8')),
            lg_ineligible     boolean       NOT NULL,
            lg_score          numeric(2,1)  CHECK (lg_score IS NULL OR lg_score BETWEEN 1.0 AND 5.0),
            lg_grade          text          CHECK (lg_grade IS NULL OR lg_grade IN ('A','B','C','D','F')),
            ach_score         numeric(2,1)  CHECK (ach_score      IS NULL OR ach_score      BETWEEN 1.0 AND 5.0),
            growth_score      numeric(2,1)  CHECK (growth_score   IS NULL OR growth_score   BETWEEN 1.0 AND 5.0),
            growth25_score    numeric(2,1)  CHECK (growth25_score IS NULL OR growth25_score BETWEEN 1.0 AND 5.0),
            ccr_score         numeric(2,1)  CHECK (ccr_score      IS NULL OR ccr_score      BETWEEN 1.0 AND 5.0),
            ach_grade         text          CHECK (ach_grade      IS NULL OR ach_grade      IN ('A','B','C','D','F')),
            growth_grade      text          CHECK (growth_grade   IS NULL OR growth_grade   IN ('A','B','C','D','F')),
            growth25_grade    text          CHECK (growth25_grade IS NULL OR growth25_grade IN ('A','B','C','D','F')),
            ccr_grade         text          CHECK (ccr_grade      IS NULL OR ccr_grade      IN ('A','B','C','D','F')),
            ach_weight        numeric(3,2),
            growth_weight     numeric(3,2),
            growth25_weight   numeric(3,2),
            ccr_weight        numeric(3,2),
            source_load_id    bigint        NOT NULL REFERENCES etl_load_log(load_id),
            PRIMARY KEY (tdoe_system_id, tdoe_school_id, year),
            CONSTRAINT lg_weights_sum CHECK (
                lg_ineligible = true
                OR (
                    ach_weight      IS NOT NULL
                    AND growth_weight   IS NOT NULL
                    AND growth25_weight IS NOT NULL
                    AND ccr_weight      IS NOT NULL
                    AND abs((ach_weight + growth_weight + growth25_weight + ccr_weight) - 1.0) < 0.001
                )
            )
        )
    """)
    op.execute("CREATE INDEX IF NOT EXISTS idx_lg_year   ON tn_letter_grade(year)")
    op.execute("CREATE INDEX IF NOT EXISTS idx_lg_school ON tn_letter_grade(tdoe_system_id, tdoe_school_id)")
    op.execute("CREATE INDEX IF NOT EXISTS idx_lg_pool   ON tn_letter_grade(school_pool, year)")

    op.execute("""
        COMMENT ON TABLE tn_letter_grade IS
          'TN A-F school letter grade — headline metrics (overall + four components). Subgroup/subject/grade-band/ccr-component breakdowns live in tn_letter_grade_metric. Coverage starts SY 2022-23. ~10% of rows have lg_ineligible=true (alt schools, preschools, career-tech, online) — these have NULL scores and NULL weights. Relationship to tn_letter_grade_metric: ach_score and ccr_score also appear in metric as (achievement,all,all,all,all) and (ccr,all,all,all,all). growth_score, the four weights, and the four component letter grades have NO breakdown counterpart and live ONLY here.'
    """)
    op.execute("""
        COMMENT ON COLUMN tn_letter_grade.year IS
          'School-year end year (SY 2022-23 → 2023). Convention shared across all TN fact tables.'
    """)
    op.execute("""
        COMMENT ON COLUMN tn_letter_grade.school_pool IS
          'TDOE classification: HS (high school, 9-12) or K8 (everything else). Drives whether CCR component applies (HS only).'
    """)
    op.execute("""
        COMMENT ON COLUMN tn_letter_grade.lg_ineligible IS
          'TRUE when school is not eligible for a letter grade (typically alt schools, preschools, career-tech, online, adult ed). When TRUE, expect all score/grade/weight columns to be NULL. TDOE does not expose the specific reason. See validate.py rule `validate_eligibility_score_consistency` which asserts no row violates "lg_ineligible=false AND all four component scores NULL" (historical artifact: 3 rows in 2022-23, 3 in 2023-24, 0 in 2024-25 — trending toward zero, treated as a regression catcher).'
    """)
    op.execute("""
        COMMENT ON COLUMN tn_letter_grade.growth25_score IS
          'Growth score restricted to the bottom-quartile (25th percentile) students. Separate TDOE accountability indicator.'
    """)
    op.execute("""
        COMMENT ON COLUMN tn_letter_grade.ccr_score IS
          'College/Career Readiness score. NULL for K8 schools (CCR only applies to HS pool).'
    """)

    # -----------------------------------------------------------------
    # 4. tn_letter_grade_metric
    # -----------------------------------------------------------------
    op.execute("""
        CREATE TABLE IF NOT EXISTS tn_letter_grade_metric (
            tdoe_system_id   integer       NOT NULL,
            tdoe_school_id   integer       NOT NULL,
            year             integer       NOT NULL CHECK (year BETWEEN 2023 AND 2100),
            component        text          NOT NULL CHECK (component IN ('achievement', 'growth', 'ccr')),
            subgroup         text          NOT NULL CHECK (subgroup IN (
                                               'all','ed','el','swd','aian','asian','black','hispanic',
                                               'nhpi','white','bhn','super_subgroup'
                                           )),
            subject          text          NOT NULL CHECK (subject IN (
                                               'all','ela','math','science','social_studies',
                                               'literacy','numeracy','ela_math'
                                           )),
            grade_band       text          NOT NULL CHECK (grade_band IN ('all','g3_5','g6_8','g9_12')),
            ccr_component    text          NOT NULL CHECK (ccr_component IN ('all','act','postsec','ic','asvab')),
            unit             text          NOT NULL CHECK (unit IN ('pct','score_1_5')),
            metric_value     numeric(5,2),
            suppressed       boolean       NOT NULL DEFAULT false,
            source_load_id   bigint        NOT NULL REFERENCES etl_load_log(load_id),
            PRIMARY KEY (tdoe_system_id, tdoe_school_id, year, component, subgroup, subject, grade_band, ccr_component),
            CONSTRAINT lgm_component_dim_consistency CHECK (
                CASE component
                    WHEN 'achievement' THEN ccr_component = 'all'
                    WHEN 'growth'      THEN ccr_component = 'all' AND grade_band = 'all'
                    WHEN 'ccr'         THEN subject = 'all' AND grade_band = 'all'
                    ELSE false
                END
            ),
            CONSTRAINT lgm_unit_matches_component CHECK (
                (component IN ('achievement','ccr') AND unit = 'pct')
                OR (component = 'growth' AND unit = 'score_1_5')
            ),
            CONSTRAINT lgm_value_suppression CHECK (
                (suppressed = true  AND metric_value IS NULL)
                OR (suppressed = false AND metric_value IS NOT NULL)
            ),
            CONSTRAINT lgm_pct_range CHECK (
                unit <> 'pct' OR metric_value IS NULL OR (metric_value BETWEEN 0 AND 100)
            ),
            CONSTRAINT lgm_score_range CHECK (
                unit <> 'score_1_5' OR metric_value IS NULL OR (metric_value BETWEEN 1.0 AND 5.0)
            )
        )
    """)
    op.execute("CREATE INDEX IF NOT EXISTS idx_lgm_school        ON tn_letter_grade_metric(tdoe_system_id, tdoe_school_id, year)")
    op.execute("CREATE INDEX IF NOT EXISTS idx_lgm_year          ON tn_letter_grade_metric(year)")
    op.execute("CREATE INDEX IF NOT EXISTS idx_lgm_subgroup      ON tn_letter_grade_metric(subgroup)      WHERE subgroup      <> 'all'")
    op.execute("CREATE INDEX IF NOT EXISTS idx_lgm_subject       ON tn_letter_grade_metric(subject)       WHERE subject       <> 'all'")
    op.execute("CREATE INDEX IF NOT EXISTS idx_lgm_ccr_component ON tn_letter_grade_metric(ccr_component) WHERE ccr_component <> 'all'")

    op.execute("""
        COMMENT ON TABLE tn_letter_grade_metric IS
          'Long-form companion to tn_letter_grade. One row per (school, year, component, subgroup, subject, grade_band, ccr_component). Each breakdown dimension is a first-class NOT NULL column with ''all'' sentinel for "not broken out on this dim". Dimensional-sanity CHECK enforces which dims apply per component. Source-of-truth convention: this table contains every breakdown row published in the A-F source, INCLUDING all-overall (all,all,all,all) rows where they appear in source. For metrics without a breakdown counterpart (growth_score, weights, letter grades), tn_letter_grade is the only place that data lives.'
    """)
    op.execute("""
        COMMENT ON COLUMN tn_letter_grade_metric.year IS
          'School-year end year (SY 2022-23 → 2023). Convention shared across all TN fact tables.'
    """)
    op.execute("""
        COMMENT ON COLUMN tn_letter_grade_metric.subgroup IS
          'Demographic subgroup. ''all'' = not broken out. ED/EL/SWD/AIAN/Asian/Black/Hispanic/NHPI/White are federal categories. ''bhn'' (TN-specific) = Black + Hispanic + Native American historically-underserved composite. ''super_subgroup'' (TN-specific) = ED + EL + SWD + BHN aggregate. As of the 2024-25 file, bhn and super_subgroup appear only in growth indicators — not enforced by CHECK because TDOE could extend their use.'
    """)
    op.execute("""
        COMMENT ON COLUMN tn_letter_grade_metric.subject IS
          'Subject area. ''all'' = not broken out. ELA/Math/Science/Social Studies are standard. ''literacy''/''numeracy'' are TVAAS-style composites used in growth indicators. ''ela_math'' is the combined ELA+Math measure TN uses for growth subgroup breakdowns — as of the 2024-25 file, primarily appears with component=growth, but not CHECK-enforced.'
    """)
    op.execute("""
        COMMENT ON COLUMN tn_letter_grade_metric.grade_band IS
          'Grade band: g3_5 (3rd-5th), g6_8 (6th-8th), g9_12 (9th-12th), or ''all''. Underscored (not g3-5) to keep SQL identifier-friendly. Only achievement component uses grade bands.'
    """)
    op.execute("""
        COMMENT ON COLUMN tn_letter_grade_metric.ccr_component IS
          'CCR sub-component: act, postsec, ic, asvab, or ''all'' (overall CCR rate). Only ccr component uses this dimension.'
    """)
    op.execute("""
        COMMENT ON COLUMN tn_letter_grade_metric.unit IS
          'Unit of metric_value. ''pct'' (0-100) for achievement and ccr; ''score_1_5'' (1.0-5.0) for growth. Stored explicitly so analysts see the unit per-row. WARNING: ''pct'' values are not directly comparable across components — an achievement ''pct'' is a student-proficiency success rate, a ccr ''pct'' is a CCR-attainment rate. Always filter by component before aggregating.'
    """)
    op.execute("""
        COMMENT ON COLUMN tn_letter_grade_metric.suppressed IS
          'TRUE when source value was "Insufficient N Count" (subgroup N too small for publication). Distinguishes "missing because too few students" from "missing because not collected".'
    """)


def downgrade() -> None:
    # Reverse-order DROP. Each table is independent (no FK between the four),
    # so order doesn't strictly matter for correctness, but reverse-creation
    # is the convention.
    op.execute("DROP TABLE IF EXISTS tn_letter_grade_metric")
    op.execute("DROP TABLE IF EXISTS tn_letter_grade")
    op.execute("DROP TABLE IF EXISTS tn_tvaas_school_subject")
    op.execute("DROP TABLE IF EXISTS tn_tvaas_school_composite")
