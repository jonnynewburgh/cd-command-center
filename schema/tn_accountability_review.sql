-- =====================================================================
-- TN Charter Accountability Schema — REVIEW DRAFT (v3)
-- =====================================================================
-- Pure-SQL draft for design review. After approval, convert to an
-- Alembic migration following the pattern in migrations/versions/.
--
-- Tables in this file (creation order = FK dependency order):
--   1. tn_tvaas_school_composite — annual school-level TVAAS composite scores
--   2. tn_tvaas_school_subject   — TVAAS by subject × grade × test × year (long)
--   3. tn_letter_grade           — A-F letter grade headline metrics (wide, ~15 cols)
--   4. tn_letter_grade_metric    — long companion with promoted breakdown dimensions
--                                  (subgroup, subject, grade_band, ccr_component as
--                                  first-class NOT NULL columns; 'all' sentinel
--                                  for "not broken out on this dim")
--
-- Sibling migrations (must run first):
--   - etl_load_log_review.sql        — shared load-provenance table
--   - tn_school_crosswalk_review.sql — TDOE ↔ NCES mapping
--
-- Year semantics: all `year` columns use the END year of the school year
-- (SY 2022-23 → 2023). Matches TDOE convention and the filename parser output.
--
-- Decisions encoded here:
--   - ID convention: tdoe_system_id, tdoe_school_id (never `system`/`school`)
--   - Subject-level PK includes `test` (future-proofs subject name reuse)
--   - ACT/EOC NULL-grade rows coalesce to 'Composite' / 'Course' at load time
--   - lg_ineligible is a boolean (TDOE doesn't expose the reason in data)
--   - Headline weights kept wide (CHECK sum=1.0 when eligible)
--   - source_load_id NOT NULL on every fact table — no orphan rows
--   - Promoted dimensions on tn_letter_grade_metric — every breakdown is
--     a first-class CHECK-constrained column, with dimensional sanity CHECK
--     enforcing which dims apply to which component
--   - unit kept on metric table with CHECK tied to component (defensive
--     denormalization — saves a join/lookup for analysts)
-- =====================================================================


-- ---------------------------------------------------------------------
-- 1. tn_tvaas_school_composite — annual school-level composites
-- ---------------------------------------------------------------------
CREATE TABLE tn_tvaas_school_composite (
    tdoe_system_id              integer   NOT NULL,
    tdoe_school_id              integer   NOT NULL,
    year                        integer   NOT NULL CHECK (year BETWEEN 2010 AND 2100),
    district_name               text,
    school_name                 text,
    overall_composite           smallint  CHECK (overall_composite           BETWEEN 1 AND 5),
    literacy_composite          smallint  CHECK (literacy_composite          BETWEEN 1 AND 5),
    numeracy_composite          smallint  CHECK (numeracy_composite          BETWEEN 1 AND 5),
    literacy_numeracy_composite smallint  CHECK (literacy_numeracy_composite BETWEEN 1 AND 5),
    science_composite           smallint  CHECK (science_composite           BETWEEN 1 AND 5),  -- NULL in 2015 (not yet reported)
    social_studies_composite    smallint  CHECK (social_studies_composite    BETWEEN 1 AND 5),  -- NULL pre-2016
    source_load_id              bigint    NOT NULL REFERENCES etl_load_log(load_id),
    PRIMARY KEY (tdoe_system_id, tdoe_school_id, year)
);

CREATE INDEX idx_tvaas_comp_year    ON tn_tvaas_school_composite(year);
CREATE INDEX idx_tvaas_comp_school  ON tn_tvaas_school_composite(tdoe_system_id, tdoe_school_id);

COMMENT ON TABLE tn_tvaas_school_composite IS
  'TVAAS composite scores per school per year. Levels are TDOE 1-5 (1=lowest growth, 5=highest). Coverage: 2015–2025 except 2019 (never published) and 2021 (COVID — no statewide testing).';
COMMENT ON COLUMN tn_tvaas_school_composite.year IS
  'School-year end year (SY 2022-23 → 2023). Convention shared across all TN fact tables.';
COMMENT ON COLUMN tn_tvaas_school_composite.literacy_numeracy_composite IS
  'TDOE-published combined L&N composite; not a simple average of literacy_composite and numeracy_composite.';


-- ---------------------------------------------------------------------
-- 2. tn_tvaas_school_subject — long: subject × grade × test × year
-- ---------------------------------------------------------------------
CREATE TABLE tn_tvaas_school_subject (
    tdoe_system_id   integer       NOT NULL,
    tdoe_school_id   integer       NOT NULL,
    year             integer       NOT NULL CHECK (year BETWEEN 2010 AND 2100),
    test             text          NOT NULL CHECK (test IN ('Grades 3-8', 'EOC', 'ACT')),
    subject          text          NOT NULL,
    grade            text          NOT NULL,                          -- '3'..'8' or 'Cumulative Grades' for Grades 3-8; 'Course' for EOC; 'Composite' for ACT
    growth_measure   numeric(5,1),                                    -- observed: -22.5 to 36.1
    standard_error   numeric(4,1)  CHECK (standard_error IS NULL OR standard_error >= 0),  -- observed: 0.1 to 5.9
    tvaas_index      numeric(6,2),                                    -- observed: -14.24 to 26.32 (growth_measure / standard_error)
    level            smallint      CHECK (level BETWEEN 1 AND 5),     -- normalized from "Level 1".."Level 5" text
    n_students       integer       CHECK (n_students IS NULL OR n_students > 0),
    source_load_id   bigint        NOT NULL REFERENCES etl_load_log(load_id),
    PRIMARY KEY (tdoe_system_id, tdoe_school_id, year, test, subject, grade)
);

CREATE INDEX idx_tvaas_subj_year     ON tn_tvaas_school_subject(year);
CREATE INDEX idx_tvaas_subj_school   ON tn_tvaas_school_subject(tdoe_system_id, tdoe_school_id);
CREATE INDEX idx_tvaas_subj_subject  ON tn_tvaas_school_subject(subject, year);

COMMENT ON TABLE tn_tvaas_school_subject IS
  'TVAAS by school × test × subject × grade × year. Test=Grades 3-8 has per-grade rows (3..8 plus "Cumulative Grades"); Test=EOC reports per course (grade=''Course'' sentinel); Test=ACT reports composite (grade=''Composite'' sentinel). Source data has NULL Grade for ACT/EOC — coalesced at load to keep PK NOT NULL.';
COMMENT ON COLUMN tn_tvaas_school_subject.year IS
  'School-year end year (SY 2022-23 → 2023). Convention shared across all TN fact tables.';
COMMENT ON COLUMN tn_tvaas_school_subject.grade IS
  'Grade as text. Values: "3"-"8" or "Cumulative Grades" (test=Grades 3-8); "Composite" (test=ACT — ACT reports a single English/Math/Reading/Science composite, not per-grade); "Course" (test=EOC — End-Of-Course exams are tied to courses like Algebra I, Biology I, not to a grade level).';
COMMENT ON COLUMN tn_tvaas_school_subject.tvaas_index IS
  'TVAAS growth index ≈ growth_measure / standard_error. Renamed from "Index" in source (avoids Postgres reserved-ish identifier).';


-- ---------------------------------------------------------------------
-- 3. tn_letter_grade — A-F headline (wide, ~15 cols)
-- ---------------------------------------------------------------------
CREATE TABLE tn_letter_grade (
    tdoe_system_id    integer       NOT NULL,
    tdoe_school_id    integer       NOT NULL,
    year              integer       NOT NULL CHECK (year BETWEEN 2023 AND 2100),   -- A-F began SY 2022-23 (end-year 2023)
    system_name       text,
    school_name       text,
    school_pool       text          NOT NULL CHECK (school_pool IN ('HS', 'K8')),
    lg_ineligible     boolean       NOT NULL,
    lg_score          numeric(2,1)  CHECK (lg_score IS NULL OR lg_score BETWEEN 1.0 AND 5.0),
    lg_grade          text          CHECK (lg_grade IS NULL OR lg_grade IN ('A','B','C','D','F')),

    ach_score         numeric(2,1)  CHECK (ach_score        IS NULL OR ach_score        BETWEEN 1.0 AND 5.0),
    growth_score      numeric(2,1)  CHECK (growth_score     IS NULL OR growth_score     BETWEEN 1.0 AND 5.0),
    growth25_score    numeric(2,1)  CHECK (growth25_score   IS NULL OR growth25_score   BETWEEN 1.0 AND 5.0),
    ccr_score         numeric(2,1)  CHECK (ccr_score        IS NULL OR ccr_score        BETWEEN 1.0 AND 5.0),

    ach_grade         text          CHECK (ach_grade        IS NULL OR ach_grade        IN ('A','B','C','D','F')),
    growth_grade      text          CHECK (growth_grade     IS NULL OR growth_grade     IN ('A','B','C','D','F')),
    growth25_grade    text          CHECK (growth25_grade   IS NULL OR growth25_grade   IN ('A','B','C','D','F')),
    ccr_grade         text          CHECK (ccr_grade        IS NULL OR ccr_grade        IN ('A','B','C','D','F')),

    ach_weight        numeric(3,2),
    growth_weight     numeric(3,2),
    growth25_weight   numeric(3,2),
    ccr_weight        numeric(3,2),

    source_load_id    bigint        NOT NULL REFERENCES etl_load_log(load_id),

    PRIMARY KEY (tdoe_system_id, tdoe_school_id, year),

    CONSTRAINT lg_weights_sum CHECK (
        lg_ineligible = true
        OR (
            ach_weight     IS NOT NULL
            AND growth_weight   IS NOT NULL
            AND growth25_weight IS NOT NULL
            AND ccr_weight      IS NOT NULL
            AND abs((ach_weight + growth_weight + growth25_weight + ccr_weight) - 1.0) < 0.001
        )
    )
);

CREATE INDEX idx_lg_year    ON tn_letter_grade(year);
CREATE INDEX idx_lg_school  ON tn_letter_grade(tdoe_system_id, tdoe_school_id);
CREATE INDEX idx_lg_pool    ON tn_letter_grade(school_pool, year);

COMMENT ON TABLE tn_letter_grade IS
  'TN A-F school letter grade — headline metrics (overall + four components). Subgroup/subject/grade-band/ccr-component breakdowns live in tn_letter_grade_metric. Coverage starts SY 2022-23 (first published year). ~10% of rows have lg_ineligible=true (alt schools, preschools, career-tech centers, online schools) — these have NULL scores and NULL weights.

Relationship to tn_letter_grade_metric: ach_score and ccr_score also appear in the metric table as all-overall rows (achievement,all,all,all,all) and (ccr,all,all,all,all). growth_score, the four weights, and the four component letter grades have NO breakdown counterpart in the source A-F file, so they live ONLY in this wide table.';
COMMENT ON COLUMN tn_letter_grade.year IS
  'School-year end year (SY 2022-23 → 2023). Convention shared across all TN fact tables.';
COMMENT ON COLUMN tn_letter_grade.school_pool IS
  'TDOE classification: HS (high school, 9-12) or K8 (everything else). Drives whether CCR component applies (HS only).';
COMMENT ON COLUMN tn_letter_grade.lg_ineligible IS
  'TRUE when school is not eligible for a letter grade (typically alt schools, preschools, career-tech, online, adult ed). When TRUE, expect all score/grade/weight columns to be NULL. TDOE does not expose the specific reason in source data. See validate.py rule `validate_eligibility_score_consistency` which asserts no row violates "lg_ineligible=false AND all four component scores NULL" (historical artifact: 3 rows in 2022-23, 3 in 2023-24, 0 in 2024-25 — trending toward zero, treated as a regression catcher).';
COMMENT ON COLUMN tn_letter_grade.growth25_score IS
  'Growth score restricted to the bottom-quartile (25th percentile) students. Separate TDOE accountability indicator.';
COMMENT ON COLUMN tn_letter_grade.ccr_score IS
  'College/Career Readiness score. NULL for K8 schools (CCR only applies to HS pool).';


-- ---------------------------------------------------------------------
-- 4. tn_letter_grade_metric — long companion with promoted dimensions
-- ---------------------------------------------------------------------
-- Each breakdown dimension is a first-class NOT NULL column with a
-- value-space CHECK. The sentinel 'all' means "not broken out on this dim".
-- The dimensional-sanity CHECK enforces which dims apply per component.
--
-- Example mappings from source columns:
--   overall_success_rate_all_students  → (achievement, all,   all,    all,  all,  pct)
--   overall_success_rate_ed            → (achievement, ed,    all,    all,  all,  pct)
--   success_rate_g3-5_math             → (achievement, all,   math,   g3_5, all,  pct)
--   growth_numeracy_score              → (growth,      all,   numeracy, all, all, score_1_5)
--   growth_ela_math_score_ed           → (growth,      ed,    ela_math, all, all, score_1_5)
--   ccr_act_rate                       → (ccr,         all,   all,    all,  act,  pct)
--   ccr_rate_ed                        → (ccr,         ed,    all,    all,  all,  pct)

CREATE TABLE tn_letter_grade_metric (
    tdoe_system_id   integer       NOT NULL,
    tdoe_school_id   integer       NOT NULL,
    year             integer       NOT NULL CHECK (year BETWEEN 2023 AND 2100),

    component        text          NOT NULL CHECK (component IN ('achievement', 'growth', 'ccr')),

    subgroup         text          NOT NULL CHECK (subgroup IN (
                                       'all',
                                       'ed',                -- Economically Disadvantaged
                                       'el',                -- English Learner
                                       'swd',               -- Students With Disabilities
                                       'aian',              -- American Indian / Alaska Native
                                       'asian',
                                       'black',
                                       'hispanic',
                                       'nhpi',              -- Native Hawaiian / Pacific Islander
                                       'white',
                                       'bhn',               -- TN-specific: Black + Hispanic + Native American composite
                                       'super_subgroup'     -- TN-specific: ED + EL + SWD + BHN aggregate (all historically underserved)
                                   )),

    subject          text          NOT NULL CHECK (subject IN (
                                       'all',
                                       'ela',
                                       'math',
                                       'science',
                                       'social_studies',
                                       'literacy',          -- TVAAS-style literacy composite (ELA + reading)
                                       'numeracy',          -- TVAAS-style numeracy composite (math)
                                       'ela_math'           -- combined ELA+Math used in growth subgroup breakdowns
                                   )),

    grade_band       text          NOT NULL CHECK (grade_band IN ('all', 'g3_5', 'g6_8', 'g9_12')),

    ccr_component    text          NOT NULL CHECK (ccr_component IN ('all', 'act', 'postsec', 'ic', 'asvab')),

    unit             text          NOT NULL CHECK (unit IN ('pct', 'score_1_5')),

    metric_value     numeric(5,2),
    suppressed       boolean       NOT NULL DEFAULT false,

    source_load_id   bigint        NOT NULL REFERENCES etl_load_log(load_id),

    PRIMARY KEY (tdoe_system_id, tdoe_school_id, year, component, subgroup, subject, grade_band, ccr_component),

    -- Dimensional sanity: which dimensions apply per component.
    -- ELSE false makes the CHECK fail loudly on any unmatched component
    -- (defense in depth — the component IN (...) CHECK should catch it first).
    CONSTRAINT lgm_component_dim_consistency CHECK (
        CASE component
            WHEN 'achievement' THEN ccr_component = 'all'
            WHEN 'growth'      THEN ccr_component = 'all' AND grade_band = 'all'
            WHEN 'ccr'         THEN subject = 'all' AND grade_band = 'all'
            ELSE false
        END
    ),

    -- Unit matches component (defensive denormalization)
    CONSTRAINT lgm_unit_matches_component CHECK (
        (component IN ('achievement', 'ccr') AND unit = 'pct')
        OR (component = 'growth' AND unit = 'score_1_5')
    ),

    -- Suppressed rows have NULL value; non-suppressed rows have non-NULL value
    CONSTRAINT lgm_value_suppression CHECK (
        (suppressed = true  AND metric_value IS NULL)
        OR (suppressed = false AND metric_value IS NOT NULL)
    ),

    -- pct unit: 0-100 range
    CONSTRAINT lgm_pct_range CHECK (
        unit <> 'pct' OR metric_value IS NULL OR (metric_value BETWEEN 0 AND 100)
    ),

    -- score_1_5 unit: 1.0-5.0 range
    CONSTRAINT lgm_score_range CHECK (
        unit <> 'score_1_5' OR metric_value IS NULL OR (metric_value BETWEEN 1.0 AND 5.0)
    )
);

CREATE INDEX idx_lgm_school          ON tn_letter_grade_metric(tdoe_system_id, tdoe_school_id, year);
CREATE INDEX idx_lgm_year            ON tn_letter_grade_metric(year);
CREATE INDEX idx_lgm_subgroup        ON tn_letter_grade_metric(subgroup)      WHERE subgroup      <> 'all';
CREATE INDEX idx_lgm_subject         ON tn_letter_grade_metric(subject)       WHERE subject       <> 'all';
CREATE INDEX idx_lgm_ccr_component   ON tn_letter_grade_metric(ccr_component) WHERE ccr_component <> 'all';
-- No standalone index on `component` (only 3 distinct values, Postgres will seq-scan
-- faster than use such a low-selectivity index). Common access patterns hit the
-- partial indexes above, all of which include `component` implicitly via the PK.

COMMENT ON TABLE tn_letter_grade_metric IS
  'Long-form companion to tn_letter_grade. One row per (school, year, component, subgroup, subject, grade_band, ccr_component). Each breakdown dimension is a first-class NOT NULL column with ''all'' sentinel for "not broken out on this dim". Dimensional-sanity CHECK enforces which dims apply to which component. Generalizes cleanly to GA/CA via a future state_letter_grade_metric view.

Source-of-truth convention: this table contains every breakdown row published in the A-F source file, INCLUDING the all-overall (all,all,all,all) rows where they appear in source (overall_success_rate_all_students → achievement all-overall row; ccr_rate → ccr all-overall row). For metrics that have no breakdown counterpart in source (growth_score, the four weights, the four component letter grades), tn_letter_grade is the only place that data lives. So: metric table is source of truth for anything dimensionally breakable; wide table is source of truth for headline-only fields PLUS denormalized convenience copies of ach_score and ccr_score that also exist in metric.';
COMMENT ON COLUMN tn_letter_grade_metric.year IS
  'School-year end year (SY 2022-23 → 2023). Convention shared across all TN fact tables.';
COMMENT ON COLUMN tn_letter_grade_metric.subgroup IS
  'Demographic subgroup. ''all'' = not broken out. ED/EL/SWD/AIAN/Asian/Black/Hispanic/NHPI/White are federal categories. ''bhn'' (TN-specific) = Black + Hispanic + Native American historically-underserved composite. ''super_subgroup'' (TN-specific) = ED + EL + SWD + BHN aggregate of all historically-underserved students. As of the 2024-25 file, bhn and super_subgroup appear only in growth indicators — not enforced by CHECK because TDOE could extend their use in future files.';
COMMENT ON COLUMN tn_letter_grade_metric.subject IS
  'Subject area. ''all'' = not broken out. ELA/Math/Science/Social Studies are standard. ''literacy''/''numeracy'' are TVAAS-style composites used in growth indicators (literacy ≈ ELA+reading; numeracy ≈ math). ''ela_math'' is the combined ELA+Math measure TN uses for growth subgroup breakdowns — as of the 2024-25 file, primarily appears with component=growth, but not CHECK-enforced.';
COMMENT ON COLUMN tn_letter_grade_metric.grade_band IS
  'Grade band: g3_5 (3rd-5th), g6_8 (6th-8th), g9_12 (9th-12th), or ''all''. Underscored (not g3-5) to keep SQL identifier-friendly. Only achievement component uses grade bands.';
COMMENT ON COLUMN tn_letter_grade_metric.ccr_component IS
  'CCR sub-component: act (ACT exam), postsec (post-secondary enrollment), ic (industry certification), asvab (military), or ''all'' (overall CCR rate). Only ccr component uses this dimension.';
COMMENT ON COLUMN tn_letter_grade_metric.unit IS
  'Unit of metric_value. ''pct'' (0-100) for achievement and ccr; ''score_1_5'' (1.0-5.0) for growth. Determined by component but stored explicitly so an analyst reading a row sees the unit without remembering the convention.

WARNING: ''pct'' values are not directly comparable across components. An achievement ''pct'' is a student-proficiency success rate; a ccr ''pct'' is a CCR-attainment rate. They share units but measure different things. Always filter by component before aggregating or comparing.';
COMMENT ON COLUMN tn_letter_grade_metric.suppressed IS
  'TRUE when source value was "Insufficient N Count" (subgroup N too small for publication under FERPA / TDOE rules). Distinguishes "missing because too few students" from "missing because not collected".';


-- =====================================================================
-- End of schema. Sibling file tn_school_crosswalk_review.sql defines
-- tn_school_crosswalk (split per "crosswalk = standalone pipeline" rule).
-- =====================================================================
