# TN Charter Accountability Pipeline — Retrospective

Source files: TDOE TVAAS (2015-2025) + TDOE A-F Letter Grade (2022-23, 2023-24, 2024-25).
Destination: four fact tables under the `tn_*` namespace, with a shared `etl_load_log` provenance table.

Built end-of-session 2026-05-17 through 2026-05-18.

---

## Architecture decisions

### Hybrid wide+long for A-F letter grade data
A-F file has ~78 columns: a small headline set (overall + four component scores/grades/weights) plus ~50 breakdown columns covering subgroup, subject, grade-band, and CCR sub-component dimensions.

- **Headline data** → `tn_letter_grade` (wide, ~20 cols). One row per school per year. Includes the fields that have no breakdown counterpart in source: `growth_score`, the four weights, the four component letter grades.
- **Breakdown data** → `tn_letter_grade_metric` (long, 8-col key + value). Each breakdown dimension is a first-class NOT NULL column with `'all'` as the sentinel for "not broken out on this dim".

**Why not all-wide:** 78 columns would break every year TDOE adds a column. And the schema couldn't generalize cross-state — GA/CA have entirely different column shapes.

**Why not all-long:** headline queries become painful joins. And growth_score / weights / component grades have no breakdown counterpart in source, so they'd need synthetic "all-overall" rows for purely structural reasons.

**Source-of-truth rule:** the metric table contains every breakdown row published in the A-F source file, INCLUDING the all-overall (`all,all,all,all`) rows where source has them (`overall_success_rate_all_students` → achievement all-overall row; `ccr_rate` → ccr all-overall row). The wide table holds the headline-only fields plus denormalized convenience copies of `ach_score` and `ccr_score`.

### Promoted-dimension structure on the metric table
First attempt used `(breakdown_dim, breakdown_value)` as a two-column pair. The cross-tab check revealed `growth_ela_math_score_{subgroup}` columns — a genuine 2-D breakdown (subject × subgroup) that can't be expressed cleanly in a single dim-value pair.

Final shape: each breakdown is a first-class NOT NULL column with `'all'` sentinel. CHECK constraints enforce both the per-column value vocabulary and the dimensional sanity per component (`achievement` never uses `ccr_component`; `growth` never uses `grade_band`; `ccr` never uses `subject` or `grade_band`).

### Explicit ID column names
Used `tdoe_system_id` / `tdoe_school_id` throughout, never `system` / `school`. Reasons: `system` is SQL-keyword-adjacent and would need quoting; explicit prefixes make cross-state queries readable (e.g. future `state_school_crosswalk` view UNION-ing TN/GA/CA crosswalks).

### Crosswalk as standalone pipeline
`tn_school_crosswalk` (TDOE system+school ID → NCES 12-digit ncessch) lives in its own migration, joined softly from fact tables (no FK). Reason: a school can appear in A-F or TVAAS before the crosswalk catches up; a hard FK would block valid loads. Pattern repeats for GA/CA.

### End-only logging with optimistic-success log row
The `etl_load_log` row is INSERTed with `status='success'` at the top of the load transaction. If validators fail or any INSERT errors, the entire transaction rolls back — log row vanishes with the fact rows. Failed attempts are recorded in a separate transaction after the rollback. Disk fallback (`logs/failed_loads_emergency.log`) catches DB-down scenarios.

No `pending` or `in_progress` state in the CHECK enum — keeps analytics queries clean.

### Severity on validators
`ValidationFailure.severity: Literal['error', 'warning']`. Errors raise `ValidationError` → ROLLBACK. Warnings get logged + threaded into the load row's `notes` column but don't abort the COMMIT. Necessary for the eligibility-anomaly validator (known historical artifact that shouldn't block loads but should be tracked).

### Filename parser as a single dispatch layer
`etl/tn_filename_parser.py` is the only place that maps filenames → `(year, file_type)`. Loaders never parse filenames themselves. Six pattern families cover 39 actual files. Failing CI fixture-consistency test surfaces if a new TDOE filename pattern lands in raw/ without parser support.

---

## TN-specific quirks discovered

### BHN and super_subgroup (TN-specific subgroups)
Source files include subgroup values not in federal categories:
- `bhn` = Black + Hispanic + Native American composite (TN's "historically underserved" racial aggregate)
- `super_subgroup` = ED + EL + SWD + BHN aggregate (all historically-underserved students)

As of the 2024-25 file, both appear only in growth indicators (specifically `growth_ela_math_score_{bhn,super_subgroup}`). Not CHECK-enforced because TDOE could extend their use in future files.

### `ela_math` combined subject
TN reports growth subgroup breakdowns under a combined ELA+Math measure: `growth_ela_math_score_{subgroup}`. The 11 subgroup columns share `subject='ela_math'` in the metric table. This is the only place the `ela_math` subject value appears.

### 195 A-F-only schools (~10% of TN school universe)
TVAAS files have 1,728 schools (2024); A-F files have 1,905. The 195 A-F-only schools are NOT CCR-only HSs as initially hypothesized — they're:
- 188 K8, 7 HS
- Zero have any numeric scores in ach/growth/growth25/ccr
- 169 flagged `lg_ineligible=true`
- 26 claim eligible but have all-sentinel scores (overlap with eligibility anomaly)

These are alt schools, preschools, head starts, career-tech centers, online schools, adult HS — real schools in TDOE's SIS that don't receive TVAAS testing. Outer-join behavior matters in any downstream view that combines TVAAS and A-F.

### Eligibility anomaly (6 lifetime rows, trending to zero)
Schools published with `lg_ineligible=false` but all four component scores AND all four weights NULL:
- 2022-23: 3 rows (Crestview Middle 840/13, Crestview Elementary 840/17, Martin Elementary 920/35)
- 2023-24: 3 rows (same schools)
- 2024-25: 0 rows (TDOE fixed these)

Schema CHECK `lg_weights_sum` admits a third branch for this state (all-NULL weights + all-NULL scores). Validator `validate_eligibility_score_consistency` catches these as WARNING per load. The trio in 22-23 and 23-24 producing the same warnings on both loads suggests TDOE's bug is per-school, not random.

### EOC / ACT NULL-grade coalescing
TVAAS subject-level source data has NULL Grade for:
- ACT rows (test reports a single composite, not per-grade)
- EOC rows (End-Of-Course exams tied to courses like Algebra I, not grades)

Schema PK requires `grade NOT NULL`. Coalesce at load time:
- ACT → `grade='Composite'`
- EOC → `grade='Course'`

`'EOC'` as a grade value would collide with `test='EOC'` (`WHERE grade='EOC'` becomes semantically identical to `WHERE test='EOC'`). Use semantically distinct sentinels.

Validator `validate_grade_test_consistency` enforces the convention post-INSERT — catches future coalesce-logic bugs.

### `(district, school) ≡ (system, school)`
TVAAS columns `District Number, School Number` and A-F columns `system, school` are the same ID space. Verified by sample join: 1,710 schools overlap, 18 TVAAS-only, 195 A-F-only, names match exactly on the merged sample. Unified storage column names: `tdoe_system_id`, `tdoe_school_id`.

### Privacy buckets `<5%` and `>95%`
TDOE suppresses exact rates below 5% or above 95% by replacing with the literal strings `'<5%'` / `'>95%'`. Currently collapsed to `suppressed=true, metric_value=NULL` in `tn_letter_grade_metric`. Loses directional info (low vs high bucket). If this matters analytically later, add a `suppression_bucket text` column to `tn_letter_grade_metric` and stop collapsing.

### Weight-pattern variation per pool and year
Schema CHECK originally required all four weights NOT NULL + sum=1.0 when eligible. Observed reality (across all 3 A-F years):

| Pattern | Pool | Count/yr | Handling |
|---|---|---|---|
| `(0.5, 0.3, 0.1, 0.1)` | HS | ~350 | Full pattern, passes original CHECK |
| `(0.5, 0.4, NaN, 0.1)` | HS | ~7 | Loader coerces growth25 to 0.0 |
| `(0.5, 0.4, 0.1, NaN)` | K8 | ~1293 | Loader coerces ccr to 0.0 |
| `(0.5, 0.5, NaN, NaN)` | K8 | ~42 | Loader coerces both to 0.0 |
| `(NaN, NaN, NaN, NaN)` | either | ~3 in 22-23/23-24, 0 in 24-25 | Eligibility anomaly — schema CHECK relaxed via migration f5 |

Loader rule: for eligible rows with at least one non-NaN weight, coerce missing weights to 0.0. For all-NaN eligible rows (the anomaly), leave NULL and let the relaxed CHECK admit them.

### Sentinel additions surfaced by loud-failure design
- A-F: `<5%` / `>95%` — privacy buckets (added during build)
- TVAAS: `*` / `**` / `N/A` — small-N suppression markers (added during canonical reload 2026-05-18; observed in 2019 subject-level file's `Growth Measure`/`Standard Error`/`Index`)
- TVAAS: `Grade 2` Test value — only in the 2017 subject-level file (K-2 assessment reported separately); CHECK constraint relaxed via migration `f6c7d8e9f0a1`

The loader raises `ValueError("unrecognized A-F cell value: ...")` on any unknown A-F text — preserves the property that TDOE-added sentinels surface as loader bugs, not silent data loss. TVAAS uses the shared `_is_suppressed` helper in `etl/state_accountability/tn/handlers.py`.

---

## Year semantics

All `year` columns across all four fact tables use the **end year of the school year**:
- SY 2022-23 → year=2023
- SY 2020-21 → year=2021
- Calendar year files (e.g. `2024_tvaas_*`) → year=2024 (TDOE's convention matches)

Documented on every `year` column COMMENT. Filename parser resolves to end-year for cross-year files (`2020-21_*` → 2021, `2022-23_A-F_*` → 2023).

Years actually present in raw/:
- TVAAS: 2015, 2016, 2017, 2018, 2019, 2021 (= SY 2020-21), 2022, 2023, 2024, 2025
- A-F: 2023, 2024, 2025 (program started SY 2022-23)
- Missing: 2019 TVAAS (never published) — handled as a gap, not an anomaly
- Missing: 2020 TVAAS — COVID year, no statewide testing

---

## `_quarantine/` cleanup protocol

During raw-dir cleanup before ETL development, files moved to `data/raw/charter accountability/TN/_quarantine/`:

- 4 unlabeled CSV originals (renamed copies as `2019_tvaas_*.csv` live in raw/; quarantine holds the originals for rollback)
- 2 byte-identical `(1)` duplicates of 2022 TVAAS district files

**Delete after: 2026-06-16** (per the `CLEANUP_LOG.md` 30-day window). By that date, the canonical wipe + reload should be done, confirming we don't need the quarantine for any reason.

Protocol is in the CLEANUP_LOG.md itself. The `_quarantine` dir is gitignored (under `data/raw/*`).

---

## What's not yet done

1. Optional: `suppression_bucket` column on `tn_letter_grade_metric` if directional `<5%`/`>95%` info becomes valuable
2. Optional: integration test harness against a real test database
3. Refresh `tn_school_crosswalk` when CCD 2023-24 / 2024-25 directory files are published (will lift coverage from 94.2% → ~99% of TVAAS universe; virtual + newly-opened schools currently unmatched)

## What got done (2026-05-18)

- **Canonical reload.** All 39 TN files loaded; `load_id` matches canonical position 1:1.
- **Loader fixes during reload:** `*`/`**`/`N/A` suppression marker handling (`handlers.py:_is_suppressed`), `Grade 2` Test value (migration `f6c7d8e9f0a1`).
- **2016 anomaly investigated:** 631-row school-composite count is real source data — spring 2016 TNReady cancellation context.
- **`tn_school_crosswalk` ETL built and run.** Source: NCES CCD school directory files (`ccd_sch_129_*`, 4 years 2019-20 through 2022-23). Loader: `etl/load_tn_school_crosswalk.py`. Coverage: 1,987 validity bands, 94.2% of TVAAS / 97.3% of A-F schools matched. Idempotent with `--truncate`.

See `etl/canonical_load_order.md` for the verified `load_id ↔ filename ↔ row count` mapping.
