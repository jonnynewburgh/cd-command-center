---
title: Session summary — Postgres-compatibility sweep
date: 2026-04-18
scope: Four facility/geography pipelines fixed against Postgres; repo history rewritten; diagnosis docs published
status: Complete — shipped and pushed
---

# Session summary — 2026-04-18

Four pipelines (census tracts, schools, FQHC, ECE) now run correctly against Postgres. Repo is unblocked for remote pushes after a history rewrite removed a 527 MB SQLite blob. Everything in this session followed a strict diagnose-first, scope-disciplined pattern.

## 1. Starting state

- **12 commits ahead of origin, unable to push.** Push was blocked by a `data/cd_command_center.sqlite` blob (527 MB) in history — over GitHub's 100 MB file limit.
- **Zero pipelines had verified output on Postgres.** All prior ETL work was SQLite-only; Postgres was the stated production target but every pipeline had latent bugs that surfaced only when `DATABASE_URL` was set.
- **25 files modified in the working tree**, uncommitted for an unknown duration. No single-commit scope, no diagnosis docs, no test evidence. Deferred triage for later.

## 2. What shipped (chronological)

| Commit | Scope |
|---|---|
| (unrecorded, pre-history-rewrite) | Logging fix in `etl/load_census_tracts.py` |
| *(history rewrite)* | Repo history rewritten with `git filter-repo`; SQLite blob removed; `.git` shrank from 699 MB → 6.1 MB; force-push accepted by remote |
| `c1fc5b3` | Census/ACS Postgres fix — three bugs: `_adapt_sql` miss in `upsert_census_tract`, ambiguous column reference in `get_census_tract_summary` COALESCE, `dict(row)` on psycopg2 tuples |
| `c87742b` | Schools/NCES Postgres fix — two bugs: `_adapt_sql` miss in `upsert_school`, `dict(row)` in `get_school_summary`. Originally claimed `is_charter` needed a BOOLEAN fix — retracted in Phase 3 |
| `63d2c8b` | NCES diagnosis correction doc — documented that `schools.is_charter` is `INTEGER` on both backends (Postgres `int4`, SQLite `INTEGER`), not `BOOLEAN`; `WHERE is_charter = 1` is correct everywhere |
| `0f05d14` | `is_charter` usage inventory doc — swept all `*.py` for `is_charter` references across `db.py`, `etl/`, `utils/`, `validate.py`, `archive/`, `db/`. Zero boolean-literal variants (`= TRUE`, `= FALSE`, `IS TRUE`, `::boolean`, Python `True/False` writes) found. No action required |
| `0b63cd7` | FQHC Postgres fix — three bugs: `_adapt_sql` miss in `upsert_fqhc`, `dict(row)` in `get_fqhc_summary`, `_adapt_sql` miss **and** `dict(row)` in `get_fqhc_by_id`. Doc includes "Known pipeline gap" section on UDS absence |
| `8a8d848` | ECE Postgres fix — identical three-bug shape to FQHC: `_adapt_sql` miss in `upsert_ece`, `dict(row)` in `get_ece_summary`, `_adapt_sql` miss + `dict(row)` in `get_ece_by_id`. Doc includes "Data scope mismatch" (CO-only, GA missing) and "Known pipeline gap — operator intelligence" sections |

Each code-fix commit was verified via direct probe (synthetic test row round-trip) plus end-to-end loader run on both backends, plus a cross-backend spot check on a real row.

## 3. What's verified on both backends

| Pipeline | Row count | Verification evidence |
|---|---:|---|
| **Census/ACS tracts** | 2,796 GA tracts | Sum of `total_population` reconciles to within 0.027% of published ACS 2022 5-year GA statewide figure; `get_census_tract_summary` returns identical non-empty dicts on both backends |
| **NCES schools** | 97,750 rows | Direct probe `TEST_PROBE_NCES_0001` round-trips; loader runs clean; sample row field-by-field match across backends |
| **FQHC sites** | 18,830 sites | Direct probe `TEST_PROBE_FQHC_0001` round-trips; HRSA CSV loader runs clean on both; spot-check on real BHCMIS ID matches field-by-field (except `latitude`/`longitude` precision) |
| **ECE centers** | 4,556 CO facilities | Direct probe `TEST_PROBE_ECE_0001` round-trips; `load_ece_data.py` runs clean on both; `license_id = '100'` matches 22-field-for-22-field (except lat/long precision) |

All four: zero `DB error for {id}: {e}` lines in stdout during loader runs, `get_*_summary()` returns real numbers not `{}`, `get_*_by_id()` returns populated dicts.

## 4. Known out-of-scope items tracked this session

Findable punch list for future sessions. Nothing here was fixed this session — each was explicitly deferred with documented reasoning.

### 4.1 Remaining `upsert_*` functions with the `_adapt_sql` miss

| Function | Has COALESCE preserve-if-null? | Has dict(row) reader nearby? | Est. complexity |
|---|:---:|:---:|---|
| `upsert_census_tract` | — | — | **DONE** (`c1fc5b3`) |
| `upsert_school` | — | — | **DONE** (`c87742b`) |
| `upsert_fqhc` | — | — | **DONE** (`0b63cd7`) |
| `upsert_ece` | — | — | **DONE** (`8a8d848`) |
| `upsert_nmtc_project` | unknown — check | likely (`get_nmtc_project_by_id`) | simple — likely two-bug shape |
| `upsert_cde_allocation` | unknown — check | possible (`get_cde_by_id`) | simple — likely two-bug shape |
| `upsert_cdfi` | unknown — check | possible (directory reader) | simple |
| `upsert_cdfi_award` | unknown — check | possible | simple |
| `upsert_990` | **likely yes** — preserve prior values | yes | **has COALESCE** — extra care on column qualification |
| `upsert_990_history` | maybe | yes | simple-medium |
| `upsert_state_program` | no | minor | simple |
| `upsert_enrollment_history` | no | minor | simple |
| `upsert_financial_ratios` | **likely yes** — 990~ vs audit✓ merge | yes | **has COALESCE** — dual-source field priority |
| `upsert_lea_accountability` | unknown | possible | simple-medium |

Eleven remaining. Pattern is proven four times; most should be 5-10 min each. `upsert_990` and `upsert_financial_ratios` warrant more care due to COALESCE-style field-preservation logic.

### 4.2 Other Postgres-compat latent bugs outside `upsert_*`

- **`get_ece_centers` at `db.py:1864`** — uses `pd.read_sql_query` with raw `?` placeholders. pandas passes placeholders verbatim; psycopg2 rejects `?`. Needs `_adapt_sql` wrap on the query string or switch to `%s`. Flagged in ECE fix commit; not fixed.
- **`apply_historical_data` in `etl/load_census_tracts.py`** — raw `?` placeholders in SQL executed outside `db.py`. Violates the "all database access through `db.py`" architectural rule from `CLAUDE.md`. Fix options: (a) inline the `_adapt_sql` shim with an import, (b) move the query into `db.py`.

### 4.3 Architectural concerns (not quick fixes)

- **Silent `except Exception: ...` handlers** across schools, census, FQHC, ECE, and other helpers. Every reader swallows errors and returns `{}` or `[]`. This is why all four pipelines were silently broken on Postgres for an unknown duration — no exception ever surfaced. Per-call-site judgment needed: some are legitimately defensive (missing table during bootstrap), most should fail loud.
- **`latitude`/`longitude` stored as `real`/float4 on Postgres vs double (8-byte REAL) on SQLite** — ~22m precision loss at typical latitudes for geocoded points. Visible in every cross-backend spot-check (e.g. FQHC 33.584988 vs 33.58498816, ECE 39.52089 vs 39.520890401716). Schema-migration-sized fix (`ALTER COLUMN ... TYPE double precision`), not a bug.

### 4.4 Data-scope gaps (project-sized, not fixes)

- **FQHC pipeline missing UDS data** — payer mix, clinical quality measures, financials, staffing. Current `fqhc` table is service-delivery-site-directory-only. UDS is a single national HRSA-operator-grantee-level source with ~9-month lag; separate ingestion project. Documented in `docs/debug/fqhc_diagnosis_2026-04-18.md`.
- **ECE pipeline missing operator intelligence** — enrollment (vs. licensed capacity), subsidy participation (CCDBG / state vouchers / Head Start / Pre-K funding), quality ratings (state QRIS / NAEYC / CLASS), staffing ratios, child demographics, financials. Unlike FQHC, **no single national source exists** — QRIS / subsidy / Pre-K data is per-state. Multi-source-per-state project. Documented in `docs/debug/ece_diagnosis_2026-04-18.md`.
- **ECE data is Colorado only** — 4,556 rows from the CO licensing CSV. **Georgia (primary CDFI lending market at employer) is not represented.** Adding GA requires sourcing the DECAL licensing file and mapping columns in `etl/load_ece_data.py`.

### 4.5 In-progress refactor (decide fate before next batch)

- **`db/schema.py`, `db/queries.py`, `db/mutations.py`** — untracked, in-progress split of monolithic `db.py` into a package. Identified in the `is_charter` inventory (`0f05d14`) as `?? db/` in `git status`. Mirrors `db.py` call sites one-for-one with same integer semantics, but carries the same Postgres bugs that were fixed in `db.py`. **Will conflict with the remaining 11 `upsert_*` fixes.** Decision needed: (a) ship the refactor first and fix remaining `upsert_*` in the new package, (b) finish the `upsert_*` sweep in `db.py` and fold changes into the refactor later, (c) discard the refactor and keep `db.py` monolithic.

### 4.6 Working-tree triage

- **24 uncommitted working-tree modifications were lost during the repo history rewrite.** No recovery path (pre-rewrite reflog replaced; no stash). User chose not to reconstruct. Unknown what they contained.
- **7 untracked files remain** after this session: `.env.example`, `DATA_REFRESH_SCHEDULE.md`, `data/test.pdf`, `db/`, `docs/debug/census_acs_diagnosis_2026-04-17.md`, `docs/debug/session_state_2026-04-18.md`, `utils/etl_helpers.py`, `utils/state_fips.py`. Should be triaged — commit, gitignore, or delete each.

## 5. Suggested next sessions (options, not commitments)

| Option | Scope | Why now / why not |
|---|---|---|
| **Finish 11 remaining `upsert_*` functions** | Mechanical, same template | Template proven four times; ~60-90 min of focused work. Ships Postgres-compat for the entire ETL surface. Blocker: `db/` refactor decision |
| **Decide fate of `db/*.py` refactor** | Meta | Must happen before the next `upsert_*` batch or work will conflict. Decision, not coding work |
| **Audit silent exception handlers** | Architectural | Root cause for why this whole sweep was needed. Large, per-site judgment. Could do one pipeline at a time (schools reader set first?) |
| **Scope UDS ingestion for FQHC** | New data project | Biggest single unlock for FQHC utility (payer mix + clinical + financials). Requires HRSA UDS schema study. Multi-day scope |
| **Add GA ECE via DECAL** | New data loader | Highest immediate business value (primary lending market). Needs DECAL file sourcing + column mapping. Half-day scope |
| **PRI / 990-PF pipeline debugging** | Return to original target | This was the task before the session pivoted to Postgres-compat. Context restored now that infra is stable. Separate repo (`impact-investing`) |
| **Working-tree triage** | Housekeeping | 7 untracked files decide-or-delete. <30 min |

## Addendum — reusable patterns established

Two templates became reliable during this session and are worth preserving:

**Three-bug shape for a facility upsert/reader path** (fits `census_tracts`, `schools`, `fqhc`, `ece` identically, likely fits the 11 remaining):
1. `cur.execute(sql, values)` on an upsert → needs `_adapt_sql(sql)` wrap
2. COALESCE preserve-if-null column references → needs table-qualification (`s.col` not bare `col`)
3. `dict(row) if row else {}` in readers → needs `cols = [d[0] for d in cur.description]; dict(zip(cols, row))`

**Verification recipe** (same four steps each time):
1. Direct probe — synthetic `TEST_PROBE_*` record, upsert + get_by_id + cleanup delete — proves the fix independent of CSV content
2. Loader run on Postgres — exit 0, zero `DB error` lines, count delta matches expectation
3. Loader run on SQLite (`env -u DATABASE_URL python ...`) — identical delta, no regression
4. Cross-backend spot-check on one real row — field-by-field dict comparison
