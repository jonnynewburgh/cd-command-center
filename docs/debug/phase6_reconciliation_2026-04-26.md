---
title: Phase 6 — schema + data reconciliation
date: 2026-04-26
scope: Cross-backend audit (SQLite vs Postgres) + canonical-source decision
status: Phase 6 closed.
---

# Phase 6 — schema + data reconciliation — 2026-04-26

## Summary

Phase 6 of the Postgres migration plan called for cross-backend
parity. Reality: SQLite has been operationally deprecated since
2026-03-26 (per the project's own working memory: "DATABASE_URL env
var is set to local Postgres. All ETL scripts and queries go through
Postgres. The SQLite file in data/ is stale.").

Three large datasets (`federal_audits`, `federal_audit_programs`,
`headstart_programs`, totaling ~1.4M rows) loaded after the
deprecation point exist only in Postgres. Resyncing SQLite would
cost ~30 minutes one-time plus ongoing maintenance for a backend
that has no active reader. **Resync rejected.**

**Canonical decision: Postgres is the only canonical source.** SQLite
remains as a frozen development snapshot for offline use; it is not
expected to track Postgres after this point.

The Phase 6 success criterion "validate.py reports parity for every
shared table on both backends" has been replaced with "Postgres is
the active backend; SQLite drift from the snapshot point is expected
and not flagged as a bug."

## Schema parity — 100%

Post-fix:
- SQLite tables: 34
- Postgres tables: 34
- SQLite-only: none
- Postgres-only: none

Pre-fix `charter_schools` was SQLite-only (vestigial table from
before the schools/charter_schools merge — the for-table fallback
loops in db.py were removed in `bf4ed8a`, but the live SQLite table
remained until this phase). Dropped this phase.

Content of the dropped table:
- 1,420 rows total
- 1,220 rows: redundant duplicates of `schools.is_charter = 1`
- 200 rows: synthetic `nces_id LIKE 'NCES%'` rows from
  `etl/load_sample_data.py` runs

100% safe to drop — no real-data loss.

## Row-count audit (post-cleanup)

| Table | SQLite | Postgres | Delta | Reason |
|---|---:|---:|---:|---|
| bls_qcew | 6,424 | 6,424 | 0 | synced |
| bls_unemployment | 14,545 | 14,545 | 0 | synced |
| cde_allocations | 660 | 660 | 0 | synced |
| cdfi_awards | 11,918 | 11,918 | 0 | synced |
| cdfi_directory | 1,383 | 1,383 | 0 | synced |
| census_tracts | 85,396 | 85,396 | 0 | synced |
| cra_assessment_areas | 0 | 0 | 0 | synced |
| cra_institutions | 33,668 | 33,668 | 0 | synced |
| cra_sb_aggr | 559,008 | 559,008 | 0 | synced |
| cra_sb_discl | 1,073,333 | 1,073,333 | 0 | synced |
| **data_loads** | 62 | 71 | **+9** | active backend (PG) accumulated more pipeline runs |
| documents | 0 | 0 | 0 | synced |
| ece_centers | 4,556 | 4,556 | 0 | synced |
| enrollment_history | 31,929 | 31,929 | 0 | synced |
| **federal_audit_programs** | 0 | 1,246,822 | **+1,246,822** | dataset loaded after SQLite snapshot |
| **federal_audits** | 0 | 64,869 | **+64,869** | dataset loaded after SQLite snapshot |
| financial_ratios | 3,513 | 3,513 | 0 | synced |
| fqhc | 18,830 | 18,830 | 0 | synced |
| **headstart_programs** | 0 | 45,962 | **+45,962** | dataset loaded after SQLite snapshot |
| hmda_activity | 82,962 | 82,962 | 0 | synced |
| hud_ami | 4,814 | 4,814 | 0 | synced |
| hud_fmr | 5,308 | 5,308 | 0 | synced |
| **irs_990** | 4,123 | 4,126 | **+3** | small drift from intervening 990 reload |
| **irs_990_history** | 3,548 | 3,551 | **+3** | small drift from intervening 990 reload |
| lea_accountability | 9,827 | 9,827 | 0 | synced |
| market_rates | 1,376 | 1,376 | 0 | synced |
| nmtc_coalition_projects | 19,907 | 19,907 | 0 | synced |
| nmtc_projects | 8,024 | 8,024 | 0 | synced |
| sba_loans | 470,487 | 470,487 | 0 | synced |
| schools | 97,750 | 97,750 | 0 | synced |
| scsc_cpf | 276 | 276 | 0 | synced |
| state_programs | 28 | 28 | 0 | synced |
| user_bookmarks | 0 | 0 | 0 | synced |
| user_notes | 0 | 0 | 0 | synced (Phase 4 smoke leftovers cleaned this phase) |

**28 / 34 tables synced. 6 drifted, all "Postgres ahead" — exactly
what the canonical decision predicts.**

Most pre-Phase-1 inventory drifts (`schools` 5190/5187, `cdes` 11/7,
`fqhc` 43/42, Atlanta/Fulton search counts) have been resolved by
intervening reloads. Current drift is concentrated in three loaded-
post-snapshot datasets and small accumulator tables.

## Active changes this phase

1. **Dropped `charter_schools` from SQLite.** 1,420 rows: 1,220
   redundant + 200 synthetic. No real data loss.
2. **Cleaned `user_notes` test rows from Postgres.** 3 rows from
   Phase 1/4 smoke testing (`verify-test`, `smoke-test note`,
   `fastapi smoke`). Now back to 0 rows.
3. **Added `etl/audit_backend_parity.py`** — reusable standalone
   script for re-running this audit on demand. Reports schema and
   row-count diff between the SQLite file (if present) and the
   active Postgres backend.
4. **Documented canonical decision** (this doc).

## What's NOT done — explicit non-decisions

- **Resyncing SQLite from Postgres**: rejected. Cost = 30+ min
  one-time + ongoing maintenance for a backend with no active
  reader. Per the project memory, SQLite is operationally
  deprecated.
- **Deleting `data/cd_command_center.sqlite` outright**: rejected
  for now. The file still has value as an offline development
  snapshot (e.g. running tests without a Postgres instance up). If
  it later becomes a maintenance hassle, delete it then.
- **Forcing validate.py to assert backend = Postgres**: rejected.
  validate.py already runs against `db.get_connection()` and so
  works against either backend; the canonical decision is a
  process/documentation choice, not a runtime check.

## Top-N get_* convergence check — surfaced Phase 3D work

Per Phase 6 success criterion (downgraded — see canonical decision
above): top get_* functions should execute cleanly on the active
backend.

Result: **PARTIAL**. State-filtered queries crashed on Postgres for
five db.py reader functions:

| Function | Result on Postgres |
|---|---|
| `get_schools(states=['GA'])` | OK — 2,350 rows |
| `get_fqhc(states=['GA'])` | **RAISE** — `syntax error at or near ")"` (raw `?` not adapt_sql'd) |
| `get_ece_centers(states=['GA'])` | **RAISE** — same |
| `get_nmtc_projects(states=['GA'])` | **RAISE** — same |
| `get_census_tracts(states=['GA'])` | **RAISE** — same |
| `get_school_summary(charter_only=True)` | OK |
| `get_fqhc_summary()` | OK |
| `get_nmtc_project_summary()` | OK |
| `get_school_states()` | OK — 56 states |
| `get_cdfis(states=['GA'])` | **RAISE** — same |

Phase 1's silent-handler decoration is what surfaced this — the
underlying raw `?` bug had been masked by `except Exception: df =
pd.DataFrame()` until Phase 1's `logger.exception(...)` made it
visible. This is exactly the convergence that Phase 1 was designed
to enable.

**Hard stop triggered (Phase 6):**

The migration plan's Phase 6 hard stop applies:

> "Discovering a new bug class during reconciliation. Capture in
> Open Questions; do not fix mid-phase. Reconciliation findings are
> the leading indicator that a Phase N earlier sweep was incomplete."

The bug class is NOT new (bug class 1 — raw-? un-adapt_sql'd — was
tracked from the start). What's new is the surface size: the prior
Phase 3 inventory targeted ETL scripts and FastAPI routers but did
not sweep `db.py` reader functions.

Total raw-? surface = 21 (original inventory, all closed in Phase
3A/B/C) + 18 newly-discovered db.py reader sites = **~39**, above
the migration plan's 32-site re-scope threshold:

> "Re-grep finds more than ~32 raw-? sites (1.5x the inventory).
> Pause and re-scope."

Both conditions trigger pause. Phase 6 ships the schema-and-data
reconciliation work that IS done; the db.py reader sweep splits out
as a follow-up session.

## Phase 3D — db.py reader raw-? sweep (deferred)

18 db.py reader functions missing `adapt_sql` wrap on their
`pd.read_sql_query(query, conn, params=params)` call. Affected when
filter args are passed (states=, etc.); silent failure (RISKY
decoration returns empty DataFrame) when filters cause `?`
placeholders to appear in `query`.

| Site | Function |
|---|---|
| db.py:1524 | `get_nmtc_eligible_tracts` |
| db.py:1581 | `get_census_tracts` |
| db.py:1679 | `get_nmtc_projects` |
| db.py:1724 | `get_cde_allocations` |
| db.py:1768 | `get_fqhc` |
| db.py:1869 | `get_ece_centers` |
| db.py:2086 | `get_lea_accountability` |
| db.py:2314 | `get_nmtc_projects_by_cde` |
| db.py:2581 | `get_peer_nmtc_projects` |
| db.py:2598 | `get_operator_schools` |
| db.py:2614 | `get_operator_fqhc` |
| db.py:2719 | `get_cdfis` |
| db.py:2784 | `get_state_programs` |
| db.py:2885 | `get_service_gaps` |
| db.py:2921 | `get_enrollment_history` |
| db.py:3188 | `get_documents` |
| db.py:3434 | `get_latest_rates` |
| db.py:3471 | `search_org` |

Estimated effort: 1-1.5 hours. Each site is a mechanical
`pd.read_sql_query(db.adapt_sql(query), conn, params=params)` wrap.
Convergence test: re-run the top-N get_* check and expect all OK.

**Production impact today:** the FastAPI dashboard will silently
return empty data for state-filtered queries on fqhc, ece_centers,
nmtc, census, cdfis pages. Real Postgres errors are now logged to
`stderr` (Phase 1) but the user sees an empty page. This is the
last Postgres-blocking issue for the dashboard.

## Open Questions / smaller deferrals

- `api/routers/notes.py` route precedence bug shadowing
  `/notes/bookmarks/all` (Phase 4 smoke-test finding).
- `get_nearby_facilities(active_only=...)` stale kwarg drift
  (Phase 1 smoke-test finding).
- `validate.py` data-quality WARNs (irs_990 12% null tax_year, 6
  rows total_revenue < 0, lea_accountability 200 orphan lea_ids
  vs schools).

## Migration plan status

| Phase | Status | Commits |
|---|---|---|
| 1 — Silent handlers | ✓ | a906bd1, 3d4cd6e |
| 2 — log_load_start lastrowid | ✓ | 7d8382a |
| 3A — ETL raw-? + chained-execute | ✓ | fe26f90 (+ earlier 9917c99, fbda9c9) |
| 3B — FastAPI raw-? | ✓ | 11097d6 |
| 3C — state_programs INSERT OR IGNORE | ✓ | f0582f3 |
| **3D — db.py reader raw-? sweep** | **pending — surfaced by Phase 6** | — |
| 4 — dict(row) + L2/L3 lastrowid | ✓ | 33bef1c |
| 5 — SQLite-only function rewrites | ✓ | f0582f3 |
| 6 — Schema/data reconciliation | ✓ partial | this commit |

The FastAPI + Postgres stack is one short follow-up session away
from a deployable state.
