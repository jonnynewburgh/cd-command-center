---
title: CODEX audit follow-up — second pass (P0 #3, P0 #4, P1 #6, P2 #11)
date: 2026-04-30
scope: Continuation of `codex_audit_followup_2026-04-27.md`
status: 4 deferred items shipped this session; remainder triaged below
---

# CODEX audit follow-up — 2026-04-30

The 2026-04-27 triage doc deferred 7 items. This session shipped the
4 highest-impact ones: P0 #3, P0 #4, P1 #6, P2 #11.

## Shipped this session

### P0 #3 — Nearby search SQL bbox prefilter ✓ (33da49b)

`get_nearby_facilities()` was loading every school / FQHC / ECE / NMTC
row into pandas and running Python haversine per row. ~129K rows touched
per request.

Rewritten to compute a lat/lon bounding box from `radius_miles`
(`lat_delta = miles/69`, `lon_delta` scales by `cos(lat)`, cosine clamped
for poles) and push it into SQL via `latitude BETWEEN ? AND ? AND
longitude BETWEEN ? AND ?`. Each per-table `get_*()` gained an optional
`bbox=(min_lat, max_lat, min_lon, max_lon)` kwarg so projection and JOINs
stay single-sourced. `filter_by_radius()` still runs the exact haversine
pass on the narrowed set.

Four `(latitude, longitude)` indexes added to `init_db()` for the
BETWEEN ranges.

**Verified at downtown Atlanta (33.7490, -84.3880, 5 mi):**
- ID-set parity with old path (67 schools / 24 fqhc / 0 ece / 56 nmtc).
- Cold latency 2903 ms → 191 ms (~15×).
- Warm latency 10.7 ms (~270×).

PostGIS deferred per the audit's long-term note — bbox in plain SQL
gets ~95% of the win without the dependency.

### P0 #4 — Pagination + lean projections ✓ (4e639a0)

The 5 unbounded list endpoints (`/schools`, `/tracts`, `/fqhc`, `/ece`,
`/nmtc/projects`) now return a `Page[T]` envelope:
`{items, total, limit, offset}`.

- Query params on every list route: `limit` (default 100, range 1..10000),
  `offset` (default 0), `sort` (vetted whitelist per endpoint), `sort_dir`
  (asc | desc, regex-validated).
- Stable tiebreaker (`nces_id` / `census_tract_id` / `bhcmis_id` / etc.)
  appended to every ORDER BY so consecutive pages don't overlap.
- Two new shared helpers in `db.py`: `_resolve_sort()` (whitelist
  lookup — the SQL-injection guard) and `_execute_paged_query()` (one
  COUNT(*) + LIMIT/OFFSET wiring).
- `db.get_*()` functions: `limit=None` keeps the unbounded behavior so
  `get_nearby_facilities()` and ETL scripts don't break.

**Latent bug fixed in the same commit:** `_cached()` built its key with
`(args, sorted(kwargs.items()))`, which raised
`TypeError: unhashable type: 'list'` the first time any cached helper
got a list-typed kwarg (states=['GA'], school_status=[...], etc.).
Replaced with a recursive `_hashable()` that turns lists into tuples.
Pre-existed this session — just hadn't been hit before P0 #4 started
passing list filters into the cached `get_schools`.

**Pre-existing data findings (out of scope, captured for triage):**
- `nmtc_projects.state` stores full state names ("Georgia") not 2-letter
  codes — so `/nmtc/projects?states=GA` returns 0 rows. Loader-side
  inconsistency vs schools/fqhc/ece. Worth a normalization sweep.
- `ece_centers` only has Colorado loaded — other state loaders haven't
  been run.

### P1 #6 — Batch ETL upserts ✓ (a855cf3)

`db.upsert_rows()` now groups rows by their column-set signature and
issues one prepared INSERT per group (SQLite: `executemany`; Postgres:
`psycopg2.extras.execute_values`). Two new optional kwargs:

- `touch_cols=[...]` — adds `<col>=CURRENT_TIMESTAMP` to the ON CONFLICT
  UPDATE clause. Used by the schools loader since the schema DEFAULT
  only fires on INSERT.
- `coalesce_cols=[...]` — switches matching columns to
  `c=COALESCE(excluded.c, table.c)`. Used by the census_tracts loader
  so a re-run of the main ACS pull doesn't wipe enrichment columns
  (EJScreen, OZ flag, 5yr-change deltas) populated by sibling pipelines.

Migrated three loaders away from per-row upsert calls:
- `etl/fetch_nces_schools.py:load_to_db`
- `etl/fetch_fqhc.py` (main loop)
- `etl/load_census_tracts.py` (per-state loop, with coalesce_cols
  matching the existing `preserve_if_null` set)

~20 existing callers of `upsert_rows()` (BLS, CRA, FAC, HUD, HMDA, SBA,
CDFI, FRED, Headstart, financial ratios) get the speedup for free —
the new kwargs default to None.

**Verified on Postgres:** 500-row schools batch went from 21,508 ms
(23 rows/sec, per-row) to 124 ms (4012 rows/sec, batched) — **172×**.
UPDATE branch fires correctly; address sentinel landed; updated_at
refreshed via touch_cols. Census-tract COALESCE preserves existing
`pct_minority` / `ej_index` / `county_name` when the upserted record
sets them to NULL.

### P2 #11 — Minimal pytest suite ✓ (307de0d)

`tests/` now contains:

- `test_api_smoke.py` — 11 tests covering the audit's recommended
  minimum (`/health`, `/schools`, `/schools/{id}`, `/tracts`, `/search`,
  `/search/nearby`) plus pagination/sort invariants and the
  `/search/nearby` distance-budget check from P0 #3.
- `test_perf_baseline.py` — 3 tests guarding hot paths from regression:
  `/search/nearby` warm under 500 ms (50× the current ~10 ms warm
  path), `/schools` warm under 2 s, batched upsert ≥ 5× faster than
  per-row.
- `conftest.py` — session-scoped TestClient + DB-discovered fixtures
  for `known_school_nces_id` / `known_school_state` so the suite skips
  rather than fails on a fresh empty DB.

`requirements.txt`: added `pytest>=8.0.0`, `httpx>=0.27.0`.

Also: the new tests surfaced FastAPI's `regex=` → `pattern=` deprecation
on the 5 paginated routers; swapped it (same regex, no behavior change).

**Result:** `pytest tests/ -q` — 14 passed in 6.6 s.

## Still deferred

| Item | Why deferred this session | When to revisit |
|---|---|---|
| **P1 #5** Pandas on the request path | Pagination already shrank the per-request rowsets dramatically. The remaining cost — `pd.read_sql_query → df.to_json → json.loads` — needs careful Timestamp handling before we can swap to `cur.fetchall()` + plain dicts. | Next perf push, or whenever a profiler points here. |
| **P1 #8** Runtime schema management → migrations | `init_db()` works for development; a migration system (Alembic / yoyo) adds tooling overhead without a clear payoff today. | Before the FastAPI+Postgres stack is deployed for shared/external use. |
| **P1 #9** Search uses `LIKE '%term%'` | Acceptable at current data sizes. Real fix needs FTS5 (SQLite) or pg_trgm/full-text (Postgres). | When search latency becomes a complaint. |
| **P2 #10b** `cra_assessment_areas` loader produces 0 rows | Single-pipeline diagnosis, not part of the audit's perf/architecture work. | Standalone diagnosis session. |

The pre-existing data inconsistencies surfaced during P0 #4 verification
(`nmtc_projects.state` storing full state names, `ece_centers`
single-state coverage) are loader-side fixes — separate session, not on
the audit's list.

## Suggested next session — pick one

| Priority | Item | Estimated session length |
|---:|---|---|
| P1 #5 | Drop pandas roundtrip on list endpoints (cur.fetchall + plain dicts; handle Timestamp serialization) | 1-2 hours |
| Data | Normalize `nmtc_projects.state` → 2-letter, fold into the loader | 1 hour |
| P2 #10b | Diagnose why `cra_assessment_areas` loader writes 0 rows | unknown until investigated |
| P1 #8 | Bootstrap an Alembic baseline + first migration | 2-3 hours |

P1 #5 is the next perf win. The data-shape fix on `nmtc_projects.state`
is the next correctness win and was discovered while writing the P0 #4
tests. Either is a self-contained session.
