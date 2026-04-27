---
title: CODEX audit follow-up — what shipped, what's deferred
date: 2026-04-27
scope: Triage of `docs/debug/codex_audit_2026-04-26.md` (perf + architecture audit)
status: 3 items shipped this session; remainder triaged + scheduled
---

# CODEX audit follow-up — 2026-04-27

The audit ([codex_audit_2026-04-26.md](codex_audit_2026-04-26.md))
identified 11 items across performance, architecture, and data
gaps. This doc records what was addressed in this session vs.
what was deferred and why.

## Shipped this session

### P0 #1 — Schools API contract drift ✓

`api/routers/schools.py` was passing `risk_tiers`,
`min_survival_score`, `max_survival_score` to a `db.get_schools()`
that didn't accept them, and `/schools/{nces_id}` was calling
`get_school_by_id()` whose SQL was `WHERE id = ?` (the wrong key).

Both fixed:
- Added `risk_tiers`, `min_survival_score`, `max_survival_score` to
  `db.get_schools()` with WHERE clauses against
  `schools.survival_risk_tier` / `schools.survival_score`
  (columns confirmed to exist).
- Renamed `get_school_by_id` to `get_school_by_nces_id`; SQL now
  `WHERE nces_id = ?`. Old name kept as a deprecated alias
  forwarding to the new function.
- Router updated to call `get_school_by_nces_id(nces_id)`.

Verified end-to-end: `/schools?states=GA&risk_tiers=High&min_survival_score=0.5`
returns 200 (was 500); `/schools/{nces_id}` returns the actual
school record (was returning empty dict → fake 404).

### P0 #2 — `_cached()` was a no-op ✓

`db._cached()` was a pass-through decorator with TTL annotations
on ~12 read functions that did nothing. Replaced with a real
in-process TTL cache:

- Thread-safe `dict` keyed on `(qualname, args, sorted_kwargs)`.
- TTL stored as monotonic-clock expiry; misses re-run the function.
- Public `cache_clear()` for invalidation after writes / pipeline
  runs / tests.

Verified: `get_school_states()` cold = 61ms, warm = 0.011ms
(5,664× speedup); identity-stable across calls; `cache_clear()`
correctly evicts.

For multi-worker production: swap the in-process dict for Redis;
the decorator interface stays the same.

### P1 #7 — Missing indexes ✓

8 indexes added to `init_db()` and applied to live Postgres:

```
schools(census_tract_id)
schools(county)
schools(enrollment)
census_tracts(county_fips)
nmtc_projects(fiscal_year)
nmtc_projects(cde_name)
fqhc(site_type)
ece_centers(facility_type, accepts_subsidies)
```

Each backs a filter actually used by routed `get_*` functions.
Audit's "re-check with query plans after each addition" guidance
deferred — current local data sizes are small enough that
`EXPLAIN ANALYZE` improvements aren't measurable. Re-run when
production data scales.

## Deferred

### P0 #3 — Nearby search Python-side filtering

Audit calls out: `get_nearby_facilities()` loads all schools, all
FQHCs, all ECEs, all NMTCs into pandas, then runs `df.apply()`
with Python `haversine_distance()` per row. ~129K rows touched
per request.

**Why deferred:** real fix is SQL bbox prefilter + lighter row
shapes (or PostGIS for proper radius queries). 1-2 hour focused
session. Doesn't block correctness; only latency. Not a 30-min
cleanup.

**Suggested approach:** add `lat BETWEEN ? AND ?` /
`lon BETWEEN ? AND ?` to each get-by-radius helper, with bbox
computed from `radius_miles` in Python (one-degree-lat ≈ 69 mi,
one-degree-lon scales by cos(lat)). Then run `filter_by_radius`
on the prefiltered DataFrame.

### P0 #4 — Unbounded list endpoints + full-row reads

`/schools` can serialize ~100K rows. `/tracts` ~85K. No pagination.
`SELECT *` everywhere.

**Why deferred:** touches ~5 list endpoints and their corresponding
`get_*` functions. Need to define a consistent
pagination/projection contract (limit/offset, sort, total-count
header) before applying. Multi-session work.

**Suggested approach:** standardize on a
`Page[T] = {items: list[T], total: int, limit: int, offset: int}`
response shape; add `?limit=&offset=&sort=` to each list endpoint
backed by `LIMIT/OFFSET` in SQL. Lean projections (`SELECT
nces_id, school_name, latitude, longitude` for map views) split
from full-detail endpoints.

### P1 #5 — Pandas on the request path

`pd.read_sql_query` → `df.to_json` → `json.loads` round-trip on
list endpoints. Costly when `SELECT *` returns whole tables.

**Why deferred:** untangles with #4 (pagination + projections).
Once endpoints return small page slices, the pandas overhead
matters less; once they return plain dicts via `cur.fetchall()`,
this item is closed by construction. Defer until #4 lands.

### P1 #6 — Row-by-row ETL writes

`upsert_school`, `upsert_census_tract`, etc. open a connection,
write one row, commit, close. ETL loops repeat per row. Slow on
SQLite, much slower on Postgres.

**Why deferred:** the ETL Postgres-compat sweep (Phases 1-6) is
complete; the next layer is throughput. Replacing per-row
`upsert_*` calls with `psycopg2.extras.execute_values` (Postgres)
or `executemany` (SQLite) is a multi-session refactor — needs
careful per-table testing because every loader has its own row
shape.

**Suggested approach:** introduce a `db.batch_upsert(table, rows,
unique_cols, preserve_cols)` helper, then migrate one loader at
a time. Existing `db.upsert_rows()` already does this for
`bls_unemployment` (FRED loader); generalize that pattern.

### P1 #8 — Runtime schema management

`init_db()` runs at API startup and inside ETL scripts. Mixes
table creation, ALTER TABLE, and index creation in one giant
function. Audit recommends moving to explicit versioned
migrations.

**Why deferred:** larger architectural decision. Existing setup
works for development; introducing a migration system (Alembic,
yoyo, etc.) before multi-environment deployment adds tooling
overhead without a clear payoff today.

**When to revisit:** before the FastAPI+Postgres stack is
deployed for shared/external use. At that point pick a migration
tool and split `init_db()` into a "create-from-scratch" baseline
+ versioned migration files.

### P1 #9 — Search uses `LIKE '%term%'`

`db.search_all` does substring scans across multiple tables/cols.
Won't use indexes well (leading-wildcard).

**Why deferred:** acceptable at current data sizes; revisit when
search latency becomes a complaint. Postgres has `pg_trgm` and
full-text indexes available; SQLite has FTS5. Both require
schema work.

### P2 #10 — Empty domain tables

`irs_990_history`, `cra_assessment_areas`, `headstart_programs`
listed as empty in the audit. **As of 2026-04-27:**

- `irs_990_history`: empty in SQLite (snapshot stale); 142 rows
  in Postgres after this session's multi-year IRS history
  loader work; will repopulate fully when
  `python etl/fetch_990_irs.py --years 2022 2023 2024` is run
  end-to-end (~30-60 min).
- `headstart_programs`: 0 in SQLite, 45,962 in Postgres
  (loaded after the SQLite snapshot date — see
  `phase6_reconciliation_2026-04-26.md`).
- `federal_audits`: 0 in SQLite, 64,869 in Postgres (same).
- `cra_assessment_areas`: 0 on both backends — loader produces
  no rows for any state filter currently used; needs separate
  diagnosis.

A "data-status endpoint" (audit's suggestion) is a good idea
but lower priority than fixing the actual loaders.

### P2 #11 — No test harness

`tests/` directory absent. Audit suggests minimal pytest suite
covering core endpoints + benchmarks.

**Why deferred:** the audit's #1 recommended test
(`/schools/{known_nces_id}` smoke) was just executed manually in
this session and is now passing. A real test suite is valuable
but out of scope for "address the audit findings" in one
session.

**When to revisit:** alongside #4 (pagination contract) so the
new contract has tests from day one.

## Suggested next session — pick one

| Priority | Item | Estimated session length |
|---:|---|---|
| P0 #3 | Nearby search SQL bbox prefilter | 1-2 hours |
| P0 #4 | Pagination + lean projections (5 endpoints) | one session per 1-2 endpoints |
| P1 #6 | ETL batch upsert helper + migrate `schools` loader | 2-3 hours |
| P2 #11 | Bootstrap minimal `tests/` suite (5-7 endpoints) | 1-2 hours |

Recommend P0 #3 next — it's a self-contained perf win (single
function rewrite + one helper), and `get_nearby_facilities` is
on the critical map-view path.
