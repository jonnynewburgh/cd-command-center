# CODEX Audit — April 26, 2026

Scope: repo audit focused on API load time, query shape, ETL throughput, and data architecture.  
Constraint honored: no existing files were edited for this audit.

## Executive summary

The main performance problem is not one single slow query. It is the combination of:

1. unbounded API endpoints,
2. full-row `SELECT *` reads,
3. pandas on the request path,
4. a cache decorator that is currently a no-op,
5. Python-side geo filtering that loads whole tables into memory,
6. row-by-row ETL writes with one connection and commit per record,
7. runtime schema management instead of explicit migrations.

There is also at least one API contract drift that looks outright broken today:

- `api/routers/schools.py` passes `risk_tiers`, `min_survival_score`, and `max_survival_score` into `db.get_schools()`, but `db.get_schools()` does not accept those arguments.
- `api/routers/schools.py` routes by `nces_id`, but `db.get_school_by_id()` queries `schools.id`.

## Observed local table sizes

These counts were read from the local `data/cd_command_center.sqlite` on 2026-04-26:

| table | rows |
|---|---:|
| `schools` | 97,750 |
| `census_tracts` | 85,396 |
| `fqhc` | 18,830 |
| `nmtc_projects` | 8,024 |
| `ece_centers` | 4,556 |
| `sba_loans` | 470,487 |
| `hmda_activity` | 82,962 |
| `cra_institutions` | 33,668 |
| `irs_990_history` | 0 |
| `cra_assessment_areas` | 0 |
| `federal_audits` | 0 |
| `headstart_programs` | 0 |

Those sizes matter because several endpoints still return whole tables or scan whole tables.

## P0 — Fix first

### 1. Schools API contract drift is broken or very close to broken

Evidence:

- `api/routers/schools.py:17-48` passes `risk_tiers`, `min_survival_score`, and `max_survival_score`.
- `db.py:1314-1323` defines `get_schools()` without those parameters.
- `api/routers/schools.py:63-69` accepts `{nces_id}` from the route.
- `db.py:1433-1441` defines `get_school_by_id()` and queries `SELECT * FROM schools WHERE id = ?`.

Why this matters:

- `GET /schools` is the primary facility list endpoint. If this route raises `TypeError`, nothing else about query tuning matters.
- `GET /schools/{nces_id}` has the wrong lookup key, which means detail loads can miss valid records and create bad caching assumptions upstream.

Action:

- Make the schools API contract explicit and consistent.
- Either remove stale survival filters from the router, or implement them in `db.get_schools()`.
- Rename the db function to match its real key, or add `get_school_by_nces_id()` and use that from the router.
- Add a smoke test that calls `/schools` with no filters and `/schools/{known_nces_id}`.

### 2. The cache layer is effectively disabled

Evidence:

- `db.py:1202-1206` defines `_cached()` as a no-op decorator.
- Many read functions still advertise TTL caching, for example `db.py:1313`, `db.py:1529`, `db.py:1733`, `db.py:1823`, `db.py:1944`, `db.py:3426`.

Why this matters:

- The code reads like hot paths are cached, but every request still hits the database.
- That false sense of safety encourages unbounded reads and expensive serialization.

Action:

- Implement a real cache or delete the decorator until one exists.
- Good first target: state lists, summary cards, latest rates, tract summaries, and org lookups.
- For local SQLite, an in-process TTL cache is enough.
- For Postgres / multi-user deployment, use an explicit cache backend and a simple invalidation story tied to `data_loads`.

### 3. Nearby search is O(all facilities) in Python per request

Evidence:

- `api/routers/search.py:29-44` calls `db.get_nearby_facilities()`.
- `db.py:2325-2376` loads all schools, all FQHCs, all ECE centers, and all NMTC projects, then filters them in Python.
- `utils/geo.py:36-66` uses `df.apply(...)` with a Python `haversine_distance()` call per row.

Why this matters:

- On the current local DB, one nearby search can touch roughly 129k facility rows before filtering.
- This gets slower as data grows and gets worse again after a move to Postgres because whole result sets must cross the DB connection before the radius filter happens.

Action:

- Push the first stage of geo filtering into SQL.
- Use a latitude/longitude bounding box in the query, then compute exact distance only on the narrowed candidate set.
- Add separate lightweight map-search queries per asset class instead of calling the heavy general list functions.
- Long-term: move geo search to PostGIS if production will rely heavily on radius queries.

### 4. Several primary list endpoints are unbounded and return full rows

Evidence:

- Routers with no pagination: `api/routers/schools.py:17-48`, `api/routers/fqhc.py:13-25`, `api/routers/tracts.py:16-37`, `api/routers/nmtc.py:13-31`.
- Corresponding DB reads use full-row queries: `db.py:1387-1424`, `db.py:1577-1582`, `db.py:1671-1680`, `db.py:1763-1769`, `db.py:1864-1869`.

Why this matters:

- `GET /schools` can serialize nearly 100k rows.
- `GET /tracts` can serialize 85k rows.
- `SELECT *` sends much more data than the map or list UI usually needs.

Action:

- Add `limit`, `offset`, and stable `sort` parameters to every list endpoint that can exceed a few thousand rows.
- Split “map points” from “full detail rows.”
- Default to lean projections for map/list views and fetch full detail separately by id.
- Return total match count separately from page size.

## P1 — High-value architecture fixes

### 5. The request path does too much pandas work and double JSON serialization

Evidence:

- Many getters use `pd.read_sql_query(...)`, for example `db.py:1424`, `db.py:1524`, `db.py:1679`, `db.py:1768`, `db.py:1869`.
- `api/deps.py:8-15` converts DataFrames to JSON by doing `df.to_json(...)` and then `json.loads(...)`.

Why this matters:

- Every request pays for DataFrame construction even when the endpoint only needs plain JSON rows.
- Then the data is serialized once by pandas and again by FastAPI.

Action:

- Keep pandas in ETL and analysis paths.
- For API list/detail routes, use cursor rows plus plain dict conversion.
- Reserve DataFrames for aggregation-heavy endpoints where they are actually buying something.

### 6. Connection handling is too chatty for both API and ETL

Evidence:

- `db.py:31-49` opens a fresh connection every time `get_connection()` is called.
- `db.py:1987-2008` opens, writes, commits, and closes for every `upsert_school()`.
- `db.py:2113-2157` does the same for every `upsert_census_tract()`.
- ETL loops call these one row at a time: `etl/fetch_nces_schools.py:347-364`, `etl/load_census_tracts.py:449-458`, `etl/fetch_fqhc.py:364-383`.
- Even the “batch” helper still loops row-by-row at the SQL layer: `db.py:1213-1258`.

Why this matters:

- This is slow in SQLite and much slower in Postgres.
- It also makes failures harder to reason about because every row is its own tiny transaction boundary.

Action:

- Introduce true batch upserts for ETL-heavy tables.
- Pass a shared connection / transaction through batch loaders.
- For Postgres, use `execute_values` or a staging-table pattern for big loads.
- Add connection pooling on the API side before external-user deployment.

### 7. Index coverage does not match actual filter patterns

Evidence:

- Existing indexes are created in `db.py:339-368`.
- `get_schools()` filters on `county`, `census_tract_id`, and enrollment bounds at `db.py:1366-1372`, but no matching indexes are created for `schools.county`, `schools.census_tract_id`, or `schools.enrollment`.
- `get_census_tracts()` filters on `county_fips` at `db.py:1573-1575`, but `init_db()` does not create `idx_tracts_county_fips`.
- `get_nmtc_projects()` filters on `cde_name` and fiscal year at `db.py:1655-1668`, but `init_db()` only indexes `state` and `census_tract_id`.
- `get_fqhc()` filters on `site_type` at `db.py:1758-1761`, but there is no `site_type` index.
- `get_ece_centers()` filters on `facility_type` and `accepts_subsidies` at `db.py:1852-1859`, but there are no matching indexes.

Why this matters:

- Some slow endpoints are slow because the query shape is bad.
- Others are slow because the query shape is reasonable but the schema is not supporting it.

Action:

- Add indexes that follow real filters, not just obvious columns.
- Likely first pass:
  - `schools(census_tract_id)`
  - `schools(county)`
  - `schools(enrollment)`
  - `census_tracts(county_fips)`
  - `nmtc_projects(fiscal_year)`
  - `nmtc_projects(cde_name)`
  - `fqhc(site_type)`
  - `ece_centers(facility_type, accepts_subsidies)`
- Re-check with actual query plans after each addition instead of indexing by instinct.

### 8. Runtime schema management is doing migration work

Evidence:

- `api/main.py:64-66` runs `db.init_db()` at app startup.
- ETL scripts also call `db.init_db()`, for example `etl/load_census_tracts.py:434` and `etl/fetch_fqhc.py:362`.
- `db.py:82-1190` mixes table creation, `ALTER TABLE`, and index creation into one giant startup function.

Why this matters:

- App boot time now includes schema drift handling.
- It is hard to know which schema version a given environment is really on.
- This makes SQLite-to-Postgres migration harder, not easier, because schema evolution is implicit.

Action:

- Move schema changes to explicit versioned migrations.
- Keep `db.py` as the data access surface if you want, but stop using runtime `ALTER TABLE` as the migration system.
- Keep `init_db()` only for local bootstrap or tests once migrations exist.

### 9. Search is still substring scanning, not search infrastructure

Evidence:

- `db.py:1926-1980` builds `LIKE '%term%'` queries across multiple columns and tables.
- Several name indexes exist, but leading-wildcard `LIKE` will not use them well.

Why this matters:

- Search latency will climb with every new data domain.
- This is exactly the kind of feature that feels fine in SQLite dev and suddenly feels sticky in production.

Action:

- For SQLite: consider FTS5 for the main search entities.
- For Postgres: use `pg_trgm` or full-text search depending on desired behavior.
- Keep the current endpoint contract, but back it with a purpose-built search layer.

## P2 — Data and product gaps

### 10. Several routed domains are empty in the current DB

Observed on the local SQLite database:

- `irs_990_history = 0`
- `cra_assessment_areas = 0`
- `federal_audits = 0`
- `headstart_programs = 0`

Why this matters:

- These are not just “future nice-to-haves.” There are live routers for some of them.
- Empty tables make the UI look broken even when the API itself is functioning.

Action:

- Add a lightweight data-status endpoint that reports row counts and last refresh time by domain.
- Hide or label empty modules in the frontend until their loaders are in place.
- Prioritize the loaders that support already-exposed routes first.

### 11. There is no real API/performance test harness yet

Evidence:

- `pytest tests/` is referenced in docs, but `tests/` is currently absent.

Why this matters:

- The schools route drift is exactly the kind of bug a tiny API smoke suite would have caught immediately.
- Performance work without baseline tests turns into guesswork.

Action:

- Add a minimal `tests/` package.
- First tests should cover:
  - `/health`
  - `/schools`
  - `/schools/{known_nces_id}`
  - `/tracts?states=GA`
  - `/search?q=...`
- Add one benchmark-style test or scripted measurement for:
  - list endpoint latency,
  - nearby search latency,
  - ETL throughput for one representative loader.

## Suggested order of attack

1. Fix the `schools` API contract drift and add smoke tests for it.
2. Add pagination plus lean projections to `schools`, `tracts`, `fqhc`, `ece`, and `nmtc`.
3. Replace `nearby` with SQL prefiltering and lighter row shapes.
4. Turn `_cached()` into a real cache for the obvious read hotspots.
5. Batch the ETL write path and add connection pooling.
6. Add the missing indexes that match actual filters.
7. Move schema changes out of runtime startup and into explicit migrations.
8. Add data-status reporting so empty domains are visible as data gaps, not mystery blanks.

## Short version for Claude Code

If Claude Code is going to help here, the highest-leverage work is:

1. make `/schools` correct,
2. make list endpoints paginated and projection-aware,
3. stop full-table Python geo filtering,
4. stop pretending the cache exists,
5. batch ETL writes and stop opening one transaction per row,
6. add missing indexes based on real query filters,
7. introduce a migration system before the Postgres move gets deeper.
