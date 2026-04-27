# Next session prompt — P0 #3 nearby-search SQL prefilter

Time block: **1.5–2 hour dedicated session.** Single bug class, single
function rewrite, single helper. Don't bundle other audit items.

## Goal

Cut `get_nearby_facilities()` from O(all-facilities-in-DB) Python-side
filtering to O(rows-inside-bbox) SQL-side filtering.

Today: every `/search/nearby` request loads ~129K facility rows
(schools 97,750 + fqhc 18,830 + ece 4,556 + nmtc_projects 8,024) into
pandas, then runs `df.apply()` with a Python `haversine_distance` per
row. Round-trip cost dominates the request.

After: each query uses a lat/lon bounding box derived from the radius
to push the first filter into the database. The bbox produces ~1-100
candidate rows per facility type instead of ~thousands. The exact
distance check and sort still happen in Python on the narrowed set.

## Files in scope

- `db.py` — `get_nearby_facilities` (around db.py:2336). Add an
  internal `_within_bbox` helper or inline the bbox WHERE in each
  branch.
- `utils/geo.py` — `filter_by_radius()`. Keep as-is (final
  exact-distance pass after the SQL prefilter).
- `api/routers/search.py` — caller. Probably no change needed.

## Approach

1. Compute bbox in Python from `(lat, lon, radius_miles)`:

   ```python
   # 1 deg lat ≈ 69 mi; 1 deg lon scales with cos(lat).
   lat_delta = radius_miles / 69.0
   lon_delta = radius_miles / (69.0 * max(0.01, math.cos(math.radians(lat))))
   min_lat, max_lat = lat - lat_delta, lat + lat_delta
   min_lon, max_lon = lon - lon_delta, lon + lon_delta
   ```

2. Add a per-table SQL helper that pulls only rows inside the bbox
   instead of calling the unfiltered `get_*` helpers. Each facility
   table has `latitude` + `longitude` columns. Example for schools:

   ```python
   sql = adapt_sql("""
       SELECT * FROM schools
       WHERE latitude  BETWEEN ? AND ?
         AND longitude BETWEEN ? AND ?
   """)
   df = pd.read_sql_query(sql, conn,
                          params=[min_lat, max_lat, min_lon, max_lon])
   ```

3. Run the existing `filter_by_radius(df, lat, lon, radius_miles)`
   for the exact distance + sort.

4. Keep the existing per-branch `try/except: logger.exception(...)`
   shape — Phase 1's silent-handler discipline still applies.

## Indexes

The bbox query on `latitude BETWEEN ? AND ?` needs an index to be
useful. Add to `init_db()`:

```python
cur.execute("CREATE INDEX IF NOT EXISTS idx_schools_latlon ON schools(latitude, longitude)")
cur.execute("CREATE INDEX IF NOT EXISTS idx_fqhc_latlon    ON fqhc(latitude, longitude)")
cur.execute("CREATE INDEX IF NOT EXISTS idx_ece_latlon     ON ece_centers(latitude, longitude)")
cur.execute("CREATE INDEX IF NOT EXISTS idx_nmtc_latlon    ON nmtc_projects(latitude, longitude)")
```

(Postgres can skip these if it prefers a sequential scan on small
tables; doesn't hurt.)

## Verification

1. **Correctness** — same lat/lon/radius input must return the same
   record set as the current implementation. Sample for downtown
   Atlanta (33.7490, -84.3880, 5 mi):

   ```python
   import db
   r = db.get_nearby_facilities(33.7490, -84.3880, radius_miles=5.0)
   # Expected (current implementation): 67 schools / 24 fqhc / 0 ece / 56 nmtc
   ```

   The new implementation should return the **same** counts on the
   same input. Spot-check a few row IDs match.

2. **Latency** — wrap the call in `time.perf_counter()` cold + warm.
   Today's cold time on the local Postgres is roughly 1–3 seconds.
   Target: <100 ms.

3. **FastAPI route** — run a TestClient `GET /search/nearby?lat=33.7490&lon=-84.3880&radius=5`
   and confirm the JSON response shape is unchanged.

## Hard stops

Per the established session discipline (Phase 1 anti-patterns):

- **Don't fix unrelated audit items mid-session.** No pagination, no
  pandas-on-request-path refactor, no SQL projection cleanup. Capture
  in commit message if you find them.
- **Don't expand scope to PostGIS.** That's a separate, larger
  decision (P0 #3 long-term in the audit). Bbox-in-SQL gets you 95%
  of the win without the dependency.
- **If you find a new bug class** while reading the function (e.g.
  another raw-? site, another silent except), capture and continue.
  Reconciliation findings are fine as commit-body footnotes.

## Commit shape

Single commit titled something like:

> Optimize get_nearby_facilities: SQL bbox prefilter (CODEX P0 #3)

Body should record:
- before/after row counts touched per request,
- before/after latency measurements,
- the indexes added,
- correctness check (count parity on the test input).

## Reference

- Audit doc: `docs/debug/codex_audit_2026-04-26.md` § P0 #3
- Triage: `docs/debug/codex_audit_followup_2026-04-27.md`
- Current implementation: `db.py` `get_nearby_facilities`
- Geo helper: `utils/geo.py` `filter_by_radius`, `haversine_distance`

## How to start

Open Claude Code in the repo root and paste:

> Read `docs/nearby-search-session-prompt.md`. Execute it.
