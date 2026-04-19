---
title: FQHC pipeline diagnosis
date: 2026-04-18
scope: upsert_fqhc, get_fqhc_summary, get_fqhc_by_id in db.py + etl/fetch_fqhc.py
status: Diagnosis only — no fixes applied
---

# FQHC pipeline diagnosis — 2026-04-18

## TL;DR

Same bug class as the schools pipeline, smaller surface:

- **BUG 1 (missing `_adapt_sql`): present** at `db.py:2299` in `upsert_fqhc`. Unlike `upsert_school`, there is **no silent `try/except`** inside `upsert_fqhc` — the error propagates to the ETL caller, which has a `try/except` at `etl/fetch_fqhc.py:377` that **prints `"DB error for site {bhcmis_id}: {e}"`** for every row. So on Postgres this fails loudly per-row, not silently, but still loads zero rows.
- **BUG 2 (unqualified COALESCE / preserve_if_null in ON CONFLICT):** N/A. `upsert_fqhc` uses plain `col=excluded.col` — no null-preservation logic, so no Postgres `AmbiguousColumn` risk after BUG 1 is fixed.
- **BUG 3 (`dict(row)` in readers): present twice** — `get_fqhc_summary` at `db.py:1848` and `get_fqhc_by_id` at `db.py:2337`. Both fail silently on Postgres (caught by bare `except Exception: return {}`), masking real data behind empty dicts.
- **Latent bug check:** `is_active = 1` literal at `db.py:1841` — **not a bug**. `is_active` is `INTEGER` on both backends (verified via `information_schema.columns` → `integer`/`int4`, and SQLite `PRAGMA table_info` → `INTEGER`), current distinct values `{1}` only. Same pattern as `is_charter` — safe under current schema.
- **No `updated_at` column on `fqhc`.** The upsert sets only `created_at` (defaults on insert, not touched on conflict). Verification strategy after the fix must use direct probes or new-row creation rather than looking for a bumped timestamp.

## 1. Inventory

### `upsert_fqhc` (`db.py:2282–2301`)

```
2282  def upsert_fqhc(record: dict):
2283      """Insert or update a FQHC site record (keyed on bhcmis_id)."""
2284      conn = get_connection()
2285      cur = conn.cursor()
2286  
2287      columns = list(record.keys())
2288      values = list(record.values())
2289      placeholders = ",".join("?" * len(values))
2290      update_clause = ",".join(
2291          f"{col}=excluded.{col}" for col in columns if col != "bhcmis_id"
2292      )
2293  
2294      sql = f"""
2295          INSERT INTO fqhc ({",".join(columns)})
2296          VALUES ({placeholders})
2297          ON CONFLICT(bhcmis_id) DO UPDATE SET {update_clause}
2298      """
2299      cur.execute(sql, values)
2300      conn.commit()
2301      conn.close()
```

### `get_fqhc_summary` (`db.py:1832–1852`)

```
1832  @_cached(ttl=300)
1833  def get_fqhc_summary() -> dict:
1834      """Return high-level FQHC counts."""
1835      conn = get_connection()
1836      cur = conn.cursor()
1837      try:
1838          cur.execute("""
1839              SELECT
1840                  COUNT(*) as total_sites,
1841                  SUM(CASE WHEN is_active = 1 THEN 1 ELSE 0 END) as active_sites,
1842                  COUNT(DISTINCT health_center_name) as unique_health_centers,
1843                  COUNT(DISTINCT state) as states_served,
1844                  SUM(total_patients) as total_patients
1845              FROM fqhc
1846          """)
1847          row = cur.fetchone()
1848          result = dict(row) if row else {}
1849      except Exception:
1850          result = {}
1851      conn.close()
1852      return result
```

### `get_fqhc_by_id` (`db.py:2329–2340`)

```
2329  def get_fqhc_by_id(bhcmis_id: str) -> dict:
2330      """Return a single FQHC site by its bhcmis_id. Returns empty dict if not found."""
2331      conn = get_connection()
2332      cur = conn.cursor()
2333      try:
2334          cur.execute("SELECT * FROM fqhc WHERE bhcmis_id = ?", (bhcmis_id,))
2335          row = cur.fetchone()
2336          conn.close()
2337          return dict(row) if row else {}
2338      except Exception:
2339          conn.close()
2340          return {}
```

Also note: `SELECT * ... WHERE bhcmis_id = ?` has raw `?` placeholders (no `_adapt_sql`) — but on Postgres, the placeholder failure raises before `dict(row)` ever runs, so the net effect is the same: caught by bare except, returns `{}`. Fixing bug 3 alone (`zip(cur.description, row)`) will not save this function on Postgres; the `cur.execute` also needs `_adapt_sql`, or equivalently switch the placeholder. **Scope note:** I will propose fixing both `get_fqhc_summary` and `get_fqhc_by_id` with the standard pattern; `get_fqhc_by_id` needs both the `_adapt_sql` wrap on its `cur.execute` *and* the `zip(cur.description, row)` pattern.

### Other FQHC helpers

- `get_fqhc_states` (`db.py:1819–1829`) — uses `row[0]` index, no `dict(row)` bug. Safe.
- `batch_update_fqhc_geo` (`db.py:2304–2326`) — already has `_adapt_sql` wrap at line 2317. Safe.

### Loader entry point (`etl/fetch_fqhc.py`)

```
Command:  python etl/fetch_fqhc.py
          python etl/fetch_fqhc.py --file data/raw/hrsa_health_centers.csv
          python etl/fetch_fqhc.py --states GA [--file ...]
```

Main loop calls `db.upsert_fqhc(record)` at line 375 with a per-row `try/except` that **prints** the error (not silent):

```
374          try:
375              db.upsert_fqhc(record)
376              loaded += 1
377          except Exception as e:
378              site_id = record.get("bhcmis_id", "?")
379              print(f"    DB error for site {site_id}: {e}")
380              errors += 1
```

Good news for diagnosis: on Postgres today (pre-fix), a test run would print thousands of visible `DB error for site ...` lines instead of silently loading zero. Bad news: it would still load zero rows.

Then at line 391 it calls `db.get_fqhc_summary()` and prints the counts — which on Postgres would show all zeros (from `{}`) even if the insert path were fixed, until `get_fqhc_summary` is also patched.

## 2. Three-bug sweep on `upsert_fqhc`

| # | Bug | In `upsert_fqhc`? | Location |
|---|---|---|---|
| 1 | Missing `_adapt_sql()` wrap | **YES** | `db.py:2299` |
| 2 | Unqualified COALESCE / preserve_if_null in ON CONFLICT update clause | **N/A** | Plain `col=excluded.col` — no COALESCE logic |
| 3 | `dict(row)` on psycopg2 return in related readers | **YES (two places)** | `db.py:1848` (`get_fqhc_summary`), `db.py:2337` (`get_fqhc_by_id`) |

Unlike `upsert_school`, `upsert_fqhc` **does not** have its own `try/except` swallow, and does not loop over alternative tables (`charter_schools` fallback). Cleaner, smaller fix.

## 3. Latent-bug sweep

### `is_active = 1` literal (`db.py:1841`)

- Postgres `fqhc.is_active` type: `integer` (`int4`) — confirmed via `information_schema.columns`.
- SQLite `fqhc.is_active` type: `INTEGER` — confirmed via `PRAGMA table_info(fqhc)`.
- Stored values on both backends: `{1}` (all 18,828 rows are active).
- `WHERE is_active = 1` works correctly on both. **No bug.** Same result as the `is_charter` check in the schools sweep.

### `ON CONFLICT(bhcmis_id)` target

- Postgres has a `UNIQUE` constraint on `bhcmis_id` (`fqhc_bhcmis_id_key` index). `ON CONFLICT(bhcmis_id)` is valid.
- SQLite `bhcmis_id TEXT UNIQUE` declared in DDL. Same.
- Both backends will accept the upsert once `_adapt_sql` is applied.

### `updated_at` column — **absent from `fqhc`**

Unlike `schools` and `census_tracts`, the `fqhc` DDL declares only `created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP` (`db.py:317`) — no `updated_at`. Consequences:

- `upsert_fqhc`'s ON CONFLICT clause does **not** include `updated_at=CURRENT_TIMESTAMP` (correct — the column doesn't exist). No fix needed here.
- Post-run verification cannot rely on "rows whose `updated_at` flipped today." We'll need an alternative: (a) direct probe (upsert a synthetic record, select it back, clean up), or (b) inspect `created_at` for any brand-new `bhcmis_id` rows not already in the table.

### Dialect / reserved-word / cast issues

- No dialect-sensitive identifiers (all columns lowercase, no reserved words).
- No ARRAY / JSON columns.
- No `pg_typeof(...)` asymmetry.
- Integer booleans (`is_active`) are stored as `int4` — safe under current schema.

Nothing else flagged.

## 4. Current state — both backends in sync

| Metric | Postgres | SQLite |
|---|---:|---:|
| `fqhc` table exists | yes | yes |
| Row count | 18,828 | 18,828 |
| GA rows | 459 | 459 |
| `MAX(created_at)` | 2026-03-25 02:02:40 | 2026-03-25 02:02:40 |
| `is_active` distinct values | `{1}` | `{1}` |

Top Postgres states: CA=2,989 · NY=879 · TX=832 · FL=783 · NC=776.

No `updated_at` column on either side.

Both backends are currently synced. The `upsert_fqhc` bug means today's Postgres sync must have come from a pre-bug load (most plausibly: initial load was against SQLite, then the SQLite DB was migrated wholesale to Postgres — matches the `created_at` timestamp being identical byte-for-byte across backends).

## 5. Proposed edits (not applied)

### Edit 1 — `upsert_fqhc`, `db.py:2299`

**Before:**
```python
    sql = f"""
        INSERT INTO fqhc ({",".join(columns)})
        VALUES ({placeholders})
        ON CONFLICT(bhcmis_id) DO UPDATE SET {update_clause}
    """
    cur.execute(sql, values)
    conn.commit()
    conn.close()
```

**After:**
```python
    sql = f"""
        INSERT INTO fqhc ({",".join(columns)})
        VALUES ({placeholders})
        ON CONFLICT(bhcmis_id) DO UPDATE SET {update_clause}
    """
    cur.execute(_adapt_sql(sql), values)
    conn.commit()
    conn.close()
```

One-character surface change on line 2299. Same shape as the `upsert_school` fix in `c87742b` and `upsert_census_tract` fix in `c1fc5b3`.

### Edit 2 — `get_fqhc_summary`, `db.py:1848`

**Before:**
```python
            row = cur.fetchone()
            result = dict(row) if row else {}
        except Exception:
            result = {}
        conn.close()
        return result
```

**After:**
```python
            row = cur.fetchone()
            if not row:
                result = {}
            else:
                cols = [d[0] for d in cur.description]
                result = dict(zip(cols, row))
        except Exception:
            result = {}
        conn.close()
        return result
```

Replaces the single `dict(row)` line with the backend-agnostic `zip(cur.description, row)` pattern. Matches `get_school_summary` and `get_census_tract_summary` after prior fixes.

### Edit 3 — `get_fqhc_by_id`, `db.py:2334` and `2337`

**Before:**
```python
    try:
        cur.execute("SELECT * FROM fqhc WHERE bhcmis_id = ?", (bhcmis_id,))
        row = cur.fetchone()
        conn.close()
        return dict(row) if row else {}
    except Exception:
        conn.close()
        return {}
```

**After:**
```python
    try:
        cur.execute(_adapt_sql("SELECT * FROM fqhc WHERE bhcmis_id = ?"), (bhcmis_id,))
        row = cur.fetchone()
        if not row:
            conn.close()
            return {}
        cols = [d[0] for d in cur.description]
        result = dict(zip(cols, row))
        conn.close()
        return result
    except Exception:
        conn.close()
        return {}
```

Adds `_adapt_sql` wrap (necessary because `?` placeholders reach psycopg2 directly) and swaps `dict(row)` for the `zip(cur.description, row)` pattern.

### Scope reminder

- Do not fix `is_active = 1` at line 1841 — it's not a bug (see § 3).
- Do not change the bare `except Exception` handlers — out of scope.
- Do not touch `batch_update_fqhc_geo` — already correct.
- Do not touch `get_fqhc_states` — already correct.

## 6. Entry-point command + expected row-count delta

**Command for Postgres smoke test (after fix):**

```
python etl/fetch_fqhc.py --states GA
```

or with a pre-downloaded file:

```
python etl/fetch_fqhc.py --states GA --file data/raw/hrsa_health_centers.csv
```

**Expected row-count delta:**

- HRSA publishes periodic updates; last load was ~2026-03-25. A run today will ON CONFLICT-update most existing rows and insert any newly-added HRSA sites. Typical HRSA delta between releases is single-digit to low-double-digit new sites per state per quarter.
- On GA specifically: baseline 459. Post-run: likely 459–470 (±minor change based on HRSA data age).
- **The true success signal is not row count.** It's (a) zero printed `DB error for site ...` lines, and (b) `get_fqhc_summary()` returning real numbers instead of `{}`.

**Direct-probe verification (no loader required):**

```python
db.upsert_fqhc({
    "bhcmis_id": "TEST_PROBE_FQHC_0001",
    "health_center_name": "Test Probe HC",
    "site_name": "Test Probe Site",
    "state": "GA",
    "is_active": 1,
    "data_year": 2024,
})
db.get_fqhc_by_id("TEST_PROBE_FQHC_0001")   # should return non-empty dict on both backends
# cleanup: DELETE FROM fqhc WHERE bhcmis_id = 'TEST_PROBE_FQHC_0001'
```

If the probe round-trips correctly, the fix is verified independent of HRSA. I'd run this as step 7a before running the loader to isolate data-source variability from the fix's correctness.

## Out-of-scope items (tracked, not fixed)

- 13+ other `upsert_*` functions in `db.py` with the same `_adapt_sql` miss.
- Silent `except Exception: ...` patterns across FQHC readers — intentional swallowing of legitimate errors masks real problems.
- No `updated_at` column on `fqhc` — makes future refresh-audit queries harder. Schema change, not a bug fix.
- ECE (`upsert_ece`, `get_ece_by_id`) likely has the same bug class — next in queue.

## Known pipeline gap — UDS data absent

The `fqhc` table is **site-directory-only**, sourced from HRSA's "Health Center Service Delivery and Look-Alike Sites" CSV (single endpoint, no auth). It carries site identity, address, parent-org name, FQHC/Look-Alike/Grantee classification, EIN, and two lightweight patient-count integers (`total_patients`, `patients_below_200pct_poverty`) that HRSA publishes inline with the directory when available.

It does **not** include UDS (Uniform Data System) performance data:

- No payer mix: no `pct_medicaid`, `pct_medicare`, `pct_uninsured`, `pct_private`
- No visit or encounter counts
- No clinical quality measures: no diabetes A1c control rate, no hypertension control rate, no cervical/breast/colorectal cancer screening rates, no prenatal care in first trimester, no childhood immunizations, no depression screening
- No staffing FTEs by role (physicians, NP/PA, dental, behavioral health, etc.)
- No revenue/cost fields: no total operating revenue, no federal Section 330 grant funding, no uncompensated care, no cost per patient
- No services-offered flags (medical, dental, behavioral, vision, pharmacy, enabling services)

### Why this matters for deal origination

For CDFI underwriting of FQHC borrowers, UDS data is load-bearing — it's the primary view into operational performance, payer mix (which drives revenue stability), and clinical outcomes (which drive Section 330 compliance). The 990 path covers some financial ground but does not substitute for UDS on utilization or clinical measures.

### What adding UDS would require (out of scope for this session)

- **New data source.** HRSA publishes UDS annually with an ~9-month lag. The raw file is the "UDS National Rollup" and per-grantee UDS reports — different endpoints from the site-directory CSV used today. Typically a CSV or Excel per year.
- **New schema.** Likely a separate `fqhc_uds_reports` table keyed on `bhcmis_org_id + reporting_year` (note: *org-level* ID, not the site-level `bhcmis_id` used in `fqhc`). HRSA publishes UDS at the grantee/organization level, not per site.
- **New ETL script.** `etl/fetch_fqhc_uds.py` or similar, with its own download + column-map + upsert path.
- **New joins.** Operator detail views would need to fan out from `fqhc.health_center_name` or `fqhc.ein` to the UDS table, probably via a lookup between BHCMIS site ID and BHCMIS org ID.

### This session's scope

This Postgres-compatibility fix (`upsert_fqhc`, `get_fqhc_summary`, `get_fqhc_by_id`) ships the **existing** site-directory pipeline correctly against both backends. It does not attempt to expand scope to UDS. The UDS gap is tracked here for the next dedicated session.
