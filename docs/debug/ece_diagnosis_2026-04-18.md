---
title: ECE pipeline diagnosis
date: 2026-04-18
scope: upsert_ece, get_ece_summary, get_ece_by_id in db.py + etl/load_ece_data.py
status: Diagnosis only — no fixes applied
---

# ECE pipeline diagnosis — 2026-04-18

## TL;DR

Identical bug shape to the FQHC fix (`0b63cd7`), minus the dialect-specific latent landmines:

- **BUG 1 (`_adapt_sql` missing in upsert): present** at `db.py:2256` in `upsert_ece`. No internal `try/except` — errors propagate to the ETL caller at `etl/load_ece_data.py:444`, which catches and **prints** each failure (visible, not silent — same pattern as FQHC).
- **BUG 2 (unqualified COALESCE / preserve_if_null):** N/A. Plain `col=excluded.col`; no null-preservation logic.
- **BUG 3 (`dict(row)` in readers): present twice** — `get_ece_summary` at `db.py:1948` and `get_ece_by_id` at `db.py:2360`. Both fail silently on Postgres via bare `except Exception`.
- **Latent bug check:** `accepts_subsidies = 1` at `db.py:1944` — **not a bug**. Column is `integer`/`INTEGER` on both backends (matches `is_charter` / `is_active` precedent). No reserved-word collisions; all ECE column names are schema-safe. No BOOLEAN assumptions anywhere.
- **`get_ece_centers` (DataFrame reader)** has a raw `?` placeholder path at `db.py:1909`. Latent on Postgres but **out of scope** for this phase — user specified dict(row) readers only; this function uses `pd.read_sql_query`. Flagged for future.
- Both backends currently in sync at **4,556 rows, CO only** (last load 2026-03-23). The `ece_centers` table has no `updated_at` column (same as `fqhc`).

## 1. Inventory

### `upsert_ece` (`db.py:2239–2258`)

```
2239  def upsert_ece(record: dict):
2240      """Insert or update an ECE center record (keyed on license_id)."""
2241      conn = get_connection()
2242      cur = conn.cursor()
2243  
2244      columns = list(record.keys())
2245      values = list(record.values())
2246      placeholders = ",".join("?" * len(values))
2247      update_clause = ",".join(
2248          f"{col}=excluded.{col}" for col in columns if col != "license_id"
2249      )
2250  
2251      sql = f"""
2252          INSERT INTO ece_centers ({",".join(columns)})
2253          VALUES ({placeholders})
2254          ON CONFLICT(license_id) DO UPDATE SET {update_clause}
2255      """
2256      cur.execute(sql, values)
2257      conn.commit()
2258      conn.close()
```

### `get_ece_summary` (`db.py:1932–1953`)

```
1932  @_cached(ttl=300)
1933  def get_ece_summary() -> dict:
1934      """Return high-level ECE counts."""
1935      conn = get_connection()
1936      cur = conn.cursor()
1937      try:
1938          cur.execute("""
1939              SELECT
1940                  COUNT(*) as total_centers,
1941                  SUM(CASE WHEN license_status = 'Active' THEN 1 ELSE 0 END) as active_centers,
1942                  SUM(capacity) as total_capacity,
1943                  COUNT(DISTINCT state) as states_covered,
1944                  SUM(CASE WHEN accepts_subsidies = 1 THEN 1 ELSE 0 END) as subsidized_centers
1945              FROM ece_centers
1946          """)
1947          row = cur.fetchone()
1948          result = dict(row) if row else {}
1949      except Exception:
1950          result = {}
1951      conn.close()
1952      return result
```

### `get_ece_by_id` (`db.py:2352–2363`)

```
2352  def get_ece_by_id(license_id: str) -> dict:
2353      """Return a single ECE center by its license_id. Returns empty dict if not found."""
2354      conn = get_connection()
2355      cur = conn.cursor()
2356      try:
2357          cur.execute("SELECT * FROM ece_centers WHERE license_id = ?", (license_id,))
2358          row = cur.fetchone()
2359          conn.close()
2360          return dict(row) if row else {}
2361      except Exception:
2362          conn.close()
2363          return {}
```

### Functions not in scope (safe or out of scope)

- `get_ece_states` (`db.py:1917–1929`) — uses `row[0]` indexing, no dict(row); raw SQL without `?`. Safe.
- `batch_update_ece_geo` (`db.py:2261–2283`) — already has `_adapt_sql()` wrap at line 2274. Safe.
- `get_ece_centers` (`db.py:1863–1913`) — uses `pd.read_sql_query(query, conn, params=params)` with raw `?` placeholders. **Latent bug on Postgres** (psycopg2 doesn't accept `?` — pandas delegates placeholder handling to the backend). **Out of scope** for this phase, which is limited to `dict(row)` readers. Flagged in out-of-scope tracker.

### Loader entry point (`etl/load_ece_data.py`)

Per-state loader — no auto-download (unlike FQHC). Requires a pre-downloaded state file; column names vary by state vintage and are mapped via `COLUMN_MAP`.

```
python etl/load_ece_data.py --file data/raw/COLicensedChildCareReportForUpload_2026-02.csv --state CO
```

Main loop at `etl/load_ece_data.py:442`:

```
441          try:
442              db.upsert_ece(record)
443              loaded += 1
444          except Exception as e:
445              lid = record.get("license_id", "?")
446              print(f"    DB error for {lid}: {e}")
447              errors += 1
```

Then at line 458 it calls `db.get_ece_summary()` and prints counts. Same pattern as FQHC: on Postgres today (pre-fix) every row would print a visible DB error, and the summary would print all zeros.

### Table schema — 22 columns, identical on both backends

| # | Column | PG type | SQLite type |
|---:|---|---|---|
| 1 | `id` | integer (PK, auto-increment) | INTEGER PK |
| 2 | `license_id` | text (UNIQUE) | TEXT UNIQUE |
| 3 | `provider_name` | text (NOT NULL) | TEXT NOT NULL |
| 4 | `facility_type` | text | TEXT |
| 5 | `license_type` | text | TEXT |
| 6 | `license_status` | text | TEXT |
| 7 | `capacity` | integer | INTEGER |
| 8 | `ages_served` | text | TEXT |
| 9 | `accepts_subsidies` | **integer** | **INTEGER** |
| 10 | `star_rating` | real | REAL |
| 11 | `operator_name` | text | TEXT |
| 12 | `address` | text | TEXT |
| 13 | `city` | text | TEXT |
| 14 | `state` | text | TEXT |
| 15 | `zip_code` | text | TEXT |
| 16 | `county` | text | TEXT |
| 17 | `census_tract_id` | text | TEXT |
| 18 | `latitude` | real | REAL |
| 19 | `longitude` | real | REAL |
| 20 | `data_year` | integer | INTEGER |
| 21 | `data_source` | text | TEXT |
| 22 | `created_at` | timestamp | TIMESTAMP |

**Indexes / constraints (Postgres):** `ece_centers_pkey` on `id`, `ece_centers_license_id_key` UNIQUE on `license_id`, plus `idx_ece_state`, `idx_ece_status`, `idx_ece_capacity`.

No `updated_at` column — same limitation as `fqhc`. Verification must rely on direct probe, not timestamp bump.

## 2. Three-bug sweep on `upsert_ece`

| # | Bug | In code? | Location |
|---|---|---|---|
| 1 | Missing `_adapt_sql()` wrap | **YES** | `db.py:2256` |
| 2 | Unqualified COALESCE / preserve_if_null in ON CONFLICT | **N/A** | Plain `col=excluded.col` |
| 3 | `dict(row)` on psycopg2 return in readers | **YES (two places)** | `db.py:1948` (`get_ece_summary`), `db.py:2360` (`get_ece_by_id`) |

Same structure as `upsert_fqhc` — no internal swallow, no alternate-table fallback. Clean three-line fix surface.

## 3. Latent-bug sweep

### `accepts_subsidies = 1` (`db.py:1944`)

- Postgres column type: `integer` (`int4`) — confirmed via `information_schema.columns`.
- SQLite column type: `INTEGER` — confirmed via `PRAGMA table_info`.
- Current distinct stored values on both backends: `{None}` (the CO file does not populate this column).
- `WHERE accepts_subsidies = 1` works correctly on both. **No bug.** Same conclusion as `is_charter` / `is_active`.

### `license_status = 'Active'` (`db.py:1941`)

- `license_status` is TEXT on both backends; 'Active' is a string literal. No dialect issue.
- Current distinct values: `{None}` (CO file does not populate this column either — so `active_centers` in the summary will return 0 even after the fix). This is a **data-content observation**, not a bug.

### Reserved words / dialect quirks

ECE column names checked against Postgres reserved words (`year`, `type`, `program`, `status`, etc.):
- `facility_type`, `license_type`, `license_status`, `ages_served`, `data_year`, `data_source` — all prefixed/compound, **not reserved**.
- No ARRAY / JSON columns.
- No dialect-sensitive casts.

Nothing flagged.

### `get_ece_centers` DataFrame reader (out of scope)

`db.py:1909` runs `pd.read_sql_query(query, conn, params=params)` with `?` placeholders. On Postgres this will raise a psycopg2 syntax error, which the surrounding `try/except Exception` swallows → returns empty DataFrame. That's a legit latent bug, but **out of scope** for this phase per the user-specified constraint ("upsert_ece and its directly-related summary/reader functions only"). Tracked in out-of-scope § below.

## 4. Scope classification — 22 ECE columns

| Category | Columns | Count |
|---|---|---:|
| **Site/center identity** | `license_id`, `provider_name`, `operator_name`, `facility_type`, `license_type`, `license_status`, `address`, `city`, `state`, `zip_code`, `county`, `census_tract_id`, `latitude`, `longitude` | 14 |
| **Capacity** | `capacity`, `ages_served` | 2 |
| **Quality ratings** | `star_rating` (QRIS — populated only for states that publish it in bulk; currently CO only among states loaded) | 1 |
| **Subsidy/funding** | `accepts_subsidies` (CCDF voucher acceptance flag; `integer` but currently all-NULL in CO data) | 1 |
| **Financials / operator intelligence** | (none) | 0 |
| **Other** | `id`, `data_year`, `data_source`, `created_at` | 4 |

### One-paragraph scope summary

The `ece_centers` table is a **state-licensing registry** — one row per licensed child care facility, sourced from per-state open-data portals (`etl/load_ece_data.py` runs per file per state; no auto-download). It carries 14 identity/location columns, 2 capacity columns, a single QRIS `star_rating` column (populated only when the state's public file includes ratings — Colorado does; most states don't), a single `accepts_subsidies` integer flag (currently null in the loaded CO data), and 4 metadata columns. It **does not** include Head Start/Early Head Start partnership, Pre-K funding source, NAEYC/accreditation status, actual enrollment (vs. licensed capacity), teacher credentials or CLASS scores, program demographics, or any financial/revenue data. Currently only Colorado is loaded (4,556 rows), and adjacent pipelines (`headstart_programs` per CLAUDE.md) cover Head Start data separately but are not joined in.

## Known pipeline gap — Operator intelligence absent

Parallel to the FQHC UDS finding:

- **Head Start / Early Head Start partnership status:** Adjacent `headstart_programs` table exists per CLAUDE.md (PIR data), but not linked to `ece_centers`. A center's Head Start partnership isn't visible via a join on `license_id` — would need name/address matching to the PIR program data.
- **Pre-K funding source:** No column for state pre-K, Title I partnership, or federal pre-K funding.
- **Accreditation:** No NAEYC, NECPA, ACSI, or COA flags.
- **Enrollment:** Only `capacity` (licensed max). No actual enrollment, enrollment trend, or waitlist size.
- **Staffing / ratios / CLASS scores:** None.
- **Demographics of enrolled children:** No % subsidized, % DLL, % IEP/IFSP.
- **Financials:** No revenue, cost per child, tuition, 330 or ESSA funding.

### Why this matters for deal origination

For CDFI underwriting of ECE operators (facility expansion, staff housing, working capital), operator-level financial and utilization data is what underwriting hinges on — licensed capacity alone does not price a deal. Larger nonprofit operators can be partially covered via the 990 path (join on `operator_name` → EIN → `irs_990`), but for-profit operators (a significant share of ECE) have no 990, and smaller nonprofits (under $50K revenue) file 990-N only with no financials.

### What closing the gap would require (out of scope this session)

- **Head Start linkage**: name/address matching from `ece_centers` → `headstart_programs` (EIN-assisted where possible). Likely a `matched_headstart_program_id` column on `ece_centers` or a mapping table. New ETL.
- **QRIS backfill for non-CO states**: per-state scrape of state QRIS portals (TN, KY, OH, NC, etc. — see `etl/load_ece_data.py:50–54`). New per-state scripts.
- **Accreditation**: NAEYC publishes a provider search with no bulk download; scrape or API request required. Separate ETL.
- **Enrollment/financials**: requires direct-from-operator data (surveys, state P-EBT data where available, federal CACFP reimbursement data). Much bigger project.

This Postgres-compatibility fix (`upsert_ece`, `get_ece_summary`, `get_ece_by_id`) ships the **existing** state-licensing-directory pipeline correctly on both backends. It does not expand scope.

## 5. Current state — both backends in sync

| Metric | Postgres | SQLite |
|---|---:|---:|
| `ece_centers` table exists | yes | yes |
| Row count | 4,556 | 4,556 |
| States loaded | CO only | CO only |
| CO row count | 4,556 | 4,556 |
| `MAX(created_at)` | 2026-03-23 12:36:37 | 2026-03-23 12:36:37 |
| `accepts_subsidies` distinct | `{NULL}` | `{NULL}` |
| `license_status` distinct | `{NULL}` | `{NULL}` |

Identical state on both. Matches the FQHC pattern of "initial load went into SQLite, wholesale migrated to Postgres, bug wasn't discovered because no subsequent runs hit Postgres."

## 6. Proposed edits (not applied)

### Edit 1 — `upsert_ece`, `db.py:2256`

**Before:**
```python
    sql = f"""
        INSERT INTO ece_centers ({",".join(columns)})
        VALUES ({placeholders})
        ON CONFLICT(license_id) DO UPDATE SET {update_clause}
    """
    cur.execute(sql, values)
    conn.commit()
    conn.close()
```

**After:**
```python
    sql = f"""
        INSERT INTO ece_centers ({",".join(columns)})
        VALUES ({placeholders})
        ON CONFLICT(license_id) DO UPDATE SET {update_clause}
    """
    cur.execute(_adapt_sql(sql), values)
    conn.commit()
    conn.close()
```

### Edit 2 — `get_ece_summary`, `db.py:1948`

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

### Edit 3 — `get_ece_by_id`, `db.py:2357` and `2360`

**Before:**
```python
    try:
        cur.execute("SELECT * FROM ece_centers WHERE license_id = ?", (license_id,))
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
        cur.execute(_adapt_sql("SELECT * FROM ece_centers WHERE license_id = ?"), (license_id,))
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

### Scope reminder

- Do not fix `accepts_subsidies = 1` literal at line 1944 — it's not a bug.
- Do not touch `license_status = 'Active'` at line 1941 — not a bug (string comparison).
- Do not touch the `get_ece_centers` DataFrame reader — out of scope this phase.
- Do not touch `batch_update_ece_geo` — already correct.
- Do not touch bare `except Exception` handlers — out of scope.

## 7. Entry-point command + expected row count delta

**Command (use existing CO file already on disk):**

```
python etl/load_ece_data.py --file data/raw/COLicensedChildCareReportForUpload_2026-02.csv --state CO
```

**Expected delta:** The file is dated 2026-02. Last load was 2026-03-23. Unless Colorado re-published the file in the interim, the row count should stay at 4,556 (all ON CONFLICT-updates) or change by a very small amount. **True success signal:** zero `DB error for ...` lines in stdout, and `get_ece_summary()` returning real counts (not `{}`).

**Direct-probe verification (no loader required):**

```python
db.upsert_ece({
    "license_id": "TEST_PROBE_ECE_0001",
    "provider_name": "Test Probe ECE",
    "state": "CO",
    "capacity": 50,
    "data_year": 2024,
    "data_source": "diagnostic probe",
})
db.get_ece_by_id("TEST_PROBE_ECE_0001")   # should return non-empty dict on both backends
# cleanup: DELETE FROM ece_centers WHERE license_id = 'TEST_PROBE_ECE_0001'
```

Run probe first (step 7a) to prove the fix independent of the CSV content, then run the loader (step 7b) for end-to-end coverage.

## Out-of-scope items (tracked, not fixed)

- `get_ece_centers` DataFrame reader at `db.py:1864–1913` uses raw `?` placeholders with `pd.read_sql_query`. Latent bug on Postgres — pandas passes placeholders to the backend verbatim, psycopg2 rejects `?`. Would need `_adapt_sql` wrap on the query string or a switch to `%s`. **Not this phase.**
- 11+ other `upsert_*` functions in `db.py` still carry the same `_adapt_sql` miss.
- No `updated_at` column on `ece_centers` (same as `fqhc`).
- `latitude`/`longitude` as `real`/float4 in Postgres — same precision limitation noted in FQHC fix, not a correctness bug.
- Silent `except Exception: ...` handlers — intentional swallowing masks real errors.
- Operator intelligence gap (see § Known pipeline gap).

## Data scope mismatch

- The current `ece_centers` table contains **Colorado data only** (4,556 rows), loaded from a state licensing CSV (`COLicensedChildCareReportForUpload_2026-02.csv`).
- **Georgia — the primary target market for CDFI lending at the user's employer — is not represented in this pipeline.**
- Adding Georgia DECAL (Department of Early Care and Learning) data requires a separate loader pass using Georgia's licensing file format. The `load_ece_data.py` entry point is per-state and takes the CSV path + state code explicitly; there is no auto-download or national aggregator.
- **This Postgres-compatibility fix does not address the data-scope question.** It ships the existing pipeline correctly against both backends for whatever data happens to be loaded. Expanding coverage to GA (or any other state) is a separate ETL task: source the DECAL file, map its columns to the `ece_centers` schema in `etl/load_ece_data.py`, and run the loader against the new file.

## Known pipeline gap — operator intelligence

- The `ece_centers` table is **licensing-directory-only**: center identity, address, licensed capacity, licensing status. It captures *who is legally allowed to operate* — not *how well they're operating* or *how they're financed*.
- It does **not** include the data needed for CDFI underwriting of ECE centers:
  - **Actual enrollment** (vs. licensed capacity) — utilization rate is the core demand signal
  - **Subsidy participation** — CCDBG, state voucher programs, Head Start partnership, Pre-K funding source (revenue mix + payer stability)
  - **Quality ratings** — state QRIS star ratings, NAEYC accreditation, CLASS scores (quality signal + differentiation)
  - **Staffing ratios** — teacher:child ratios, credentialed staff % (cost structure + compliance)
  - **Child demographics** — ages served, income mix, special needs (market fit + subsidy eligibility)
  - **Financials** — revenue, expenses, operating margin, balance sheet (underwriting basis)
- **Unlike FQHC, where UDS provides a single national operator-intelligence source** (with a ~9-month lag but consistent schema across all 1,400+ grantees), **ECE has no single national source.** QRIS data lives in state-specific systems. Subsidy data lives in state CCDBG admin systems. Head Start program data lives in the federal PIR (already loaded into `headstart_programs` as a separate table, not joined to `ece_centers`). Pre-K funding lives in state DOE systems. Financials for non-Head-Start, non-nonprofit centers are generally not publicly available at all.
- **Adding operator intelligence to ECE is a multi-source project per state, substantially larger in scope than adding UDS for FQHC.** A minimum-viable version for Georgia would require: GA DECAL QRIS (Quality Rated) scores, GA CAPS subsidy participation, Bright from the Start Pre-K participation, and a join to `headstart_programs` for Head Start operators — four sources, all state-specific.
- **This fix does not attempt to close that gap.** It ships the licensing directory correctly against both backends; operator intelligence is a Phase 7+ scope decision.
