---
title: Postgres readiness inventory — SQL function + silent exception scan
date: 2026-04-22
scope: All Python files under repo root — db.py, etl/, api/, utils/, validate.py, archive/
status: Inventory only — no code edits. Reference doc for sizing the full Postgres migration.
---

# Postgres readiness inventory — 2026-04-22

## Why this doc exists

Every prior Postgres-compat session has surfaced a new bug class:
adapt_sql raw-?, chained execute, INSERT OR IGNORE, dict(row),
cur.lastrowid. Each was discovered from a single user-visible
symptom, not from a systematic sweep. This doc closes the
remaining two suspected bug classes (SQLite-only SQL functions,
silent exception handlers) and aggregates **every known**
Postgres-compat bug class into one list, so the next session can
size the full surface instead of rediscovering it.

Read this **before** starting any new B1/B2/B4/B5/B6 fix session.

---

## Bug class 1 — SQLite-only SQL functions

### Grep patterns run

Against `*.py` under the repo root:

- `\b(DATETIME|STRFTIME|JULIANDAY)\b` (case-insensitive) — SQLite date functions
- `\b(GROUP_CONCAT|IFNULL|INSTR)\b` (case-insensitive) — SQLite-named aggregates / null helpers
- `\bSUBSTR\s*\(` — string slicing (Postgres also supports SUBSTR; flag-only)
- `'now'` — `datetime('now', ...)` / `date('now', ...)` literals
- `\bdate\s*\(\s*'now'` — explicit `date('now', ...)` form
- `\bCAST\s*\(` — explicit casts
- `(AUTOINCREMENT|PRAGMA)` — SQLite-specific schema and config
- `\bsqlite3\b` — direct sqlite3 module usage

### Findings

| Function searched | Hits | Real-bug hits |
|---|---:|---:|
| `DATETIME` / `datetime('now', ...)` | 17 (mostly Python `datetime` module imports) | 1 SQL site |
| `STRFTIME` | 1 | 1 (already guarded) |
| `JULIANDAY` | 0 | 0 |
| `GROUP_CONCAT` | 0 | 0 |
| `IFNULL` | 0 | 0 |
| `INSTR` | 0 | 0 |
| `SUBSTR(...)` | 2 | 0 (Postgres supports SUBSTR; both sites are inside hardcoded sqlite3 connections, separate bug — see §1.2) |
| `'now'` (SQL literal) | 3 | 3 (1 unguarded, 2 guarded) |
| `CAST(...)` | 0 | 0 |
| `AUTOINCREMENT` | 30+ | 0 (all rewritten to `SERIAL` by `adapt_sql()`) |
| `PRAGMA` | 0 | 0 |
| `||` (string concat in SQL) | not exhaustively grepped — Postgres supports this; flag-only per prompt | unknown but acceptable |
| Direct `sqlite3.` usage | 5 files | 3 real bugs (see §1.2) |

### 1.1 — Full site table: SQLite-only SQL functions

| # | file:line | Function | SQL context | Wrapped in `adapt_sql`? | Target table | Exposure |
|---:|---|---|---|---|---|---|
| F1 | `validate.py:372` | `datetime('now', '-7 days')` | `SELECT COUNT(*) FROM data_loads WHERE status = 'error' AND started_at > datetime('now', '-7 days')` (passed to `_scalar()` which wraps with `db.adapt_sql()` — but adapt_sql does pure substitution, does NOT rewrite `datetime('now', ...)`) | Yes (irrelevant — adapt_sql doesn't touch this function) | `data_loads` | **ACTIVE** |
| F2 | `db.py:3405` | `date('now', '-{N} days')` | `conditions.append(f"rate_date >= date('now', '-{int(days)} days')")` — only the `else` branch of `if _IS_POSTGRES` (Postgres branch at :3402 uses `CURRENT_DATE - INTERVAL`) | N/A — branched | `market_rates` | **DEAD** on Postgres |
| F3 | `db.py:3797` | `strftime('%Y-%m', date('now', '-{N} months'))` | `conditions.append(f"period >= strftime(...)")` — only the `else` branch (Postgres branch at :3794 uses `TO_CHAR(CURRENT_DATE - INTERVAL ..., 'YYYY-MM')`) | N/A — branched | `bls_unemployment` | **DEAD** on Postgres |

**Real Postgres-blocking sites: 1 (`validate.py:372`).** The two `db.py` sites are correctly branched on `_IS_POSTGRES`.

### 1.2 — Direct `sqlite3` module usage (separate sub-bug)

`get_connection()` is the only sanctioned site that imports `sqlite3` for backend selection. Every other `import sqlite3` in the codebase bypasses get_connection, hardcodes the SQLite file path, and ignores `DATABASE_URL`. On a Postgres-configured environment these sites either (a) silently read a stale local SQLite file, or (b) crash because the file isn't present.

| # | file:line | What it does | Exposure |
|---:|---|---|---|
| S1 | `db.py:12, 44, 45` | `import sqlite3` + `sqlite3.connect()` + `conn.row_factory = sqlite3.Row` inside `get_connection()` (else branch) | **ALLOWED** — backend selection |
| S2 | `db.py:4206-4209` | `get_school_tearsheet_data()`: `import sqlite3 as _sqlite3; conn.row_factory = _sqlite3.Row` — applied **unconditionally** to whatever `get_connection()` returns | **DANGEROUS** on Postgres: psycopg2 connections do not accept `row_factory = sqlite3.Row` (silently no-ops or raises depending on attribute presence; subsequent `dict(row)` in the function then receives a tuple and raises TypeError, swallowed by the per-section silent excepts at `db.py:4231/4255/4270/4282/4296/4305/4354/4372`). End result on Postgres: every section returns its empty default → tearsheet PDF is blank. **DEAD** for now (no current FastAPI route reaches it; the rendering pipeline is archived) but the bug is real if revived. |
| S3 | `etl/fetch_bls_unemployment.py:394-404` | Inside `--all-counties` branch: `import sqlite3; sqlite3.connect(DB_PATH)` to read `census_tracts`, bypassing `db.get_connection()` | **ACTIVE on `--all-counties`**, **LATENT** otherwise. Reads from the stale local SQLite even when DATABASE_URL points at Postgres. Returns wrong/empty FIPS list silently. |
| S4 | `etl/fetch_bls_qcew.py:391-401` | Same pattern as S3, `--all-counties` branch | **ACTIVE on `--all-counties`**, **LATENT** otherwise. Same silent-stale-data bug. |
| S5 | `etl/load_census_tracts.py:327` | `import sqlite3` (local) but never calls `sqlite3.connect`; uses `db.get_connection()` | **DEAD** — vestigial import. |
| S6 | `etl/load_sample_data.py:14` | `import sqlite3` at module top but never calls `sqlite3.connect`; uses `db.*` helpers | **DEAD** — vestigial import. |
| S7 | `etl/migrate_sqlite_to_postgres.py:21, 142, 143` | `sqlite3.connect(SQLITE_PATH)` is the migration source by design | **ALLOWED** — that's the point of the script. |

**Real Postgres-blocking sites: 3 (S2, S3, S4).** S5/S6 are vestigial imports that should be removed during cleanup but cause no runtime issue.

### 1.3 — Classification summary (bug class 1)

| Site | Classification |
|---|---|
| F1 `validate.py:372` | **ACTIVE** |
| F2 `db.py:3405` | DEAD on Postgres (branched) |
| F3 `db.py:3797` | DEAD on Postgres (branched) |
| S2 `db.py:4206` (get_school_tearsheet_data) | LATENT (no live caller; DANGEROUS if revived) |
| S3 `etl/fetch_bls_unemployment.py:394` (--all-counties) | ACTIVE on the `--all-counties` flag |
| S4 `etl/fetch_bls_qcew.py:391` (--all-counties) | ACTIVE on the `--all-counties` flag |
| S5 `etl/load_census_tracts.py:327` | DEAD (vestigial import) |
| S6 `etl/load_sample_data.py:14` | DEAD (vestigial import) |

| Exposure | Count |
|---|---:|
| ACTIVE | 3 |
| LATENT | 1 |
| DEAD (real) | 2 |
| DEAD (vestigial-import) | 2 |
| ALLOWED (backend selection / migration source) | 2 |

---

## Bug class 2 — Silent exception handler inventory

### Grep patterns run

Against `*.py` under the repo root:

- `except\s+(Exception)?\s*:\s*(pass|continue)` (one-line form) — 0 matches in this repo (the repo always wraps the body on a separate line)
- `except\s+Exception(\s+as\s+\w+)?\s*:` with `-A 2` context — **141 matches across 42 files**
- `^\s*except\s*:\s*$` (bare `except:`) — **0 matches**

### High-level distribution

| File / dir | Count | Predominant body | Risk profile |
|---|---:|---|---|
| `db.py` | 64 | `df = pd.DataFrame()`, `result = {}`, `states = []`, `return {}`, `continue` | **High — silent swallow with no logging at every read path.** This is the primary source of the "every Postgres bug stays hidden until a user notices empty data" pattern. |
| `validate.py` | 2 | prints + `sys.exit(1)` / `fail(...)` | LOW — explicit error path |
| `utils/geo.py` | 2 | `return {}`, `pass` | RISKY (geocode helper) |
| `utils/pdf_extractor.py` | 2 | `return ""`, `return {}` | LOW — best-effort PDF extraction |
| `archive/app.py` | 2 | `pass`, `return []` | DEAD |
| `etl/*.py` (39 files combined) | 69 | majority print `f"... {e}"` and either `continue`, `errors += 1`, `raise`, or `sys.exit(1)` | LOW-RISK — almost all log the underlying exception (the ETL pattern is consistent) |
| `api/`, `api/routers/` | **0** | n/a | n/a — all error paths route through `db.py` |

### 2.1 — DANGEROUS sites (silent swallow + caller relies on the value)

These are the handlers that have hidden Postgres-compat bugs in prior sessions and will continue to hide them. All in `db.py`. None log the exception; all return a structurally valid empty default that callers cannot distinguish from "no data found."

| # | file:line | Enclosing function | Try body (abbreviated) | On except | Cross-ref to known bug class |
|---:|---|---|---|---|---|
| H1 | `db.py:1446-1447` | `get_schools(...)` (for-table fallback loop) | `pd.read_sql_query(query, conn, params)` against `schools` then `charter_schools` | `continue` (no log) | Hides any psycopg2 exception in `get_schools` reads — the schools/charter_schools dual-table fallback was the masking layer for several A3 drift symptoms. |
| H2 | `db.py:1472-1473` | `get_school_by_id(...)` | `cur.execute(... WHERE id = ?)` then `dict(row)` | `continue` (no log) | **Masks A1 silent-404.** psycopg2 raises on raw `?`; `dict(row)` would raise on tuple cursor; both swallowed → empty dict → API returns 404. |
| H3 | `db.py:1494-1495` | `get_school_states()` | `cur.execute("SELECT DISTINCT state ...")` then row[0] iteration | `continue` (no log) | Masks raw-? on the schools table (no `adapt_sql()`). Returns `[]` on Postgres → state filter dropdown is empty. |
| H4 | `db.py:1528-1529` | `get_school_summary(...)` | `cur.execute(SELECT COUNT/SUM ...)` then `dict(zip(cols, row))` | `continue` (no log) | Masks raw-? + dict(zip) bugs. Returns `{}` → summary cards blank. |
| H5 | `db.py:2049-2050` | `upsert_school(...)` (for-table fallback loop) | `cur.execute(adapt_sql(INSERT ... ON CONFLICT ...))` | `continue` (no log) | Hides any psycopg2 error during ingest; the next iteration tries `charter_schools` (which doesn't exist on Postgres). End state: silent loss of upserted rows. **Likely contributor to A3 drift.** |
| H6 | `db.py:2311-2313` | `get_fqhc_by_id(bhcmis_id)` | `cur.execute(adapt_sql("SELECT * FROM fqhc WHERE bhcmis_id = ?"))` then `dict(zip(cols, row))` | `return {}` | **Masks A1 silent-404 for FQHC.** dict(zip) is fine; the suspect is any future schema/column drift or a real psycopg2 exception. |
| H7 | `db.py:2330-2332` | `get_ece_by_id(license_id)` | analogous to H6 | `return {}` | Same silent-404 risk for ECE. |
| H8 | `db.py:2344-2346` | `get_nmtc_project_by_id(cdfi_project_id)` | `cur.execute("SELECT * FROM nmtc_projects WHERE cdfi_project_id = ?")` (raw `?`, no `adapt_sql`!) then `dict(row)` | `return {}` | **A1 silent-404 confirmed.** Both raw-? and dict(row) bug classes here. Already tracked in A1. |
| H9 | `db.py:2387-2412` (4 handlers) | `get_nearby_facilities(lat, lon, ...)` | calls to `get_schools`, `get_fqhc`, `get_ece_centers`, `get_nmtc_projects` + `filter_by_radius` | `pass` (no log) | Hides every facility type independently. On Postgres, all four can fail silently → "Nothing nearby" with no error. Masking surface for nearly the entire dashboard map. |
| H10 | `db.py:2451-2453` | `batch_update_school_census_tracts(records)` (for-table loop) | `cur.executemany(adapt_sql(UPDATE ... SET census_tract_id = ?))` | `continue` (no log) | Hides psycopg2 errors during census-tract assignment; the second iteration hits non-existent `charter_schools`. **Possible contributor to A3 drift on schools.census_tract_id coverage.** |
| H11 | `db.py:3057-3058` | `get_user_notes(entity_type, entity_id)` | `cur.execute("SELECT ...")` then `[dict(row) for row in cur.fetchall()]` (raw `?`, no `adapt_sql`!) | `notes = []` | **Masks A1 silent-empty for notes.** Both raw-? and dict(row) bug classes. |
| H12 | `db.py:3109-3110` | `get_bookmarks()` | `cur.execute("SELECT * FROM user_bookmarks ORDER BY ...")` then `[dict(row) for row in cur.fetchall()]` | `bookmarks = []` | **A1+B6 confirmed: this is the surprise from today's session.** dict(row) on psycopg2 tuples raises TypeError, swallowed → sidebar empty. |
| H13 | `db.py:3153-3154` | `is_bookmarked(entity_type, entity_id)` | `cur.execute(adapt_sql("SELECT 1 ..."))` then `cur.fetchone() is not None` | `found = False` | LOW — fixed in `d7855ee` for Postgres; left silent because `False` is a valid no-bookmark answer. Still hides any future schema break. |
| H14 | `db.py:3349-3350` | `_compute_for_ein(...)` (audit ratios sub-block) | `_cur.execute("SELECT ... FROM financial_ratios WHERE ein = ? AND fiscal_year = ?")` (raw `?`, no `adapt_sql`!) | `pass` | Optional audit-quality lookup; `pass` keeps `acid_audit/cl_audit/has_audit` at None → 990-only ratios are still computed. Hides raw-? on `financial_ratios`. Tolerable behaviorally; non-tolerable for diagnosis. |
| H15 | `db.py:4231-4372` (8 handlers, get_school_tearsheet_data) | per-section reads on enrollment, accountability, state averages, cpf scores, financials_990, financial_ratios, nearby_schools, census_tract | various `result["section"] = []` / `{}` | DEAD per S2 above (`row_factory = _sqlite3.Row` aborts every section on Postgres anyway) but each section's silent default is itself a masking layer. |

**DANGEROUS count: ~20 distinct handler sites, all in `db.py`, that are actively masking known or class-equivalent Postgres bugs.**

### 2.2 — RISKY sites (silent swallow but lower blast radius)

These swallow without logging but the function is either (a) a list/df helper where `[]`/`pd.DataFrame()` is a plausible "no data" answer the caller already handles, or (b) a state-list builder for a dropdown that degrades gracefully.

The remaining ~40 `except Exception: df = pd.DataFrame()` and `states = []` patterns in `db.py` (lines 1812, 1826, 1853, 1910, 1926, 1953, 1976, 2357, 2626, 2645, 2660, 2704, 2760, 2776, 2820, 2836, 2919, 2957, 3019, 3036, 3201, 3267, 3426, 3454, 3483, 3519, 3555, 3596, 3638, 3690, 3714, 3762, 3813, 3863, 3923, 3986, 4070, 4084, 4109, 4167, 4184) are **RISKY**. None log. Each is a `get_*` reader for a specific data source. On Postgres failure, the dashboard renders "No data" without distinguishing infra-failure from genuine emptiness. They are the substrate that has made every prior bug "look like a data loading problem" rather than "look like a Postgres compat bug."

### 2.3 — LOW-RISK sites

| Pattern | Count | Why low-risk |
|---|---:|---|
| `db.py:69-75` `_try_exec()` SAVEPOINT pattern | 2 | Intentional — isolating ALTER TABLE failures on existing schemas. SAVEPOINT properly contains the exception. |
| `db.py:166-167` charter_schools migration except | 1 | Intentional — `pass  # charter_schools doesn't exist, that's fine` (commented). |
| ETL `except Exception as e: print(f"... {e}")` then `continue`/`errors += 1`/`raise` | ~60 | Logged with the underlying exception text; subsequent error count printed in summary. Standard ETL row-by-row resilience. |
| ETL `except Exception as e: db.log_load_finish(run_id, error=str(e)); raise` | ~15 | Logged + re-raised. Correct pattern. |
| `validate.py:426/433` | 2 | Logged + fails the validation. |

### 2.4 — ACCEPTABLE sites (intentional flow control)

| Pattern | Count |
|---|---:|
| ETL `except Exception: break` (e.g. `fetch_990_irs.py:214`, paginate-until-empty) | 2 |
| `etl/geocode_facilities.py:128` retry-with-backoff | 1 |

### 2.5 — Severity summary (bug class 2)

| Severity | Count | Notes |
|---|---:|---|
| DANGEROUS | ~20 | All in `db.py`; all silently mask raw-?, dict(row), or upsert failures on Postgres |
| RISKY | ~40 | All in `db.py`; silent `df = pd.DataFrame()` / `states = []` readers |
| LOW-RISK | ~80 | ETL pattern with `print(f"... {e}")` + continue/raise |
| ACCEPTABLE | 3 | flow control |
| **Total** | **141** | across 42 files |

Bare `except:` count: **0**. The codebase consistently uses `except Exception` rather than bare except.

### 2.6 — Cross-reference: silent handlers masking known bug classes

| Handler site | Known bug class masked | Tracked under |
|---|---|---|
| H2 `get_school_by_id` | raw-? + dict(row) (silent-404) | A1 |
| H6 `get_fqhc_by_id` | raw-? + dict(row) (silent-404) | A1 |
| H7 `get_ece_by_id` | raw-? + dict(row) (silent-404) | A1 |
| H8 `get_nmtc_project_by_id` | raw-? + dict(row) (silent-404) | A1 |
| H11 `get_user_notes` | raw-? + dict(row) (silent-empty) | A1 / B6 |
| H12 `get_bookmarks` | dict(row) (silent-empty) | A1 / B6 |
| H3 `get_school_states` | raw-? on schools | B2 |
| H4 `get_school_summary` | raw-? on schools | B2 |
| H5 `upsert_school` (for-table fallback) | raw-? + non-existent charter_schools on Postgres | A4 / B1 / **A3 drift** |
| H10 `batch_update_school_census_tracts` (for-table fallback) | raw-? + non-existent charter_schools on Postgres | A4 / **A3 drift** |
| H1 `get_schools` (for-table fallback) | psycopg2 exception → returns DataFrame() and tries charter_schools | A4 / A3 drift |
| H14 `_compute_for_ein` audit lookup | raw-? on financial_ratios | B1 |
| H15 (×8) `get_school_tearsheet_data` per-section | dict(row), raw-?, plus the `row_factory = sqlite3.Row` poisoning at S2 | LATENT (no live caller) |

**Of ~20 DANGEROUS handlers, 13 directly mask currently-tracked Postgres bug classes.** The remaining ~7 are equivalent shape (silent-404 or silent-empty on a get_*_by_id / list reader) and would mask the same bug classes if/when they surface.

---

## cur.lastrowid inventory (5-minute side-task per prompt)

`cursor.lastrowid` is a SQLite/MySQL convention. psycopg2's `cursor.lastrowid` returns `None` for `INSERT` statements unless the table has an OID, which Postgres tables no longer have by default. The Postgres-correct approach is `INSERT ... RETURNING id` followed by `cur.fetchone()[0]`.

### Sites

| # | file:line | Enclosing function | Used as | Caller-visible impact on Postgres | Exposure |
|---:|---|---|---|---|---|
| L1 | `db.py:1292` | `log_load_start(pipeline)` | `run_id = cur.lastrowid` → returned to ETL caller, passed back to `log_load_finish(run_id, ...)` | `run_id = None`. `log_load_finish` then runs `UPDATE data_loads SET ... WHERE id = NULL`, which matches zero rows. **Pipeline run logging is silently broken on Postgres** — every pipeline appears to never finish (status stays "running"), and `validate.py`'s "no errors in 7 days" check is structurally meaningless. | **ACTIVE** — every ETL script calls this on entry. |
| L2 | `db.py:3071` | `save_user_note(entity_type, entity_id, note_text)` | `note_id = cur.lastrowid` → returned to FastAPI caller | Returns `None` to `api/routers/notes.py:48`; dashboard receives `{"note_id": null}`. Prevents subsequent edit/delete by id. Already tracked in B6. | **ACTIVE** |
| L3 | `db.py:3174` | `save_document(record)` | `doc_id = cur.lastrowid` → returned to FastAPI caller | Returns `None`. Already tracked in B6. | **ACTIVE** |

**Total cur.lastrowid sites: 3** (under the 5-site flag-and-stop threshold). One new site found beyond what B6 currently lists: **L1 `log_load_start` → silent breakage of `data_loads` pipeline run logging.** This is a meaningful new finding — it means every pipeline run since Postgres became active has been logged as "running" forever, and `validate.py`'s data_loads sanity check (`SELECT COUNT(*) FROM data_loads WHERE status = 'error' AND started_at > datetime('now', '-7 days')`) would also misreport even if its `datetime('now', ...)` syntax error were fixed first.

Recommend adding L1 to B6's scope; same fix shape (`INSERT ... RETURNING id`).

---

## Postgres readiness — aggregated bug class list

Single source of truth for every known Postgres-compat bug class
in this repo. Counts marked `~` are approximate within ±2-3.

| # | Bug class | Inventoried in | Total sites | Fixed | Remaining | Notes |
|---:|---|---|---:|---:|---:|---|
| 1 | Raw `?` placeholders not routed through `adapt_sql` | `chained_execute_inventory_2026-04-21.md` (B2 cross-ref) | ~28 | ~7 | ~21 | 7 FastAPI loud-crash (A1), 2 FastAPI silent-404 (A1), 6 active ETL blockers (B2), 8 dead/archived. Mandate fresh re-grep at session start — multi-line miss patterns. |
| 2 | `INSERT OR IGNORE` (SQLite-only conflict syntax) | `insert_or_ignore_inventory_2026-04-22.md` | 3 | 1 (`save_bookmark` in d7855ee) | 2 (1 needs schema work — `state_programs`; 1 already correctly branched in `load_cra_lending.py`) | 0 INSERT OR REPLACE sites. |
| 3 | Chained `cur.execute(...).fetchone()` (psycopg2 returns None) | `chained_execute_inventory_2026-04-21.md` | 14 | 6 | 8 | 1 ACTIVE (`compute_financial_ratios.py --limit`), 7 LATENT, 1 of those is CHAIN+PLACEHOLDER (rides along with adapt_sql). |
| 4 | `cur.lastrowid` (psycopg2 returns None for INSERTs) | **this doc** | 3 | 0 | 3 | L1 `log_load_start` (NEW — pipeline run logging silently broken), L2 `save_user_note`, L3 `save_document`. Same fix shape: `INSERT ... RETURNING id`. |
| 5 | `dict(row)` / sqlite3.Row assumption on psycopg2 cursors | partial sweep — flagged in B6 after `get_bookmarks` surprise | unknown (≥6 confirmed: `get_school_by_id`, `get_fqhc_by_id`, `get_ece_by_id`, `get_nmtc_project_by_id`, `get_user_notes`, `get_bookmarks`; tearsheet section adds 8 more if revived) | 0 | ≥6, **sweep incomplete** | Decision needed: switch `get_connection()` to `psycopg2.extras.RealDictCursor` (large blast radius across every SELECT path) vs. add a `row_to_dict(cur, row)` helper site-by-site. Until decided, every new `get_*_by_id` ships with the bug. |
| 6 | SQLite-only SQL functions | **this doc** | 3 SQL function sites + 3 direct-sqlite3 sites = 6 | 0 | 6 (1 ACTIVE: `validate.py:372`; 2 ACTIVE-on-flag: `--all-counties` in BLS scripts; 1 LATENT: tearsheet `row_factory`; 2 vestigial-import dead) | Real fixable Postgres-blocking sites: 4 (F1, S2, S3, S4). |
| 7 | Silent exception handlers in `db.py` | **this doc** | 141 total `except Exception` sites; ~20 DANGEROUS in `db.py`; ~40 RISKY readers | 0 | All — but most are appropriate as-is. Recommended action: add logging to ~20 DANGEROUS sites, leave the rest with `logger.exception(...)` decoration. | Architectural — each DANGEROUS site needs individual judgment (log-and-continue vs log-and-raise vs remove handler). This bug class is what has hidden bug classes 1, 4, 5 from every prior verification attempt. **Fix concurrently with or before any further B1/B2/B5/B6 work.** |
| 8 | Schema drift between SQLite and Postgres | tracked, uninventoried | unknown | n/a | unknown | A4 (vestigial `charter_schools` table), the for-table fallback loops in db.py that try `["schools", "charter_schools"]` on Postgres where the second never exists — masked by H1/H5/H10 silent excepts. Suspected contributor to A3 row-count drift. |
| 9 | Data drift between SQLite and Postgres | tracked, uninventoried | known on ≥6 tables (`irs_990` 4126/4123, `schools` 5190/5187, Atlanta 157/150, Fulton 191/160, `cdes` 11/7, `fqhc` 43/42) | 0 | Needs per-table audit + canonical-source decision + resync | Tracked as A3. Cannot be sized until bug classes 1, 4, 5, 7 are closed — current loaders silently lose rows under Postgres failure. |

### Cross-cutting observation

Bug classes 1, 2, 3, 4, 5, 6 are all individually small (3-28 sites
each, ~70 sites combined). Bug class 7 (silent exception handlers)
is what makes them feel infinite — every fix attempt finds another
class because the silent handlers prevent verification from ever
saying "no, that's still broken."

Closing class 7 first (or concurrently with class 1) collapses the
discovery cost of every other class. Without it, sweep N+1 will
discover bug class N+1 the same way every prior sweep has.

---

## Estimated Postgres readiness

Honest estimate, not optimistic. Assumes the user does the work
(small fixed cost per session, ~1 active context per fix area) and
that the sweep order is **silent handlers first**, then everything
else.

### Effort to close each bug class

| # | Bug class | Remaining effort estimate | Confidence |
|---:|---|---|---|
| 1 | Raw `?` (~21 remaining) | 4-6 hours across 2-3 sessions. Each site is mechanical `cur.execute(adapt_sql(...))`. Per-file commits. | High — proven template. |
| 2 | INSERT OR IGNORE (~2 remaining) | ~2-2.5 hours (per `insert_or_ignore_inventory_2026-04-22.md`). Site 1 needs schema work for `state_programs`. | High. |
| 3 | Chained execute (~8 remaining) | ~2 hours. Mostly summary-block prints in LATENT scripts. | High. |
| 4 | cur.lastrowid (3 sites) | ~1 hour total. Each site: rewrite to `INSERT ... RETURNING id; row = cur.fetchone(); id = row[0] if row else None`. L1 (`log_load_start`) needs verification that ETL callers tolerate the change. | High. |
| 5 | dict(row) | **~4-8 hours, blocked by an architectural decision.** If the decision is `RealDictCursor`, sweep every SELECT in db.py for `row[0]`/`row[1]` index access (~50+ sites) and rewrite to `row['col']`. If the decision is `row_to_dict` helper, ~6+ confirmed sites + a fresh full sweep (estimated ~12-20 sites total — every list-builder using `[dict(row) for row in cur.fetchall()]`). Plus tearsheet revival decision. | Medium-low (depends on decision). |
| 6 | SQLite-only SQL functions (4 real Postgres-blocking) | ~1.5 hours. F1 (validate.py): rewrite to backend-branched `_IS_POSTGRES`. S2 (tearsheet): replace `row_factory = sqlite3.Row` with backend-aware row→dict (folded into class 5). S3/S4 (--all-counties): route through `db.get_connection()` instead of hardcoded `sqlite3.connect`. | High. |
| 7 | Silent exception handlers (~20 DANGEROUS, ~40 RISKY) | **~6-10 hours** if the goal is "log all DANGEROUS, leave RISKY readers as-is with logger.exception decoration." Per-site judgment. Heaviest cost is structural deliberation, not code edits. | Medium — this is the class that controls the discovery cost of every other class. |
| 8 | Schema drift (charter_schools removal etc) | ~2 hours — A4 in pipeline doc. Drop charter_schools, remove for-table fallback loops. Cannot be done before class 7 (silent excepts) is fixed, otherwise removal will break silently. | Medium. |
| 9 | Data drift between backends | **~6-12 hours of audit work** (~30 minutes per drifted table × 6+ tables) plus reconciliation runs. Cannot be sized below this until classes 1, 4, 5, 7 are closed — current loader behavior under Postgres is unknown. | Low — depends on findings. |

### Total estimated effort

**~28-44 hours of focused work**, distributed across **8-12 sessions**, to close every known Postgres-compat bug class.

Plus **unknown** for class 9 (data drift) which can't be sized until the upstream bugs are fixed. Realistic ceiling: another 1-2 sessions of audit + 1-3 sessions of resync runs.

### Recommended sequence

1. **Bug class 7 first** (silent handlers — at least the ~20 DANGEROUS ones in `db.py`). Without this, every subsequent sweep continues to under-count by the bug-class-of-the-week pattern.
2. Bug class 1 (adapt_sql raw-?). Largest mechanical surface; templated.
3. Bug classes 4 + 6 together (cur.lastrowid + SQLite-only functions). Both are small, mechanical, share the `_IS_POSTGRES`-branching idiom.
4. Bug class 5 (dict(row)) — needs the architectural decision first. Recommend pairing the decision with the get_bookmarks / get_user_notes fixes in B6.
5. Bug classes 2 + 3 (INSERT OR IGNORE + chained execute). Mostly LATENT remaining sites; close at convenience.
6. Bug class 8 (schema drift / charter_schools removal). Only after class 7 is closed.
7. Bug class 9 (data drift / A3) — after everything above.

### Convergence test

A session is "converged" when:
- A full re-grep finds zero new sites in the bug classes already closed.
- `validate.py` runs cleanly on Postgres.
- `python etl/run_pipeline.py --dry-run` runs cleanly on Postgres with `data_loads` rows correctly transitioning `running → success/error`.
- A spot-check of 3 dashboard endpoints (notes, bookmarks, school detail) returns non-empty data on Postgres.

Until those four conditions hold, "Postgres is ready" is a forecast, not a fact.

---

## Scope-discipline notes

Per prompt:
- No code edits in this session.
- No fixes for the obvious dangerous sites (H8 `get_nmtc_project_by_id` raw-? + dict(row) + silent return; L1 `log_load_start` lastrowid → broken pipeline logging) — flagged here for the next session.
- `cur.lastrowid` inventory came in at 3 sites (under the 5-site flag-and-stop threshold) so it is included in this doc as planned.
- Hard stop respected.
