---
title: NCES pipeline diagnosis
date: 2026-04-18
scope: etl/fetch_nces_schools.py, etl/fetch_nces_charter_schools.py, etl/load_charter_schools.py, and their db.py consumers
status: Diagnosis only — no fixes applied
---

# NCES pipeline diagnosis — 2026-04-18

## TL;DR

- **SQLite backend: works.** GA/2022 run loaded 2,332 schools (99 charter, 2,233 traditional) into `schools`. Post-run totals match expectations.
- **Postgres backend: silently broken.** Same run reports "Loaded: 2,332 schools" in stdout but writes *zero* rows. Every `upsert_school()` call raises a psycopg2 exception that is swallowed by a bare `except Exception: continue` block in `db.py`. Direct probe confirms: `upsert_school({...valid record...})` returns normally and leaves the table unchanged.
- **Root cause:** `upsert_school()` at `db.py:2052` builds SQL with SQLite `?` placeholders and never calls `_adapt_sql()` before execution. Identical bug pattern to the census-tract bug already fixed in commit `c1fc5b3` (BUG 1).
- **Secondary bug:** `get_school_summary()` at `db.py:1506` uses `dict(row)` on a psycopg2 tuple and also uses `is_charter = 1` (integer) instead of a boolean. Both fail silently; summary returns `{}`, which is why the post-run CLI output prints "Total schools: 0".
- **Smallest possible fix:** one-line change — add `_adapt_sql` wrapping to the `cur.execute` at `db.py:2074`. That alone unblocks inserts. The summary/lookup bugs are independent and do not block ingestion (they only affect reads).

## 1. Inventory

Three ETL scripts live under `etl/` that touch NCES school data:

| File | Lines | Purpose | DB target | Status |
|------|-------|---------|-----------|--------|
| `etl/fetch_nces_schools.py` | 473 | Canonical loader: fetches **all public schools** (charter + traditional) from Urban Institute Education Data API into the `schools` table with `is_charter` flag | `schools` | In use |
| `etl/fetch_nces_charter_schools.py` | 487 | Legacy loader: fetches **charter schools only** from same API into a separate `charter_schools` table | `charter_schools` (SQLite only) | Legacy / Phase 1 era |
| `etl/load_charter_schools.py` | 165 | Earliest Phase 1 loader (last touched `ce071e4`); superseded by both of the above | `charter_schools` | Deprecated |

Both fetch scripts hit the same API base:
- Directory: `https://educationdata.urban.org/api/v1/schools/ccd/directory/{year}/`
- Enrollment: `https://educationdata.urban.org/api/v1/schools/ccd/enrollment/{year}/grade-99/`

Canonical entry point for current work: `python etl/fetch_nces_schools.py --states GA --year 2022`.

The `charter_schools` table **does not exist in Postgres** (`SELECT to_regclass('charter_schools')` returns `None`). It exists in SQLite as a legacy sibling table (1,420 rows, 18 GA).

## 2. Scope (as agreed)

Do not fix, refactor, or modify. Inventory → run → report → stop.

## 3. Pre-run current state

| Backend | schools total | schools GA | charter_schools exists | charter_schools total |
|--------|--------------:|-----------:|------------------------|----------------------:|
| Postgres | 97,735 | 2,335 | no | — |
| SQLite | 97,735 | 2,335 | yes | 1,420 (GA: 18) |

Most recent `updated_at` on Postgres GA schools before today's run: `2026-04-01 12:24:23`.

Last code touch to either fetch script: commit history shows both predate the current bug sweep (census tract `c1fc5b3`).

## 4. End-to-end run

### 4a. SQLite (`env -u DATABASE_URL python etl/fetch_nces_schools.py --states GA --year 2022`)

Exit code: 0. Stdout (trimmed):

```
Fetching GA... 2,332 schools (99 charters)
Fetched 2,332 total schools from API.
Loading into database...
Done.
  Loaded: 2,332 schools
  Charter schools: 99
  Traditional public: 2,233

Database now contains:
  Total schools: 97,750
  Open schools:  95,936
```

No stderr. Totals are consistent (pre 97,735 + 15 new ≈ 97,750). Three sample rows verified present in SQLite `schools` table after the run.

Result: **SQLite path works.**

### 4b. Postgres (same command, `DATABASE_URL` set)

Exit code: 0. Stdout (trimmed):

```
Fetching GA... 2,332 schools (99 charters)
Fetched 2,332 total schools from API.
Loading into database...
Done.
  Loaded: 2,332 schools
  Charter schools: 99
  Traditional public: 2,233

Database now contains:
  Total schools: 0
  Open schools:  0
```

No stderr. But the "Loaded: 2,332" line is the loop counter in the ETL script — **not** a verification of database state.

Direct post-run verification:

```
schools total:   97,735     (unchanged from pre-run)
schools GA:      2,335      (unchanged)
MAX(updated_at): 2026-04-01 12:24:23    (not today)
```

**No rows were inserted or updated.** The "Loaded: 2,332" output is misleading.

## 5. Bug pattern check on `upsert_school` (three bugs fixed in census-tract path)

| # | Bug | In `upsert_school` / read helpers? | Location |
|---|-----|------------------------------------|----------|
| 1 | `cur.execute(sql, values)` with `?` placeholders but **no `_adapt_sql()` wrap** | **YES** | `db.py:2074` (`upsert_school`) |
| 2 | Unqualified column reference inside `COALESCE`/`preserve_if_null` ON CONFLICT clauses (Postgres `AmbiguousColumn`) | **N/A** | `upsert_school` builds a plain `excluded.{col}` update clause — no COALESCE / preserve_if_null logic |
| 3 | `dict(row)` on psycopg2 return (psycopg2 returns tuples, not sqlite3.Row) | **YES (twice)** | `db.py:1471` (`get_school_by_id`), `db.py:1524` (`get_school_summary`) |

### BUG 1 confirmed (the blocker)

Direct probe against Postgres:

```python
db.upsert_school({
    'nces_id': 'TEST_PROBE_0001',
    'school_name': 'Test Probe',
    'state': 'GA',
    'is_charter': False,
    ...
})
# → returns normally, no exception raised
# SELECT ... WHERE nces_id='TEST_PROBE_0001' → None
```

Mechanism — `db.py:2067–2079`:

```python
for table in ["schools", "charter_schools"]:
    try:
        sql = f"""
            INSERT INTO {table} ({...})
            VALUES ({placeholders})                 # placeholders = "?,?,?,..."
            ON CONFLICT(nces_id) DO UPDATE SET ...
        """
        cur.execute(sql, values)                    # <-- no _adapt_sql(sql) wrap
        conn.commit()
        conn.close()
        return
    except Exception:
        continue                                     # silent swallow
conn.close()
```

On Postgres, `cur.execute(sql, values)` raises a `psycopg2` syntax error at the first `?`. The except catches it, the loop tries `charter_schools` (does not exist → another exception → swallowed), loop ends, function returns normally. The ETL caller believes the row was loaded.

Same pattern appears in `get_school_by_id` (line 1467 uses `?` without adaptation, but that's short-circuited by bug 3 before it matters on Postgres) and the schools read path.

### BUG 3 confirmed (summary returns zero)

`get_school_summary` at `db.py:1506`:
- Line 1510: `charter_filter = "WHERE is_charter = 1"` — works in SQLite (int 1) but Postgres column is BOOLEAN; comparison against integer literal raises a type error when `charter_only=True`. Not directly reached by this CLI run (`charter_only` defaults to False), so not the reason for the 0.
- Line 1524: `return dict(row) if row else {}` — `row` from psycopg2 is a tuple. `dict(tuple)` raises `TypeError`. Caught by `except Exception: continue`. Loop advances to `charter_schools` (table missing) → another exception → returns `{}`. `summary.get("total_schools", 0)` → 0.

This is the reason the CLI says "Total schools: 0" even when rows actually exist in Postgres (they do — 97,735 of them). It is *also* the mechanism that makes the Postgres breakage look more total than it is in stdout.

### Silent-failure pattern (meta)

The single most dangerous pattern in these helpers is `try: ... except Exception: continue` wrapped around a `for table in ["schools", "charter_schools"]` loop. This pattern:
1. Hides real Postgres errors (placeholder syntax, ambiguous columns, type mismatches).
2. Coerces every failure into silent no-op success.
3. Was originally written to support a soft migration from `charter_schools` to `schools` in SQLite; on Postgres where the sibling table does not exist, it just doubles the swallow.

Applies to at least: `upsert_school`, `get_school_by_id`, `get_school_summary`, `get_school_states` (and likely more — out of scope here).

## 6. First stage producing wrong output

Pipeline stages in `fetch_nces_schools.py`:

1. Parse args → OK
2. Fetch directory data from Urban API → OK (2,332 schools returned for GA 2022)
3. Fetch enrollment data → OK (merged in)
4. Normalize to records list → OK (same record shape feeds SQLite path which works)
5. **Call `db.upsert_school(record)` in a loop** → **FAILS silently on Postgres** (BUG 1)
6. Print "Loaded: N" — counter is incremented before DB confirmation, so misleading
7. Call `db.get_school_summary()` → **FAILS silently on Postgres** (BUG 3), returns `{}`
8. Print totals → prints 0/0

**First break point: stage 5** (`db.upsert_school`). Everything upstream is fine and backend-agnostic. Stage 7 is broken independently but is a read path; it does not cause the data loss, it only disguises it.

## 7. Charter-specific concerns

User flagged the `ncessch` → `seasch` join key issue for charter schools.

Reviewed: both fetch scripts pull `ncessch` as the primary key (stored as `nces_id`) and `seasch` as a secondary state-assigned ID (stored as `seasch`). For GA the Urban API returns both fields populated for charter rows. No join-key mismatch observed in the 99 charter rows fetched this run — `nces_id` is unique per row, `seasch` is populated but not used as a join key in the insert path. The ON CONFLICT clause keys solely on `nces_id`.

This is not the current blocker. If it becomes relevant at matching time (e.g., joining to SCSC CPF via `seasch`), it lives downstream of this pipeline.

`fetch_nces_charter_schools.py` (the legacy loader) writes to the Postgres-missing `charter_schools` table, so it is a separate dead-end on Postgres independent of the bug described here.

## 8. Known blockers / open items

- **Postgres ingest blocked** until `upsert_school` gets `_adapt_sql` applied. One-line change at `db.py:2074` unblocks it.
- **Postgres summary/lookup cosmetically broken** independent of ingestion — `get_school_summary` (line 1524), `get_school_by_id` (line 1471). These need the `zip(cur.description, row)` pattern used in the census-tract fix (`c1fc5b3`) and removal of the silent except where possible.
- **`charter_schools` table absent in Postgres.** The fallback `for table in ["schools", "charter_schools"]` loop has no useful branch in Postgres. Not a blocker for the current fix, but the loop should probably be dropped when this code is touched.
- **`fetch_nces_charter_schools.py` (legacy)** writes to `charter_schools` only and is a no-op on Postgres even after `upsert_school` is fixed. Decision pending whether to keep, retire, or merge into the canonical loader.

## 9. Smallest fix (for reference, not applied)

In `db.py:2074`:

```python
cur.execute(sql, values)
```

becomes

```python
cur.execute(_adapt_sql(sql), values)
```

That single wrap makes the Postgres path accept the statement. Inserts will then succeed; `updated_at=CURRENT_TIMESTAMP` is already backend-agnostic. The read-path fixes (`get_school_summary`, `get_school_by_id`) are a separate, non-blocking change.

No changes applied. Awaiting authorization.

---

## Correction — 2026-04-18 late session

Sections above (under "Bug pattern check on `upsert_school`" and the closing remarks for `get_school_summary`) asserted that `schools.is_charter` is `BOOLEAN` in Postgres and that `WHERE is_charter = 1` would therefore fail on the Postgres backend when `charter_only=True`. **That claim was wrong.**

Verified after commit `c87742b` by running `information_schema.columns` directly against the live Postgres database and executing the query both ways:

| Backend | `is_charter` column type | Distinct stored values |
|---|---|---|
| Postgres | `integer` (`int4`) | `0`, `1` |
| SQLite   | `INTEGER`           | `0`, `1` |

Both backends store `is_charter` as INTEGER. `WHERE is_charter = 1` works correctly on both — no fix needed.

The "fix" that was proposed in the original bug flag (`WHERE is_charter = TRUE`) would have actively **broken** the Postgres path:

```
psycopg2.errors.UndefinedFunction: operator does not exist: integer = boolean
LINE 1: SELECT COUNT(*) FROM schools WHERE is_charter = TRUE
                                                      ^
```

Postgres is strict about `integer = boolean` type mismatches and rejects the comparison.

Four-way verification of `get_school_summary` on both backends × both `charter_only` modes, against the current `c87742b` code, returns identical non-empty dicts on every invocation:

```
PG  charter_only=False: {'total_schools': 97750, 'open_schools': 95936, 'total_enrollment': 47430706}
PG  charter_only=True : {'total_schools':  8358, 'open_schools':  8016, 'total_enrollment':  3861134}
SQL charter_only=False: {'total_schools': 97750, 'open_schools': 95936, 'total_enrollment': 47430706}
SQL charter_only=True : {'total_schools':  8358, 'open_schools':  8016, 'total_enrollment':  3861134}
```

### Implications

- The **"Known remaining issues" note in the `c87742b` commit message** (`"is_charter = 1 literal in get_school_summary fails on Postgres when charter_only=True (column is BOOLEAN, not INT)"`) is **obsolete**. No action needed on `db.py:1510`.
- Phase 3 was closed as a no-op. No `db.py` changes made.
- Lesson: before flagging a Postgres type-mismatch bug, confirm the actual column type via `information_schema.columns` or `pg_typeof(...)` rather than assuming conventional Postgres idioms — this schema was built SQLite-native and migrated column-for-column, so INTEGER booleans persisted into Postgres as `int4`.
