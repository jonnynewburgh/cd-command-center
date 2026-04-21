---
title: db/ refactor triage
date: 2026-04-19
scope: Diagnostic-only review of the untracked db/ package to decide commit / finish / delete
status: Diagnosis complete — awaiting decision
---

# db/ refactor triage — 2026-04-19

Diagnostic-only. No code was modified. This doc supports a fate decision for the untracked `db/` directory before the next round of `upsert_*` fixes in `db.py`.

## 1. Inventory

### 1.1 File list

| File | Lines | Last modified (file mtime) |
|---|---:|---|
| `db/core.py` | 84 | 2026-04-13 06:26 |
| `db/schema.py` | 1,024 | 2026-04-13 11:27 |
| `db/mutations.py` | 871 | 2026-04-13 11:27 |
| `db/queries.py` | 2,008 | 2026-04-13 11:28 |
| **Total** | **3,987** | All untouched for 6 days |

Compare with `db.py`: **4,406 lines**, last modified 2026-04-19 09:11.

### 1.2 Directory tree

```
db/
├── core.py          # connection + _adapt_sql + _try_exec + _cached
├── mutations.py     # 36 write functions
├── queries.py       # 71 read functions (incl. 1 helper _search_table)
├── schema.py        # init_db() with 34 CREATE TABLE statements
└── __pycache__/
    ├── mutations.cpython-313.pyc
    ├── queries.cpython-313.pyc
    └── schema.cpython-313.pyc
```

**No `__init__.py`.** No tests. No submodules beyond the three stored above. `core.cpython-313.pyc` is absent from `__pycache__` — only `mutations/queries/schema` have been imported successfully at some point in the past.

### 1.3 First-40-line structure of each file

- **`core.py`** — module-level `DATABASE_URL` resolution, `get_connection()`, `_adapt_sql()`, `_try_exec()`, no-op `_cached()` decorator. This is the complete file (84 lines).
- **`schema.py`** — `from db.core import get_connection, _adapt_sql, _try_exec, _IS_POSTGRES`. Single `init_db()` function that wraps the cursor in a `_Cur` shim so every `execute()` auto-calls `_adapt_sql()`. Then 34 `CREATE TABLE IF NOT EXISTS` blocks.
- **`mutations.py`** — `from db.core import get_connection, _adapt_sql, _IS_POSTGRES`. Starts with `upsert_rows`, `log_load_start`, `log_load_finish`, then one group per table.
- **`queries.py`** — `from db.core import get_connection, _adapt_sql, _cached, _IS_POSTGRES`. Grouped the same way as `mutations.py` — schools, tracts, NMTC, FQHC, ECE, 990, CDFI, etc.

## 2. Scope per file

### 2.1 `db/schema.py` — 34 tables
DDL only. Uses `CREATE TABLE IF NOT EXISTS`. Tables (in order):

`schools, lea_accountability, census_tracts, nmtc_projects, cde_allocations, fqhc, ece_centers, irs_990, irs_990_history, cdfi_directory, state_programs, enrollment_history, cdfi_awards, user_notes, user_bookmarks, documents, financial_ratios, data_loads, market_rates, hud_ami, hud_fmr, cra_institutions, cra_assessment_areas, cra_sb_discl, cra_sb_aggr, sba_loans, hmda_activity, bls_unemployment, bls_qcew, scsc_cpf, nmtc_coalition_projects, federal_audits, federal_audit_programs, headstart_programs`

**Same 34 tables, same names, same order as `db.py` `init_db()`.** Schema coverage is 100%.

Not ORM models. Not SQLAlchemy. Plain DDL strings — same style as `db.py`.

### 2.2 `db/mutations.py` — 36 write functions

All `upsert_*`, `save_*`, `update_*`, `delete_*`, `batch_*`, `link_*`, `log_*`, and `compute_and_store_ratios`.

Function list: `upsert_rows, log_load_start, log_load_finish, upsert_school, upsert_charter_school, update_school_census_tract, batch_update_school_census_tracts, upsert_nmtc_project, upsert_cde_allocation, upsert_lea_accountability, upsert_census_tract, upsert_ece, batch_update_ece_geo, upsert_fqhc, batch_update_fqhc_geo, upsert_990, link_ein_to_school, link_ein_to_fqhc, upsert_990_history, upsert_cdfi, upsert_state_program, upsert_enrollment_history, upsert_cdfi_award, save_user_note, update_user_note, delete_user_note, save_bookmark, delete_bookmark, save_document, update_document_data, delete_document, upsert_financial_ratios, compute_and_store_ratios, upsert_scsc_cpf, upsert_nmtc_coalition_project, link_nmtc_coalition_to_projects`.

**Same 36 functions as `db.py`** — these are copies of the write path from `db.py`, refactored into a separate module.

### 2.3 `db/queries.py` — 71 read functions

All `get_*`, `search_*`, `is_*` functions plus one new private helper (`_search_table`).

**Same set as `db.py` with two deltas:**
- `_search_table` is **new** in `db/queries.py` — a refactoring of the repeated per-table `SELECT ... LIKE ?` pattern inside `search_all`. Quality-of-life improvement, not present in `db.py`.
- `get_school_tearsheet_data` is present in **`db.py` (line 4224)** but **missing from `db/queries.py`**. Added to `db.py` after 2026-04-13 (the refactor's last-touch date).

## 3. Overlap analysis — mutations

All 36 `db/mutations.py` functions have a same-named counterpart in `db.py`. Classification:

| Class | Count | Detail |
|---|---:|---|
| Significant divergence — **`db.py` is fixed, `db/` is broken** | **4** | `upsert_census_tract`, `upsert_school`, `upsert_fqhc`, `upsert_ece` — see §3.1 |
| Identical copy | 32 | Same signature, same SQL, same execution pattern |
| New/unique to refactor | 0 | |

### 3.1 Bug-fix drift — the four `upsert_*` that shipped yesterday

Commits `c1fc5b3`, `c87742b`, `0b63cd7`, `8a8d848` (all on 2026-04-18) added three categories of Postgres-compat fixes to `db.py`. The refactor (last touched 2026-04-13) does **not** have any of them:

| Function | Fix in `db.py` | Status in `db/mutations.py` |
|---|---|---|
| `upsert_school` | `cur.execute(_adapt_sql(sql), values)` at `db.py:2085` | `cur.execute(sql, values)` at `db/mutations.py:137` — **broken on PG** |
| `upsert_census_tract` | `_adapt_sql` wrap at `db.py:2238` + column qualification `census_tracts.{col}` at `db.py:2228` | no `_adapt_sql` wrap at `db/mutations.py:307`; ambiguous COALESCE `{col}` at `db/mutations.py:297` — **broken on PG, both bugs** |
| `upsert_fqhc` | `_adapt_sql` wrap | no `_adapt_sql` wrap at `db/mutations.py:384` — **broken on PG** |
| `upsert_ece` | `_adapt_sql` wrap | no `_adapt_sql` wrap at `db/mutations.py:333` — **broken on PG** |

Scale marker: `db.py` uses `_adapt_sql(` **36 times**; `db/mutations.py` uses it **9 times**. That's a 27-call deficit in write paths — the refactor pre-dates the entire Postgres-compat sweep.

### 3.2 Reverse check — does `db/` contain any fix that `db.py` is missing?

**No.** Zero-call search for patterns that are in `db/mutations.py` but not `db.py`. No functions in `db/mutations.py` are further along than their `db.py` twin.

## 4. Overlap analysis — queries

Classification for 71 functions in `db/queries.py`:

| Class | Count | Detail |
|---|---:|---|
| Significant divergence — **`db.py` is fixed, `db/` is broken** | **4** | `get_school_summary`, `get_census_tract_summary`, `get_fqhc_summary`, `get_fqhc_by_id` — and likely `get_ece_summary` / `get_ece_by_id` (not separately verified; same shape) |
| New/unique to refactor | 1 | `_search_table` (private helper, used only by `search_all`) |
| Identical copy | ~66 | |

### 4.1 Bug-fix drift — the four read-side fixes that shipped yesterday

Same four commits above also fixed reader functions that did `dict(row)` directly on a psycopg2 tuple:

| Function | `db.py` | `db/queries.py` |
|---|---|---|
| `get_school_summary` | `cols = [d[0] for d in cur.description]; dict(zip(cols, row))` at `db.py:1526–1527` | `dict(row) if row else {}` at `db/queries.py:198` — **returns garbage on PG** |
| `get_census_tract_summary` | `cols = [...]; dict(zip(cols, row))` at `db.py:1648–1649` | `dict(row) if row else {}` at `db/queries.py:317` — **returns garbage on PG** |
| `get_fqhc_summary` | fixed at `db.py:1851–1852` | `dict(row) if row else {}` at `db/queries.py:516` — **returns garbage on PG** |
| `get_fqhc_by_id` | `_adapt_sql` + fixed at `db.py:2342, 2347–2348` | raw `?` + `dict(row)` at `db/queries.py:528, 531` — **broken on PG** |

Scale marker: `db.py` has the broken `dict(row) if row else {}` pattern in **3** remaining places; `db/queries.py` has it in **9** places. The refactor pre-dates the sweep.

### 4.2 `get_school_tearsheet_data` drift

Present in `db.py` (lines 4224–end), absent from `db/queries.py`. Added to `db.py` **after** the refactor was snapshotted. If the refactor is adopted, this function would need to be migrated separately — or its callers would break.

Also note: `get_school_tearsheet_data` itself uses `conn.row_factory = _sqlite3.Row` at `db.py:4237` — SQLite-only. It is a **pre-existing Postgres-compat latent bug** in `db.py`, not a refactor issue. Relevant because adopting `db/` does not erase it.

## 5. Import graph

### 5.1 What the codebase imports

**33 files import the `db` module**, all as `import db` (bare):

- `validate.py`
- `api/main.py`
- `api/routers/accountability.py, audits.py, cdfis.py, ece.py, fqhc.py, headstart.py, housing.py, lending.py, nmtc.py, notes.py, orgs.py, rates.py, search.py, tracts.py`
- `etl/compute_financial_ratios.py, fetch_990_irs.py, fetch_bls_qcew.py, fetch_bls_unemployment.py, fetch_cra_data.py, fetch_edfacts_auto.py, fetch_fac.py, fetch_hmda.py, fetch_hud_ami.py, fetch_hud_fmr.py, fetch_sba_loans.py, load_cra_lending.py, load_headstart_pir.py, load_nmtc_coalition.py, load_scsc_cpf.py, patch_pct_asian.py`
- `utils/db_backup.py`

**Zero files** do `from db.core`, `from db.queries`, `from db.mutations`, or `from db.schema` — *except* the refactor files themselves (`db/mutations.py`, `db/queries.py`, `db/schema.py` each `from db.core import ...`).

### 5.2 Python's current resolution for `import db`

Verified via `importlib.util.find_spec`:

```
db       → C:\...\cd-command-center\db.py   (module, not a package)
db.core  → ModuleNotFoundError: __path__ attribute not found on 'db'
```

With both `db.py` and `db/` present and `db/` missing `__init__.py`, Python's precedence order is **regular package > module > namespace package**. Since `db/` has no `__init__.py`, it is at most a namespace package; `db.py` wins. As a consequence:

- Every `import db` in the codebase resolves to **`db.py`**. The refactor is completely bypassed.
- `from db.core import ...` inside `db/mutations.py`, `db/queries.py`, `db/schema.py` **currently fails** because `db` is the module `db.py`, which has no submodules.
- The stale `__pycache__` inside `db/` must be from an earlier state where either `db.py` was absent or an `__init__.py` existed and was later removed.

**Consequence: `db/` is currently dead code and cannot be imported while `db.py` exists.**

## 6. Completeness

| Dimension | `db.py` | `db/*` | Coverage |
|---|---:|---:|---:|
| Tables in `init_db()` | 34 | 34 | 100% |
| Top-level `def`s | 112 | 111 (71 queries + 36 mutations + 4 in core) | ~99% |
| Write functions | 36 | 36 | 100% |
| Read functions | 70 (+ `get_school_tearsheet_data` post-refactor) | 70 + 1 new helper | ~99% |
| `_adapt_sql()` call sites in write path | 36 | 9 | **25%** (largest drift axis) |
| Functions missing fixes shipped yesterday | 0 | **≥8** (4 upserts + 4 readers) | — |

### 6.1 Functions in `db.py` but NOT in `db/*`

Only one true omission:

1. `get_school_tearsheet_data` — added to `db.py` after 2026-04-13.

All other `db.py` entries either (a) have a twin in `db/*` (possibly broken), or (b) are helpers (`get_connection`, `_adapt_sql`, `_try_exec`, `_cached`, `init_db`) that live in `db/core.py` / `db/schema.py`.

Net: the structural migration is **essentially complete**. The correctness migration is *partial* — the refactor was snapshotted before yesterday's Postgres-compat sweep.

## 7. Git history

- `git log --all -- db/` → **empty**. `db/` has never been committed on any branch.
- `git log --all --grep="refactor|split|modulariz"` → two matches, both unrelated (`b709a8d`/`7719f76`, ECE auto-download work).
- No stashed, branched, or historical reference to the refactor exists. The directory is local-only, created and left on disk.

## 8. Working tree state

Besides `db/`, the untracked tree contains seven files (`.env.example`, `DATA_REFRESH_SCHEDULE.md`, `data/test.pdf`, two `docs/debug/*` sessions docs, `utils/etl_helpers.py`, `utils/state_fips.py`). None of them reference the refactor:

- `CLAUDE.md` — no mention of `db/`, `db.queries`, `db.mutations`, or `modularize`.
- `AGENTS.md` — no mention of the refactor.
- No modified import lines anywhere in `etl/`, `api/`, or `utils/`.
- No partial migration in-progress (e.g. `api/routers/*.py` still do plain `import db`).

The refactor is a self-contained artifact. Adopting it would require coordinated edits to 33 caller files (or an `__init__.py` that re-exports everything from `db.py`-equivalents — effectively a shim).

## 9. Three paths forward (trade-offs only — no recommendation)

### Path A — Commit as-is, finish incrementally

**What it looks like:** `git add db/` → commit with a message like "Snapshot in-progress db.py split"; keep `db.py` as the active module; treat `db/` as a staging area to migrate into over several sessions; eventually flip imports once `db/` reaches parity.

**Pros**
- Preserves the Apr-13 work in git history — no risk of accidental deletion.
- Makes the intent visible to future sessions (and to future-you reading `git log`).
- Cheap to execute now. Does not block today's upsert work.

**Cons**
- Commits a directory that is **currently unimportable and known-broken** on Postgres. Anyone who tries to use it (including future-Claude) will hit the bugs §3.1 / §4.1 catalogued or the `__init__.py` / shadowing issue §5.2.
- The committed version drifts further from `db.py` with every fix landed in `db.py`. The 8+ drift items from yesterday will become 19 if the remaining 11 `upsert_*` fixes land in `db.py` first. Cost of finishing the refactor rises monotonically.
- Risk of "committed but forgotten" — the refactor sits in-tree looking official but is not wired up to anything. Likely source of future confusion.

### Path B — Finish the refactor now, before next upsert batch

**What it looks like:** (1) add `__init__.py` that re-exports every public symbol so `import db` keeps working; (2) delete `db.py` or rename it to avoid the module-vs-package conflict; (3) port `get_school_tearsheet_data` into `db/queries.py`; (4) re-apply the 8+ fixes from yesterday (`c1fc5b3`, `c87742b`, `0b63cd7`, `8a8d848`) into `db/mutations.py` and `db/queries.py`; (5) run the four-pipeline verification recipe on Postgres to prove no regression; (6) commit. Then proceed with the 11 remaining `upsert_*` fixes in the new package.

**Pros**
- Resolves the decision permanently — no drift risk going forward.
- The next 11 `upsert_*` fixes land in the right place the first time; no rework.
- Smaller files are easier to navigate — `db.py` at 4,406 lines is awkward; three files at <2,100 each is much nicer.
- Session already has the four-pipeline verification recipe warm from yesterday.

**Cons**
- Biggest scope of the three paths. Probably a half-day of work before any new functionality moves — not 60 minutes.
- Re-applying yesterday's eight fixes means re-doing diagnosis work that is fresh in `docs/debug/*_diagnosis_2026-04-18.md`. Mechanical but tedious.
- Introduces a single large commit (or a multi-commit series) that touches the entire DB access layer. Higher review burden.
- Mixes two concerns in a single session: "finish refactor" and "unblock remaining upserts." If the refactor hits an unexpected snag, the upsert sweep is blocked too.

### Path C — Delete `db/`, stay on `db.py`

**What it looks like:** `rm -rf db/`; keep `db.py` as the sole module; proceed with the 11 remaining `upsert_*` fixes against the monolith; address file-size concerns (if any) later.

**Pros**
- Simplest path. Zero blocking work before the next upsert batch.
- No risk of the shadowing / `__init__.py` trap in §5.2 for any session that does not know the refactor exists.
- The 11 remaining fixes are small, mechanical, and proven — landing them against `db.py` is the path of least friction.
- Does not foreclose a future refactor; one can be started fresh (and correctly, with `__init__.py`) whenever the file-size pain justifies it.

**Cons**
- Loses ~3,987 lines of work done on Apr 13 — though most of that work is either (a) mechanical copy-paste from `db.py`, or (b) out of date relative to yesterday's fixes. The unique contributions are `_search_table` (small) and the module structure itself (reconstructible).
- If monolithic `db.py` will ever be split, that work gets repeated eventually. But the repeat will start from a now-correct baseline.
- Feels wasteful to delete uncommitted code, even if the code is broken and not wired up.

### Common to all three paths

No path short-circuits the 11-function `upsert_*` sweep. Path A defers it least, Path B most, Path C doesn't affect it.

Whichever path is taken, at least one of the following is worth recording in the decision: the fate of the stale `db/__pycache__`, the missing `__init__.py` (Paths A/B), and the fact that `get_school_tearsheet_data` is not in `db/queries.py` (Paths A/B).

## Appendix — method notes

- "Broken on PG" above means verified by reading the code, not by running it. The `_adapt_sql` wrap is the same root cause fixed in `c1fc5b3`/`c87742b`/`0b63cd7`/`8a8d848`; the `dict(row) if row else {}` pattern is the same reader bug fixed in the same four commits.
- Identical-copy claims for the other 32 mutations and ~66 queries are inferred from signature alignment plus identical docstrings seen in the sampled functions; a line-by-line diff across all 107 functions was not performed for this triage.
- No code was executed. No files were modified other than this document.
