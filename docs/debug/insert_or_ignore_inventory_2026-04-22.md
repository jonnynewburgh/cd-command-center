---
title: INSERT OR IGNORE Postgres-compat inventory
date: 2026-04-22
scope: All Python files under repo root — db.py, etl/, api/, utils/, validate.py, archive/
status: Inventory only — no code edits. Approach recommendation at the bottom.
---

# INSERT OR IGNORE inventory — 2026-04-22

## Context

SQLite supports `INSERT OR IGNORE` as a conflict-silencer: if the insert would violate a unique constraint, SQLite swallows the conflict and returns without raising. Postgres does not accept this syntax at all — psycopg2 raises `syntax error at or near "OR"` on any statement containing `INSERT OR IGNORE`.

The existing `adapt_sql()` helper in `db.py:49-54` does pure string substitution (`?` → `%s`, `INTEGER PRIMARY KEY AUTOINCREMENT` → `SERIAL PRIMARY KEY`). It does not rewrite `INSERT OR IGNORE`.

One site (`upsert_state_program`) was flagged during the 2026-04-21 micro-session as a distinct bug class from the adapt_sql raw-? sweep and deferred to B5. This doc scopes the rest of the surface.

## Grep patterns run

Against `*.py` under the repo root, multi-line mode enabled:

- `INSERT\s+OR\s+IGNORE` (case-insensitive, multi-line) — 3 matches
- `INSERT\s+OR\s+REPLACE` (case-insensitive, multi-line) — 0 matches
- `\.execute\(\s*[\"'f][\"']?\s*INSERT\s+INTO` (case-insensitive, multi-line) — 5 matches (bare INSERT sites, separately classified below)
- `INSERT\s+INTO` (files-with-matches) — cross-checked: `db.py`, `etl/fetch_990_irs.py`, `etl/fetch_lea_accountability.py`, `etl/migrate_sqlite_to_postgres.py`, `etl/load_cra_lending.py`

`api/` and `api/routers/` have zero direct `INSERT` statements — all writes route through `db.py` helpers. `utils/` and `archive/` have no INSERT OR IGNORE sites.

## Summary

**3 INSERT OR IGNORE sites. 0 INSERT OR REPLACE sites. 5 bare INSERT sites (3 already safe via ON CONFLICT, 2 are intentional new-row inserts keyed on AUTOINCREMENT).**

Of the 3 INSERT OR IGNORE sites, only **one** is an active code path that will break on Postgres. The other two are either (a) already branched per-backend with a correct Postgres path, or (b) a trivial one-line rewrite with a natural UNIQUE key.

| INSERT OR IGNORE site | Schema support | Fixable as ON CONFLICT? |
|---|---|---|
| `db.py:2794` `upsert_state_program` / `state_programs` | **No UNIQUE constraint** | Not trivially — needs schema work |
| `db.py:3120` `save_bookmark` / `user_bookmarks` | `UNIQUE(entity_type, entity_id)` | Yes — trivial |
| `etl/load_cra_lending.py:330` `flush(batch)` / `cra_sb_discl` \| `cra_sb_aggr` | Both have UNIQUE constraints | Already done — gated by `if is_pg` with correct Postgres path |

| Exposure | Count |
|---|---:|
| ACTIVE | 3 |
| LATENT | 0 |
| DEAD | 0 |

(All three sites have an ACTIVE caller path. Exposure is not the bottleneck — UNIQUE-constraint coverage is.)

## Full site table — INSERT OR IGNORE

| # | file:line | Enclosing function | Full statement | `?` placeholders | Through `adapt_sql`? | Target table |
|---:|---|---|---|---|---|---|
| 1 | `db.py:2794` | `upsert_state_program(record)` | `f"INSERT OR IGNORE INTO state_programs ({','.join(columns)}) VALUES ({placeholders})"` (f-string, `placeholders` = `",".join("?" * len(values))`) | Yes | **No** | `state_programs` |
| 2 | `db.py:3120` | `save_bookmark(entity_type, entity_id, label)` | `"INSERT OR IGNORE INTO user_bookmarks (entity_type, entity_id, label) VALUES (?, ?, ?)"` | Yes | **No** | `user_bookmarks` |
| 3 | `etl/load_cra_lending.py:330` | `flush(batch)` inside `_load_file(table, filepath, ...)` | `f"INSERT OR IGNORE INTO {table} ({col_list}) VALUES ({q_marks})"` — inside the `else:` branch of `if is_pg:`; the Postgres branch at `:322` uses `psycopg2.extras.execute_values` with `ON CONFLICT DO NOTHING` | Yes (`?` via `q_marks`) | **No** — but gated by `if is_pg` | `cra_sb_discl` or `cra_sb_aggr` (dynamic) |

All three sites use `?` placeholders and none route through `adapt_sql()`. Site 3 is harmless because its SQLite branch is unreachable when `db._IS_POSTGRES` is true. Sites 1 and 2 are currently broken on Postgres (they will raise a syntax error before the placeholder conversion question even matters).

Cross-check against the adapt_sql inventory (`docs/debug/chained_execute_inventory_2026-04-21.md` and the raw-? sweep tracked in B2): sites 1 and 2 are additional raw-? sites not listed in either inventory. They are not in scope for the adapt_sql pure-substitution sweep because even a correct `?` → `%s` substitution leaves `INSERT OR IGNORE` unparseable on Postgres.

## Full site table — INSERT OR REPLACE

None.

## Full site table — bare `INSERT INTO` (no `INSERT OR IGNORE`)

Reported for completeness. Classified into (a) sites already carrying an `ON CONFLICT` clause — safe against duplicates and unrelated to the B5 bug class — and (b) sites with no conflict clause, which rely on an AUTOINCREMENT surrogate key so every call produces a new row by design.

| # | file:line | Enclosing function | Statement shape | ON CONFLICT clause? | Target table |
|---:|---|---|---|---|---|
| B1 | `db.py:2731` | `upsert_cdfi(record)` | `INSERT INTO cdfi_directory (...) VALUES (...) ON CONFLICT(cdfi_name, state) DO UPDATE ...` | Yes | `cdfi_directory` |
| B2 | `db.py:2940` | `upsert_enrollment_history(record)` | `INSERT INTO enrollment_history (...) VALUES (...) ON CONFLICT(nces_id, school_year) DO UPDATE ...` | Yes | `enrollment_history` |
| B3 | `etl/fetch_lea_accountability.py:241` | `_upsert_lea(row)` | `INSERT INTO lea_accountability (...) VALUES (...) ON CONFLICT(lea_id, data_year) DO UPDATE ...` | Yes | `lea_accountability` |
| N1 | `db.py:3068` | `save_user_note(entity_type, entity_id, note_text)` | `INSERT INTO user_notes (...) VALUES (?, ?, ?)` — intentional new-row insert; function returns `cur.lastrowid` | No — not needed | `user_notes` (PK `id`, no other UNIQUE) |
| N2 | `db.py:3167` | `save_document(record)` | `INSERT INTO documents (...) VALUES (...)` — intentional new-row insert; function returns `cur.lastrowid` | No — not needed | `documents` (PK `id`, no other UNIQUE) |

**B1/B2/B3 are safe against duplicates** (ON CONFLICT handles it). They are still bare `cur.execute(...)` calls that bypass `adapt_sql`, so their `?` placeholders would still break on Postgres — but that's the B1/B2 adapt_sql sweep bug class, not B5. Do not touch in this workstream.

**N1/N2 are intentionally bare**. Each call is meant to create a new row. Safe on both backends provided `cur.lastrowid` is handled correctly — which is a separate bug class (see "Other bug classes surfaced" below).

## Per-site schema check

### Site 1 — `state_programs` (`db.py:463-476`)

```sql
CREATE TABLE IF NOT EXISTS state_programs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    state TEXT NOT NULL,
    program_name TEXT NOT NULL,
    program_type TEXT,
    ...
)
```

- PRIMARY KEY: `id INTEGER PRIMARY KEY AUTOINCREMENT`
- UNIQUE constraints: **none**
- Natural conflict key for `ON CONFLICT(...) DO NOTHING`: **none available**. Best candidate would be `(state, program_name)` — matches the existing schema intent and the loader's record shape — but it is not currently declared.
- **FLAG: SCHEMA WORK REQUIRED.** Cannot be rewritten to `ON CONFLICT` without either adding a UNIQUE constraint (schema migration) or picking a column list to `ON CONFLICT(col1, col2) DO NOTHING` — but Postgres requires an actual UNIQUE index backing that column set, so the migration is unavoidable.
- Side finding: `upsert_state_program`'s docstring says "Ignores duplicates", but on SQLite the only duplicate `INSERT OR IGNORE` can detect here is a PK conflict on `id`, and `id` is AUTOINCREMENT, so it never conflicts. The function is currently **silent accept-all** on both backends where it runs at all; on Postgres it additionally fails with a syntax error. The loader `etl/load_state_programs.py` re-reads the seed CSV, which means every re-run grows duplicate rows on SQLite. Surface this when the schema work is scheduled.

### Site 2 — `user_bookmarks` (`db.py:557-565`)

```sql
CREATE TABLE IF NOT EXISTS user_bookmarks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    entity_type TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    label TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(entity_type, entity_id)
)
```

- PRIMARY KEY: `id INTEGER PRIMARY KEY AUTOINCREMENT`
- UNIQUE: `UNIQUE(entity_type, entity_id)`
- Natural conflict key: `(entity_type, entity_id)` — perfect match for current usage (`save_bookmark` inserts exactly those three columns and the label).
- **OK — trivial ON CONFLICT rewrite.** Target statement:
  ```sql
  INSERT INTO user_bookmarks (entity_type, entity_id, label)
  VALUES (?, ?, ?)
  ON CONFLICT(entity_type, entity_id) DO NOTHING
  ```
  plus `adapt_sql(...)` wrap so `?` → `%s`.

### Site 3 — `cra_sb_discl` / `cra_sb_aggr` (`db.py:740-765` / `778-797`)

Both tables have:

- PRIMARY KEY: `id INTEGER PRIMARY KEY AUTOINCREMENT`
- UNIQUE:
  - `cra_sb_discl`: `UNIQUE(respondent_id, agency_code, year, state_fips, county_fips, msa_code, census_tract, row_code, loan_type)` (9 columns)
  - `cra_sb_aggr`: `UNIQUE(year, state_fips, county_fips, msa_code, census_tract, row_code)` (6 columns)
- Natural conflict key available: yes, on both.
- **OK — already correctly handled by the `if is_pg:` branch at `load_cra_lending.py:318-325`**, which uses `psycopg2.extras.execute_values` with the statement `f"INSERT INTO {table} ({col_list}) VALUES %s ON CONFLICT DO NOTHING"`. This relies on the sole non-PK unique index for conflict inference on each table, which works because each table has exactly one UNIQUE constraint.
- **No action needed for B5.** Flag only: the SQLite branch's `INSERT OR IGNORE` is dead code on Postgres runs, but it's real code on SQLite runs and should stay.

## Runtime exposure per site

### Site 1 — `upsert_state_program` / `state_programs`

- Callers:
  - `etl/load_state_programs.py:142` (only caller)
- Documented command in `CLAUDE.md`:
  ```bash
  python etl/load_state_programs.py
  python etl/load_state_programs.py --file data/raw/my_programs.csv
  ```
- Not in `run_pipeline.py` — not part of the scheduled annual pipeline.
- `DATABASE_URL` exported to Postgres in `~/.bashrc` per `DATA_REFRESH_SCHEDULE.md`, so any manual run hits Postgres and crashes immediately on the first `INSERT OR IGNORE`.
- **Classification: ACTIVE.** This is the function that surfaced the bug class during the 2026-04-21 micro-session.

### Site 2 — `save_bookmark` / `user_bookmarks`

- Callers:
  - `api/routers/notes.py:78` — `POST /bookmarks` endpoint. Imported and wired through `api/main.py:24 import db`. Reachable from the Next.js dashboard's bookmark UI.
  - `archive/app.py:1094` — DEAD (archived Streamlit frontend).
- **Classification: ACTIVE.** Any dashboard user hitting "bookmark this" on a Postgres-backed deployment would see a 500 from the FastAPI layer. This is in the same active-breakage category as the A1 items.

### Site 3 — `cra_sb_discl` / `cra_sb_aggr` loader

- Callers: internal — `_load_file` is called by `main()` in the same file.
- Documented command in `CLAUDE.md`:
  ```bash
  python etl/load_cra_lending.py
  python etl/load_cra_lending.py --year 2023 --states GA TX NY
  ```
- **Classification: ACTIVE — but already correctly branched.** The SQLite `INSERT OR IGNORE` branch is unreachable when `db._IS_POSTGRES` is true.

## Approach recommendation

### Approach A — per-function rewrite to `ON CONFLICT DO NOTHING`

- **Site 2** (user_bookmarks): one-line statement change + `adapt_sql` wrap. ~20-30 min including POST /bookmarks smoke test on both backends.
- **Site 1** (state_programs): cannot be rewritten without schema work. Requires:
  1. Decide natural UNIQUE key (likely `(state, program_name)`; confirm against existing seed data for collisions).
  2. Write a migration adding the UNIQUE constraint.
  3. Update loader to handle existing duplicate rows (SQLite has been accumulating them — the current INSERT OR IGNORE never actually prevented anything).
  4. Rewrite `upsert_state_program` to `INSERT ... ON CONFLICT(state, program_name) DO NOTHING` wrapped in `adapt_sql`.
  5. Re-run `python etl/load_state_programs.py` against both backends and diff counts.
  Estimated ~1.5-2 hours.
- **Site 3**: no action; verify the existing branch is still exercised by running the loader against Postgres. ~10 min.

Pros: scope matches reality (N=2 sites actually need a change, and one of them needs schema work regardless of approach). adapt_sql stays pure-substitution — a property worth preserving because every `adapt_sql()` call site across db.py depends on it. Each commit is small and verifiable.

Cons: two commits instead of one. Site 1's schema work is unavoidable work though — it would be deferred under any approach, not saved.

### Approach B — extend `adapt_sql` to rewrite `INSERT OR IGNORE` at statement level when `_IS_POSTGRES`

- Rewrite `INSERT OR IGNORE INTO <table> (...)` → `INSERT INTO <table> (...) ... ON CONFLICT DO NOTHING`.

Pros: one commit covers every site that surfaces in the future, without per-function work.

Cons: multiple serious ones.
  1. **Does not fix Site 1.** `state_programs` has no UNIQUE constraint. A naked `ON CONFLICT DO NOTHING` on Postgres requires either a conflict target column list or a backing unique constraint for inference; without one, Postgres raises `there is no unique or exclusion constraint matching the ON CONFLICT specification`. The only way to fix Site 1 is the schema migration, regardless of approach.
  2. **adapt_sql is currently pure string substitution** (`?` → `%s`, AUTOINCREMENT → SERIAL). Statement-level rewriting is a different complexity class — requires parsing or at least careful regex, has edge cases (multi-line statements, comments, nested identifiers, PL/pgSQL in the future), and expands the test surface for every caller of adapt_sql (~40+ sites across db.py).
  3. **Does not fix the underlying raw-? problem at sites 1 and 2.** Both skip `adapt_sql` entirely. Extending adapt_sql does nothing if the caller doesn't invoke it. So even under Approach B, sites 1 and 2 still need a `cur.execute(adapt_sql(...), values)` wrap at the call site — which is most of the per-function work anyway.
  4. **Blast radius.** One adapt_sql change affects every upsert_* and every SELECT path that goes through it. Regression surface is large, gains are small (saves one line per site at best).

- Effort: ~45 min for the helper change, plus a regression sweep across every adapt_sql call site (~40). Still doesn't close Site 1.

### Approach C — hybrid (adapt_sql for sites with UNIQUE constraints; per-function + schema for sites without)

- At N=1 real fixable site (Site 2), extending adapt_sql is strictly worse than a one-line per-function rewrite: the helper change touches shared infrastructure to replace a single one-line change, while still leaving the adapt_sql-wrap problem at sites 1 and 2 in place.
- The "hybrid" only makes sense if N grows; at the current inventory count it reduces to Approach A.

### Recommendation: Approach A.

- N of real fixable sites = 1 (Site 2). Site 3 is already correctly handled. Site 1 needs schema work no matter what.
- Approach B's value scales with N. With N=1, the helper-change blast radius is not justified.
- Keep `adapt_sql()` as pure string substitution — preserves the invariant that callers can reason about it by reading the current ~6 lines.
- Bundle or split the Site 1 and Site 2 fixes at author preference. Recommend separate commits — Site 2 is a ~30-min clean fix; Site 1 is a schema migration that should not be tangled with an unrelated one-liner.

### Estimated effort to close the bug class

| Sub-task | Effort |
|---|---|
| Site 2 (user_bookmarks) rewrite + adapt_sql wrap + POST /bookmarks smoke test on Postgres and SQLite | 20-30 min |
| Site 1 (state_programs) schema migration + loader dedup + function rewrite + reload + diff | 1.5-2 hours |
| Site 3 verification (confirm `if is_pg:` branch still exercised correctly on a Postgres run) | ~10 min |
| **Total** | **~2-2.5 hours, ideally split across 2 commits / 2 sessions** |

### Sites requiring schema work (cannot be trivially rewritten)

- **`state_programs`** — no UNIQUE constraint. Proposed natural key: `(state, program_name)`. Decision deferred to the Site 1 session.

## Other bug classes surfaced during grep (flagged, not inventoried this session)

Per scope discipline: flag and move on, do not inventory in this session.

- **`cur.lastrowid` on Postgres.** `save_user_note` (`db.py:3071`) and `save_document` (`db.py:3170`) both do `note_id = cur.lastrowid` / `doc_id = cur.lastrowid` after bare INSERTs. `cur.lastrowid` is a SQLite/MySQL convention; psycopg2's `cursor.lastrowid` is typically `None` for INSERT statements unless the table has an OID, which Postgres tables no longer have by default. Both functions return the id to FastAPI callers (`api/routers/notes.py:48` returns the note id to the dashboard). Would return `None` on Postgres. Separate bug class — recommend a dedicated inventory session if/when B5 is closed.
- **Raw `?` placeholders not routed through `adapt_sql`.** Sites 1 and 2 of this inventory, plus B1/B2/B3 of the bare-INSERT section (`upsert_cdfi`, `upsert_enrollment_history`, `_upsert_lea`), all call `cur.execute(...)` without `adapt_sql()`. This is the existing adapt_sql sweep tracked in B1 and B2 of `project-pipeline.md` — not new. No action here; just noting the overlap so the B5 fix for Site 2 must include the `adapt_sql` wrap.

## Open question from caller's prompt

Step 3 of the prompt cut off mid-sentence at "Or" — presumably a third classification bullet ("Or is it dead code / only reached by archived app?"). Inferred from context and applied uniformly across the inventory (Site 2 has one DEAD caller in `archive/app.py` plus one ACTIVE in the FastAPI router; classified on the ACTIVE caller as per the chained-execute inventory precedent). If a different third bullet was intended, re-run this step.
