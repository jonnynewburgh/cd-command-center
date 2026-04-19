---
title: is_charter literal inventory
date: 2026-04-18
scope: All Python files under repo root — db.py, etl/, utils/, api/, validate.py, archive/, plus the in-progress db/ package
status: Inventory only — no edits applied
---

# `is_charter` literal inventory — 2026-04-18

## Context

After the `c87742b` schools-compat fix, Phase 3 set out to fix a presumed `is_charter = 1` boolean-comparison bug in `get_school_summary`. Verification showed the bug did not exist — `schools.is_charter` is `INTEGER` on both backends (Postgres `int4`, SQLite `INTEGER`), so `WHERE is_charter = 1` works correctly everywhere. See `nces_diagnosis_2026-04-18.md` § "Correction" for the write-up.

This inventory sweeps every place `is_charter` appears in code, categorizes the usage, and confirms no variants (`= TRUE`, `= FALSE`, `IS TRUE`, `::boolean`, Python `True/False` writes) exist anywhere. **Nothing here needs fixing** under the current schema — the inventory is a cross-check, not a punch list.

## Grep patterns run

- `is_charter` (all occurrences, `*.py`)
- `is_charter\s*=\s*(TRUE|FALSE|True|False)` case-insensitive
- `is_charter\s+IS\s+(TRUE|FALSE)`
- `is_charter::` (Postgres cast syntax)
- `UPDATE.*is_charter` (mutation sites)
- `is_charter.*(True|False)` (any nearby Python bool literal)

## Summary by category

| Category | Count | Risk |
|---|---:|---|
| `WHERE is_charter = 1` (SELECT filter) | 8 | None — INT on both backends |
| `UPDATE ... WHERE is_charter = 1` | 3 | None — same |
| `conditions.append("is_charter = 1")` / fragment builder | 3 | None — same |
| `record["is_charter"] = 1` (write) | 2 | None — writes INT |
| `is_charter = 1 if charter_val == 1 else 0` (Python variable) | 1 | None — writes INT |
| Schema / DDL (`is_charter INTEGER DEFAULT 0`) | 4 | None — defines the column |
| Python-side comparison on DataFrame column (`== 1`, `== 0`) | 8 | None — archive Streamlit only |
| Python `.get("is_charter", 1)` default value | 3 | None — utils/maps.py |
| Boolean-literal forms (`= TRUE`, `= FALSE`, `IS TRUE`, `::boolean`) | **0** | — |
| Python `True`/`False` writes into `is_charter` | **0** | — |

**Key finding: zero boolean-literal forms exist.** Every comparison and every write uses integer `0`/`1`, which is backend-safe under the current schema.

---

## Full listing

### `db.py` (shipped — module-per-file)

| Line | Context | Category |
|---:|---|---|
| 119 | `is_charter INTEGER DEFAULT 0,  -- 1 = charter school, 0 = traditional public` | Schema DDL |
| 146 | `# Copy data from old table, setting is_charter=1 since old table was charter-only` | Comment (migration logic) |
| 151 | `grade_low, grade_high, is_charter,` | SELECT column list during charter_schools → schools migration |
| 169 | `# Add is_charter column to schools table if it was created without it` | Comment |
| 170 | `_try_exec(cur, "ALTER TABLE schools ADD COLUMN is_charter INTEGER DEFAULT 0")` | Migration DDL |
| 359 | `CREATE INDEX IF NOT EXISTS idx_schools_is_charter ON schools(is_charter)` | Index DDL |
| 1353 | `charter_only: if True, only return charter schools (is_charter=1)` | Docstring |
| 1360 | `conditions.append("s.is_charter = 1")` | SELECT filter fragment — `search_schools()` |
| 1510 | `charter_filter = "WHERE is_charter = 1" if charter_only else ""` | SELECT filter fragment — `get_school_summary()` |
| 2087 | `"""Backward-compatible wrapper: inserts with is_charter=1."""` | Docstring — `upsert_charter_school()` |
| 2089 | `record["is_charter"] = 1` | **Write — integer literal** |
| 2571 | `cur.execute("SELECT COUNT(*) FROM schools WHERE ein IS NOT NULL AND is_charter = 1")` | Count query |
| 4326 | `"SELECT nces_id, school_name, enrollment, is_charter, "` | SELECT column list (no comparison) |

### `validate.py`

| Line | Context |
|---:|---|
| 269 | `charters = _scalar("SELECT COUNT(*) FROM schools WHERE is_charter = 1")` |

### `etl/` (ETL scripts)

| File | Line | Context | Notes |
|---|---:|---|---|
| `fetch_990_data.py` | 334 | `conditions = ["is_charter = 1", "school_status = 'Open'"]` | SELECT filter fragment |
| `fetch_990_data.py` | 450 | `"WHERE lea_id = ? AND is_charter = 1"` | UPDATE filter |
| `fetch_990_data.py` | 456 | `"WHERE school_name = ? AND state = ? AND is_charter = 1"` | UPDATE filter |
| `fetch_bmf_eins.py` | 346 | `conditions.append("is_charter = 1")` | SELECT filter fragment |
| `fetch_bmf_eins.py` | 393 | `charter_filter = "" if all_schools else "AND is_charter = 1"` | UPDATE filter |
| `fetch_bmf_eins.py` | 603 | `SELECT COUNT(*) FROM schools WHERE is_charter = 1` | Count query |
| `fetch_bmf_eins.py` | 605 | `SELECT COUNT(*) FROM schools WHERE is_charter = 1 AND ein IS NOT NULL AND ein != ''` | Count query |
| `fetch_nces_schools.py` | 9 | `with is_charter=1 or 0 based on the API's charter field.` | Module docstring |
| `fetch_nces_schools.py` | 276 | `- is_charter flag from API charter field` | Docstring |
| `fetch_nces_schools.py` | 303 | `is_charter = 1 if charter_val == 1 else 0` | **Python var — integer** |
| `fetch_nces_schools.py` | 325 | `"is_charter": is_charter,` | Record dict — integer |
| `fetch_nces_schools.py` | 460 | `sum(1 for r in all_records if r.get("is_charter") == 1)` | Python count |
| `fetch_nces_schools.py` | 461 | `sum(1 for r in all_records if r.get("is_charter") == 0)` | Python count |
| `load_scsc_cpf.py` | 83 | `"SELECT nces_id, school_name FROM schools WHERE state = 'GA' AND is_charter = 1"` | SELECT filter |

### `utils/maps.py` (archived Streamlit map helpers)

| Line | Context |
|---:|---|
| 202 | `is_charter = getattr(row, "is_charter", 1)` |
| 203 | `if is_charter:` |
| 210 | `prefix = "Charter" if is_charter else "Public"` |
| 248 | `if "is_charter" in df.columns:` |
| 249 | `return (df["is_charter"] == 1).any()` |
| 250 | `return True  # old data without is_charter column assumed to be charters` |
| 266 | `is_charter = _get("is_charter", 1)` |
| 267 | `school_type = "Charter" if is_charter else "Public"` |
| 271 | `if is_charter:` |

These are all Python-side usages on already-materialized rows/DataFrames, not SQL. Safe either way.

### `archive/app.py` (archived Streamlit UI — not shipped)

| Line | Context |
|---:|---|
| 316–322 | DataFrame mask `schools_df["is_charter"] == 0` / `== 1` |
| 576–577 | Column list + rename `is_charter → "Charter"` |
| 628–632 | `school.get("is_charter") == 1` predicate |
| 650–715 | `is_charter = school.get("is_charter") == 1` + display branches |
| 1680 | Column rename in export |

All pandas-side comparisons on INT columns. Not SQL.

### `db/` (unshipped in-progress refactor package)

These files are currently **untracked** in git (`?? db/` in `git status`) — they appear to be a work-in-progress split of `db.py` into a package (`db/schema.py`, `db/queries.py`, `db/mutations.py`). They mirror the `db.py` call sites one-for-one with the same `is_charter = 1` integer semantics:

| File | Line | Context |
|---|---:|---|
| `db/schema.py` | 51 | `is_charter INTEGER DEFAULT 0, ...` |
| `db/schema.py` | 78 | migration comment (copy from `db.py:146`) |
| `db/schema.py` | 83 | `grade_low, grade_high, is_charter,` |
| `db/schema.py` | 101–102 | ALTER TABLE ADD COLUMN |
| `db/schema.py` | 291 | CREATE INDEX |
| `db/queries.py` | 41 | docstring |
| `db/queries.py` | 48 | `conditions.append("s.is_charter = 1")` |
| `db/queries.py` | 184 | `charter_filter = "WHERE is_charter = 1" if charter_only else ""` |
| `db/queries.py` | 875 | `SELECT COUNT(*) FROM schools WHERE ein IS NOT NULL AND is_charter = 1` |
| `db/mutations.py` | 147 | wrapper docstring |
| `db/mutations.py` | 149 | `record["is_charter"] = 1` |

All integer-literal, same shape as `db.py`. No surprises to flag.

---

## Variants explicitly checked (all zero hits)

```
grep -niE 'is_charter\s*=\s*(TRUE|FALSE|True|False)'     # 0 matches in code
grep -niE 'is_charter\s+IS\s+(TRUE|FALSE)'                # 0 matches
grep -nE  'is_charter::'                                   # 0 matches
grep -nE  'UPDATE.*is_charter\s*='                         # 0 UPDATE-SET assignments
```

The only hit on the TRUE/FALSE pattern is inside `docs/debug/nces_diagnosis_2026-04-18.md` (the correction note itself, not code).

## Conclusion

Under the current schema (`is_charter INTEGER DEFAULT 0` on both backends), every `is_charter` usage in the repo is safe. No `db.py` change needed; no ETL change needed; the previous concern is fully retired.

If the schema is ever migrated to actual `BOOLEAN` on Postgres, 16 call sites would need to be updated (all the `= 1` and `= 0` forms above); `record["is_charter"] = 1` writes would need to become `True`/`False`; the `conditions.append("is_charter = 1")` fragments would need conditional dialect handling or a CAST. That's a schema-migration-sized change, not a bug fix — out of scope for now.
