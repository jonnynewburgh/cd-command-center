---
title: Chained cursor.execute(...).fetch* Postgres-compat inventory
date: 2026-04-21
scope: All Python files under repo root — db.py, etl/, api/, utils/, validate.py, archive/
status: Inventory only — no edits applied. Awaiting commit-structure decision.
---

# Chained `cursor.execute(...).fetch*` inventory — 2026-04-21

## Context

SQLite's `cursor.execute()` returns the cursor, which lets you chain `.fetchone()` / `.fetchall()`. Psycopg2's `cursor.execute()` returns `None`, so any chained call crashes on Postgres with `AttributeError: 'NoneType' object has no attribute 'fetchone'`.

One chained site was fixed in `c1c562c` (`etl/fetch_990_data.py:749-750`, split into two statements). During that work ~14 more sites were spotted across the ETL scripts but left untouched. This doc is a fresh full sweep.

## Grep patterns run

Against `*.py` under the repo root:

- `\.execute\([^)]*\)\.fetchone\(\)` — single-line, no nested parens
- `\.execute\([^)]*\)\.fetchall\(\)`
- `\.execute\([^)]*\)\.fetchmany\(`
- `\)\.fetchone\(\)` / `\)\.fetchall\(\)` with `-B 1` — catches multiline execute+fetch where the closing `)` lands on its own line (the common multi-line case)
- `\)\s*\.\s*fetch(one|all|many)\(` — whitespace-tolerant
- `^\s*\.fetch(one|all|many)\(` — defensive: fetch as a bare continuation line (no matches; not an idiom in this repo)

Cross-checked against the three files that use `.fetchone()` / `.fetchall()` but did **not** match the chained pattern — `db.py`, `validate.py`, `utils/db_backup.py` — all already use the split two-statement form.

`api/`, `api/routers/`, `utils/` (other than `db_backup.py`), and `archive/` have **zero** matches.

## Summary

**14 chained sites across 6 ETL files.** All but one are in the end-of-`main()` summary blocks that print row counts after the real work is done. The one exception is the `--limit` branch of `compute_financial_ratios.compute_ratios()`.

| Classification | Count |
|---|---:|
| CHAIN-ONLY | 13 |
| CHAIN+PLACEHOLDER (needs adapt_sql wrap + split) | 1 |
| CHAIN+OTHER | 0 |

| Exposure | Count |
|---|---:|
| ACTIVE — scheduled or prerequisite to scheduled work | 7 |
| LATENT — documented CLI but not scheduled / rarely run | 7 |
| DEAD | 0 |

Note on ACTIVE vs LATENT: `DATABASE_URL` is exported to a Postgres URL in `~/.bashrc` per `DATA_REFRESH_SCHEDULE.md`, so any manual `python etl/foo.py` run in the dev's shell hits Postgres. ACTIVE here means "documented in `CLAUDE.md` commands as part of an ongoing workflow and likely to be run again soon"; LATENT means "one-shot backfill, manual-PDF ingest, or partially broken upstream — low probability of a near-term Postgres run, but the bug is still real."

`run_pipeline.py` does **not** invoke any of these six scripts, so the scheduled annual pipeline is not a vector. None of the 14 sites is reached via a FastAPI route.

## Full site table

| # | file:line | Enclosing function | Chained expression | Classification | Exposure |
|---:|---|---|---|---|---|
| 1 | `etl/fetch_990_irs.py:636` | `main()` | `cur.execute("SELECT COUNT(*) FROM irs_990").fetchone()[0]` | CHAIN-ONLY | ACTIVE |
| 2 | `etl/fetch_990_irs.py:637-639` | `main()` | `cur.execute("SELECT COUNT(*) FROM irs_990 WHERE data_source = 'IRS'").fetchone()[0]` (multi-line) | CHAIN-ONLY | ACTIVE |
| 3 | `etl/fetch_bmf_eins.py:603` | `main()` | `cur.execute("SELECT COUNT(*) FROM schools WHERE is_charter = 1").fetchone()[0]` | CHAIN-ONLY | ACTIVE |
| 4 | `etl/fetch_bmf_eins.py:604-606` | `main()` | `cur.execute("SELECT COUNT(*) FROM schools WHERE is_charter = 1 AND ein IS NOT NULL AND ein != ''").fetchone()[0]` (multi-line) | CHAIN-ONLY | ACTIVE |
| 5 | `etl/fetch_bmf_eins.py:607` | `main()` | `cur.execute("SELECT COUNT(*) FROM irs_990").fetchone()[0]` | CHAIN-ONLY | ACTIVE |
| 6 | `etl/fetch_bmf_eins.py:608-610` | `main()` | `cur.execute("SELECT COUNT(*) FROM irs_990 WHERE data_source = 'IRS BMF'").fetchone()[0]` (multi-line) | CHAIN-ONLY | ACTIVE |
| 7 | `etl/fetch_lea_accountability.py:291` | `main()` | `cur.execute("SELECT COUNT(*) FROM lea_accountability").fetchone()[0]` | CHAIN-ONLY | LATENT |
| 8 | `etl/fetch_lea_accountability.py:292-294` | `main()` | `cur.execute("SELECT COUNT(*) FROM lea_accountability WHERE graduation_rate IS NOT NULL").fetchone()[0]` (multi-line) | CHAIN-ONLY | LATENT |
| 9 | `etl/fetch_nmtc_award_books.py:287-290` | `main()` | `cur.execute(db.adapt_sql("SELECT id FROM cde_allocations WHERE cde_name = ? AND allocation_year = ?"), (rec["cde_name"], year)).fetchone()` (multi-line, already `adapt_sql`-wrapped — only the chain is broken) | CHAIN-ONLY | LATENT |
| 10 | `etl/fetch_nmtc_award_books.py:318` | `main()` | `cur.execute("SELECT COUNT(*) FROM cde_allocations").fetchone()[0]` | CHAIN-ONLY | LATENT |
| 11 | `etl/fetch_nmtc_award_books.py:319-321` | `main()` | `cur.execute("SELECT COUNT(*) FROM cde_allocations WHERE allocation_amount IS NOT NULL AND allocation_year > 0").fetchone()[0]` (multi-line) | CHAIN-ONLY | LATENT |
| 12 | `etl/compute_financial_ratios.py:70` | `compute_ratios(limit)` — only reached when `--limit` is passed | `conn.execute(eins_sql).fetchall()` (no `?`; `LIMIT {limit}` is an f-string interp) | CHAIN-ONLY | LATENT (ACTIVE only on the `--limit` test path) |
| 13 | `etl/patch_pct_asian.py:117-120` | `main()` | `conn.execute(f"SELECT nces_id, state FROM schools WHERE state IN ({placeholders})", args.states).fetchall()` — `placeholders` is a comma-joined string of `?` chars passed to raw (un-adapted) SQL | **CHAIN+PLACEHOLDER** | LATENT |
| 14 | `etl/patch_pct_asian.py:122` | `main()` | `conn.execute("SELECT nces_id, state FROM schools").fetchall()` | CHAIN-ONLY | LATENT |

### Notes on individual sites

- **Site 9** (`fetch_nmtc_award_books.py:287-290`) is the only chained call inside a real data path (not a summary block). It's inside a per-record loop that upserts CDEs. `adapt_sql` is already wrapped around the SQL, so only the chain split is needed. This is also the only chained site on a warm code path in that script — the other two sites (10, 11) are the end-of-main summary.
- **Site 12** (`compute_financial_ratios.py:70`) only fires when `--limit N` is passed. On the normal unlimited-EINs path this branch is skipped entirely. The bug is latent on the production path and active only in dev testing.
- **Site 13** (`patch_pct_asian.py:117-120`) is the only CHAIN+PLACEHOLDER site. The `?` characters are constructed at runtime (`",".join("?" * len(args.states))`) and interpolated into an f-string, then passed to `conn.execute()` without any `adapt_sql(...)` wrap. On Postgres this would raise `psycopg2.errors.SyntaxError` on the `?` before ever reaching the chain bug — so it is *doubly* broken on Postgres. The fix needs both `adapt_sql(...)` around the f-string and a split into two statements.

### Exposure reasoning per file

- **`fetch_990_irs.py`** — ACTIVE. IRS primary-source 990 fetcher, documented in `CLAUDE.md`; alternative path to ProPublica's `fetch_990_data.py`. Likely to be re-run as part of ongoing 990 work.
- **`fetch_bmf_eins.py`** — ACTIVE. EIN matcher, documented as the prerequisite step before `fetch_990_data.py` (the comment on line 624 literally tells the user to run that script next). High probability of near-term re-run.
- **`fetch_lea_accountability.py`** — LATENT. Script notes Urban Institute API is returning 500 for proficiency scores; only grad rates currently work. `run_pipeline.py` uses `fetch_edfacts_auto.py` for LEA data instead, so this script isn't on any recurring path.
- **`fetch_nmtc_award_books.py`** — LATENT. Manual PDF-scraping script for CDFI Fund award books. Annual-release cadence at most; currently no pending annual refresh.
- **`compute_financial_ratios.py`** — Site 12 is LATENT on the production path. Note: the rest of `compute_ratios()` uses `pd.read_sql_query` (Postgres-compatible) — the chained-execute bug lives only in the `--limit` test branch.
- **`patch_pct_asian.py`** — LATENT. One-time backfill patch script (per commit `06bcd34`). Purpose: add `pct_asian` to already-loaded school rows. Probably already run once against SQLite; unlikely to be re-run.

## Out-of-scope observations (noted, not fixed)

Found during the sweep but deliberately not touched this phase:
- Other `upsert_*` functions in `db.py` without Postgres-compat fixes (nmtc, cde, cdfi, state_program, enrollment_history, financial_ratios, lea_accountability) — tracked in `docs/debug/db_refactor_triage_2026-04-19.md`.
- `silent exception handlers` — also out of scope.
- `site 13`'s raw-`?` placeholder bug — this one is in scope because the chain fix requires touching the same expression. The `adapt_sql` wrap rides along.

## Proposed commit structure — trade-offs

### Option 1: one commit covering all 14 sites

**Pros:**
- Single searchable commit; matches the phrasing of prior Postgres-compat sweeps in this repo (`c1c562c` bundled 15 sites across `db.py` + two ETL files).
- One round of verification at the end; one PR message to write.
- Historically the repo has grouped by bug class (see `c1fc5b3`, `c87742b`, `0b63cd7`, `8a8d848`), not by file.

**Cons:**
- Six files touched in one commit; if verification surfaces one site that breaks in a non-chained way, the whole commit is entangled.
- Bisect less useful — `git blame` on any summary-print line points at the whole sweep, not the per-file fix.

### Option 2: one commit per ETL script (6 commits)

**Pros:**
- Smaller, more readable diffs — each file's fix is self-contained.
- Per-file verification is natural: run the script, confirm exit 0, commit.
- Good bisect granularity.

**Cons:**
- Six very similar commit messages; feels repetitive given the uniform bug class.
- Six pushes (or one push of six commits) — trivial but slightly noisier in the git log.
- The CHAIN+PLACEHOLDER site in `patch_pct_asian.py` would naturally bundle with its sibling CHAIN-ONLY site in the same file, mixing fix-shapes inside one commit.

### Option 3: split by exposure — ACTIVE first, LATENT second, DEAD triaged separately

**Pros:**
- Ships the fix to the scripts most likely to be run next as one discrete unit; LATENT fixes become a lower-priority follow-up.
- If the LATENT scripts (e.g. `fetch_lea_accountability` with its 500'd upstream) turn out to need deeper work, they can be deferred without blocking the ACTIVE fix.

**Cons:**
- Splits `fetch_nmtc_award_books.py` and `patch_pct_asian.py` into the same commit even though they're unrelated scripts — coherence by exposure, not by subject.
- Exposure classification is a judgment call, not a strict property of the code — future readers might disagree.
- Creates more pressure on the "first" commit since it has multiple files in it.
- No DEAD sites, so this really is just a two-commit split, which doesn't add much over option 2 if option 2 already exists.

## Recommendation

**Option 2 — one commit per ETL script, six commits total.**

Reasoning:

1. Unlike `c1c562c` (which touched `db.py` as the core fix plus two ETL call-sites of those `db.py` functions), this sweep only touches ETL scripts that are mutually independent. There's no shared reviewable locus.
2. Each script's fix is small enough that per-file diffs are very easy to read; bundling them only saves commit-message boilerplate.
3. Per-file verification matches how `CLAUDE.md` documents each script — one CLI entry point per script. Running the entry point, confirming exit 0 on both backends, and committing maps 1:1 to files.
4. The CHAIN+PLACEHOLDER site (patch_pct_asian.py:117-120) does ride along with its file's CHAIN-ONLY sibling, and the commit body will call that out clearly.
5. Exposure-ordered execution still works inside Option 2 — apply ACTIVE files first (`fetch_990_irs.py`, `fetch_bmf_eins.py`), then LATENT. Option 3's value is preserved without needing to bundle unrelated files into one commit.

Suggested order, picked to front-load the ACTIVE fixes:

1. `etl/fetch_bmf_eins.py` — 4 sites, CHAIN-ONLY, ACTIVE
2. `etl/fetch_990_irs.py` — 2 sites, CHAIN-ONLY, ACTIVE
3. `etl/compute_financial_ratios.py` — 1 site, CHAIN-ONLY (test path), mixed-exposure
4. `etl/fetch_lea_accountability.py` — 2 sites, CHAIN-ONLY, LATENT
5. `etl/fetch_nmtc_award_books.py` — 3 sites, CHAIN-ONLY, LATENT
6. `etl/patch_pct_asian.py` — 2 sites (1 CHAIN-ONLY + 1 CHAIN+PLACEHOLDER), LATENT

## Verification plan (per file, at fix time)

For each file, I'll run the file's documented CLI entry point against both backends and confirm exit 0 + same behavioral output.

| File | Entry point |
|---|---|
| `fetch_bmf_eins.py` | `python etl/fetch_bmf_eins.py --dry-run` (skip download) or `--limit 10 --dry-run` |
| `fetch_990_irs.py` | `python etl/fetch_990_irs.py --dry-run --limit 5` |
| `compute_financial_ratios.py` | `python etl/compute_financial_ratios.py --limit 10` (hits site 12) |
| `fetch_lea_accountability.py` | `python etl/fetch_lea_accountability.py --states GA --dry-run` |
| `fetch_nmtc_award_books.py` | `python etl/fetch_nmtc_award_books.py --dry-run` (must verify PDFs exist in `data/raw/`; if not, direct python -c call into `main()` after seeding cde_allocations) |
| `patch_pct_asian.py` | `python etl/patch_pct_asian.py --states GA --year 2023` — exercises both site 13 (with `--states`) and site 14 (without) |

Each run under both `DATABASE_URL=postgresql://...` (active) and `unset DATABASE_URL` (SQLite).

## Awaiting decision

1. **Commit structure** — Option 1, 2, or 3? (My recommendation: Option 2.)
2. **Scope confirmation** — fix all 14 chained sites; on site 13, apply both the split and the `adapt_sql` wrap; do not touch anything else. OK?
3. **Verification depth** — for LATENT scripts whose CLI entry point requires external data (e.g. PDF inputs for `fetch_nmtc_award_books.py`), is it acceptable to call `main()` via `python -c` after seeding the minimum DB state, or do you want a full real-data run?
