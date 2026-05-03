---
title: CODEX audit follow-up — third pass (P1 #5, #8, #9 + P2 #10b + nmtc state)
date: 2026-05-03
scope: Continuation of `codex_audit_followup_2026-04-30.md`
status: every CODEX audit item is now either shipped or has a documented "won't fix" rationale
---

# CODEX audit follow-up — 2026-05-03

The 2026-04-30 doc closed out P0 #3, P0 #4, P1 #6, P2 #11 and listed
4 remaining deferred items (P1 #5, P1 #8, P1 #9, P2 #10b) plus a data
finding (`nmtc_projects.state` storing full names). This session
shipped all five.

## Shipped this session

### `nmtc_projects.state` normalization ✓ (e1c39c5)

The CDFI Fund publishes NMTC project data with full state names ("Georgia"),
while every other facility table uses 2-letter codes. Result:
`/nmtc/projects?states=GA` returned 0 rows even when 149 GA projects existed.

- New `STATE_NAME_TO_ABBREV` map + `state_name_to_abbrev()` helper in
  `utils/state_fips.py`. Accepts 2-letter codes (passed through) or
  full state names (case-insensitive). 50 states + DC + 4 territories.
- `etl/load_nmtc_data.py:load_projects` now normalizes through the
  helper so future loads write 2-letter codes.
- One-shot backfill of all 8024 existing rows; every distinct value
  mapped cleanly.
- Verified: `/nmtc/projects?states=GA` -> 149 (was 0); CA=636, NY=303,
  TX=309 match the audit's row counts.

`nmtc_coalition_projects.state` has its own bug (truncated 2-char codes
like 'TE'/'PE'/'LO') — different loader, different root cause, separate
session.

### P2 #10b — `cra_assessment_areas` empty ✓ (c33f7d3)

Diagnosis: the FFIEC zips on disk (1996-2024) don't ship a dedicated
`Agg_Assessment_Area.dat` file, AND `fetch_cra_data.py:--dir` mode
only loads transmittal files (the assessment-area branch is single-year
mode only).

The data is already in the DB though — every row of `cra_sb_discl` is
keyed by `(respondent_id, year, state_fips, county_fips, msa_code)`.
Each unique tuple IS an assessment area.

- New `etl/derive_cra_assessment_areas.py` aggregates `cra_sb_discl`,
  joins `cra_institutions` for `institution_name`, normalizes
  `state_fips` -> 2-letter, sets `area_type = 'MSA' | 'County'` based
  on `msa_code` non-zeroness, synthesizes `assessment_area_name` for
  display.
- Idempotent: deletes the year range it's about to write before
  inserting.
- Backfill: **156,313 rows** across all 21 years in `cra_sb_discl`,
  100% institution-name fill.

### P1 #5 — pandas roundtrip on list endpoints ✓ (5d33025)

Bench from this session disproved the audit's framing. On the actual
schools page, the `df.to_json + json.loads` roundtrip is ~2× FASTER
than a pure-Python NaN/NaT scrub loop (pandas' to_json is C-implemented;
Python loops over hundreds of thousands of cells are not):

```
rows  pandas-roundtrip  python-scrub-loop  ratio
  25            1.1 ms             2.2 ms   0.5x
 100            2.6 ms             3.8 ms   0.7x
 500            6.8 ms            14.0 ms   0.5x
2000           27.8 ms            64.3 ms   0.4x
```

So the audit's "Action: cursor rows + plain dict conversion" would be a
regression here, not a win. The real concrete issue was a latent
format-drift risk: `df.to_json(orient='records')` defaulted to
`'epoch'` (millisecond ints), which pandas formally deprecated in 4.x
and will remove. Every list endpoint was logging a Pandas4Warning.

Fix: pin `date_format='iso'`. ISO 8601 is what the Next.js consumer
already expects; this also locks the timestamp contract before pandas
flips the default under us. Test-run warning count: 45 -> 17.

### P1 #9 — pg_trgm-backed search ✓ (e2676d8)

The audit's complaint about "LIKE leading-wildcards not using indexes"
understated the problem: LIKE on Postgres is also case-sensitive, so
`search('atlanta')` returned 0 ever, only `search('Atlanta')` worked.
The dashboard's autocomplete was effectively broken for any query that
wasn't perfectly capitalized.

- `_search_table` now emits ILIKE on Postgres, LIKE on SQLite (where
  it's already case-insensitive).
- `init_db` creates `pg_trgm` extension + 15 GIN trigram indexes on
  the searched name/city columns. Wrapped in `_try_exec` so a hosted
  Postgres without superuser rights gracefully degrades.
- `search_all` dropped state and structural-ID columns (`nces_id`,
  `census_tract_id`) from the OR'd substring search — they had no
  trigram index and forced Postgres into a full Seq Scan even when
  the indexed columns matched. They're filter columns, not text-search
  columns.

Verified
- EXPLAIN ANALYZE: schools `'atlanta'` query went from a 5056-cost Seq
  Scan (274 ms, 97593 rows filtered out) to a 583-cost Bitmap Heap
  Scan (2.2 ms, BitmapOr over 3 trigram indexes).
- `search_all()` cold latency: 332-450 ms -> 65-97 ms (~5× faster).
- `search_all('atlanta')` went from 0 results to 280.

### P1 #8 — Alembic migrations baseline ✓ (08e4efc)

Bootstrap chassis without retiring `init_db` yet:

- `alembic.ini` + `migrations/env.py` wired to read the same
  `DATABASE_URL` env var the app honors. Normalizes `postgres://` /
  `postgresql://` short forms to `postgresql+psycopg2://` (what
  SQLAlchemy expects).
- `migrations/versions/542621587619_baseline_*.py` — empty no-op
  revision declaring the current schema as the baseline. Future
  revisions chain off it.
- `requirements.txt`: alembic>=1.13, sqlalchemy>=2.0.
- `db.init_db` docstring + `CLAUDE.md` commands list updated to point
  new schema changes at migrations.
- Local Postgres stamped at the baseline; `alembic current` reports
  `542621587619 (head)`.

Workflow from here
- Existing dev/prod DBs: `alembic stamp head` once.
- New DBs: still bootstrap via `db.init_db()`, then `alembic stamp head`.
- New schema changes: `alembic revision -m "..."` and op.execute() /
  op.create_table() — do NOT add new CREATE/ALTER to init_db.

Out of scope (deliberate)
- Backfilling per-table CREATE TABLE migrations from `init_db` for ~30
  tables — multi-session refactor; needs SQLAlchemy models or
  hand-written op.create_table() calls.
- "Auto-stamp on import if schema is empty" — too magical for the
  value, easy to invoke manually.

## Final status of the original CODEX audit

| Item | Status | Commit |
|---|---|---|
| P0 #1 schools API drift | ✓ | 3e285a0 (2026-04-27) |
| P0 #2 `_cached` was a no-op | ✓ | 3e285a0 + 4e639a0 (cache-key bug fix) |
| P0 #3 nearby search | ✓ | 33da49b (2026-04-30) |
| P0 #4 pagination + lean projections | ✓ | 4e639a0 |
| P1 #5 pandas on request path | ✓ (deprecation fix; bench disproved the deeper change) | 5d33025 |
| P1 #6 batched ETL upserts | ✓ | a855cf3 |
| P1 #7 missing indexes | ✓ | 3e285a0 |
| P1 #8 Alembic migrations baseline | ✓ | 08e4efc |
| P1 #9 search infra (pg_trgm + ILIKE) | ✓ | e2676d8 |
| P2 #10 empty domain tables | ✓ partial — see below | c33f7d3 (cra_assessment_areas) |
| P2 #11 minimal tests/ suite | ✓ | 307de0d |

P2 #10 partial: `cra_assessment_areas` is now populated (156K rows
derived from `cra_sb_discl`). The audit's other "empty in audit
snapshot" tables (`irs_990_history`, `federal_audits`,
`headstart_programs`) were already explained by SQLite/Postgres
snapshot drift in the 2026-04-27 doc — populated on Postgres, just
empty in the snapshot the auditor read.

The "data-status endpoint" the audit suggested as a next step
(report row counts and last refresh time per domain) is still on the
backlog — not in the original audit list, just a nice-to-have.

## Suggested next session

Nothing on the original CODEX audit. New surfaces that have come up
during this work:

| Item | Why | Estimated session length |
|---|---|---|
| `nmtc_coalition_projects.state` truncated codes | Discovered during the nmtc state normalization. Different loader, different root cause. | 1 hour |
| Backfill `init_db` into Alembic migrations | Real value of the migrations layer doesn't land until init_db is retired. SQLAlchemy models or 30 hand-written `op.create_table()` calls. | 3-5 hours |
| `data-status` endpoint (counts + last-refresh per domain) | Audit's P2 nice-to-have — would surface "loader hasn't run for state X" issues earlier. | 1-2 hours |
| Drop the pandas non-SQLAlchemy connection warning | Switch `pd.read_sql_query` calls to use a SQLAlchemy engine wrapper. Touches every read; same pattern as P1 #5 deeper fix. | 2-3 hours |

Take your pick — none are pressing. The dashboard request path is
healthy, the ETL throughput is fine, the test + migration chassis
are in place.
