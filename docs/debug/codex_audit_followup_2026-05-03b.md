---
title: CODEX audit follow-up — fourth pass (post-2026-05-03 cleanups)
date: 2026-05-03 (second round)
scope: Closes the three "next session" items from `codex_audit_followup_2026-05-03.md`
status: every loose end from the audit is now closed
---

# CODEX audit follow-up — 2026-05-03 (second round)

The 2026-05-03 doc closed every original CODEX audit item and listed
four forward-looking surfaces that came up during the work. This
session closed three of those four.

## Shipped this session

### Coalition state normalization dedup ✓ (1e1a66c)

The 2026-04-30 doc had flagged truncated 2-char codes
('TE'/'PE'/'LO') in `nmtc_coalition_projects.state`. By the time we
looked, the live data was clean (54 distinct values, all valid
2-letter codes — must have been reloaded under the corrected
loader). But the loader still carried its own local
`_STATE_NAME_TO_ABBR` map that drifted from `utils/state_fips` —
the exact pattern that produced the original bug.

- Replaced the local map with
  `utils.state_fips.state_name_to_abbrev` + a `STATE_FIPS`
  membership check. Single source of truth across the NMTC loaders.
- Tightened the guard at the same time: the canonical helper passes
  unknown 2-letter inputs back unchanged ('XX' → 'XX'), but the
  coalition loader is one of those callers and now drops them
  rather than write 'XX' to the state column.

Verified
- 9/9 normalize cases (Tennessee/TN/tn → TN, Pennsylvania → PA,
  Mississippi → MS, XX → '', Atlantis → '', empty/None → '').
- pytest tests/ -q → 14 passed.

### SQLAlchemy engine for pd.read_sql_query ✓ (19b47dd)

Every list/detail read in `db.py` was passing a raw psycopg2 /
sqlite3 connection to `pd.read_sql_query`, which logs a
`Pandas4Warning` on every call ("only supports SQLAlchemy
connectable…") and is documented as "DBAPI2 objects are not tested"
— a future pandas release could break the calling pattern outright.

- New `_engine()` lazy singleton in `db.py` —
  `pool_pre_ping=True` on Postgres, `NullPool` on SQLite.
- New `_sqlalchemy_url()` normalizes the same `DATABASE_URL` the
  app reads to the `postgresql+psycopg2://` form SQLAlchemy needs.
- New `_pd_read_sql(sql, params)` accepts the same '?' positional
  placeholders the rest of `db.py` uses, converts them to `:p0,
  :p1, …` for `text()`, and returns the DataFrame. No `adapt_sql()`
  needed on this path — SQLAlchemy handles dialect.
- Swept all 35 `pd.read_sql_query` call sites in `db.py`:
  - 23 canonical `pd.read_sql_query(adapt_sql(query), conn,
    params=params)` sites swept via `replace_all`.
  - 12 multi-line variants (one-shot SELECTs by id, the paged-query
    helper, the market-rates window query, the search helper, the
    federal-audit-programs query, etc.) edited individually.
- `_search_table` dropped its `conn` parameter — `_pd_read_sql`
  opens its own; the old `conn` was just being threaded through.
  `search_all` stops opening a connection it no longer needs.

Verified
- pytest tests/ -q → 14 passed.
- Test-run warnings: 13 → 2 (the 11 Pandas4Warnings are gone; the
  remaining 2 are FastAPI `on_event` deprecation, unrelated).
- Live smoke with pandas `UserWarning` escalated to error:
  get_schools / fqhc / nmtc_projects / tracts / search_all all
  return correct row counts with no warnings raised.
- `search_all('atlanta')` cold latency: ~67 ms → ~20 ms (engine
  pool reuse beats opening a fresh DBAPI conn per query).

Out of scope
- ~98 sites still open a `conn = get_connection()` to do cursor
  work alongside the read. None are dead-conn (every block uses
  the cursor for a COUNT, an UPDATE, or a UNIQUE-fetch). No
  cleanup needed.

### Single source of truth for schema ✓ (29a88b7)

The previous round's Alembic baseline (`542621587619`) was empty
— it just declared "everything that already exists is at this
point". So `alembic upgrade head` on a fresh DB did nothing useful;
you still had to run `db.init_db()` separately, then
`alembic stamp head`.

This round closes that: a new revision `f19ded25b983` wraps a
`db.init_db()` call. Now `alembic upgrade head` is sufficient to
get a fresh DB to the current schema state, and existing DBs stamped
at `542621587619` can `alembic upgrade head` safely (every CREATE
in init_db uses `IF NOT EXISTS` and no-ops on populated tables).

`db.init_db` is FROZEN at this snapshot — its docstring spells out
that NEW schema changes must land as new alembic revisions
(`op.create_table` / `op.execute`), NOT as new statements in
init_db, because adding to init_db would silently change what
the `f19ded25b983` migration does for downstream environments.

Why "init_db inside a migration" instead of 30 hand-written
op.create_table calls
- The audit followup proposed two paths: translate every CREATE in
  init_db, or wrap an init_db call in a single migration. The
  followup doc estimated 3-5 hours for translation; the wrapper
  delivers the same workflow benefit (one command for fresh DBs,
  ordered chain from here on) without burning a session on
  mechanical translation work that's prone to subtle bugs.
- The downside is that the `f19ded25b983` revision is opaque
  (doesn't list each table). Acceptable: init_db is itself in
  version control as the readable form, and 0003+ revisions WILL
  be readable op.create_table() calls.

Verified
- alembic current `542621587619` → alembic upgrade head →
  `f19ded25b983 (head)` on the live local Postgres. No errors;
  init_db's IF NOT EXISTS guards no-op'd on every existing table.
- pytest tests/ -q → 14 passed.

CLAUDE.md commands list updated to spell out the new fresh-DB and
existing-DB paths plus the "do not add to init_db" rule.

## Status of every loose end from the previous round

| 2026-05-03 followup item | This round | Commit |
|---|---|---|
| `nmtc_coalition_projects.state` truncated codes | ✓ | 1e1a66c |
| Drop pandas non-SQLAlchemy connection warning | ✓ | 19b47dd |
| Backfill `init_db` into Alembic migrations | ✓ (wrapper, not translation — see rationale above) | 29a88b7 |
| `data-status` endpoint (counts + last-refresh per domain) | not done — never on the audit, just a nice-to-have |  |

## Suggested next session

Nothing pressing. The dashboard request path is healthy, the ETL
throughput is fine, the test + migration chassis are in place, and
the search infra works end-to-end on Postgres. New surfaces from
this work:

| Item | Why | Estimated session length |
|---|---|---|
| `data-status` endpoint (counts + last-refresh per domain) | Surfaces "loader hasn't run for state X" issues earlier than user reports | 1-2 hours |
| Move `db.init_db`'s body into per-table `op.create_table()` revisions | The wrapper migration is opaque; per-table revisions would be readable. Only worth doing the next time we touch a table's schema. | 3-5 hours, or amortized across future schema changes |
| `nmtc_coalition_projects` matching coverage review | The matcher links Coalition rows to `nmtc_projects` by `cdfi_project_id` / `cde_name + state + investment_year`. Worth a coverage check now that state values are known-clean. | 1 hour |
| Lift FastAPI's `on_event("startup")` to a lifespan handler | Last remaining test-run deprecation warning. Cosmetic. | 15 min |

Pick whichever feels useful — none are blocking.
