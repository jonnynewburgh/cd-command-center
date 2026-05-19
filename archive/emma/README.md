# EMMA continuing-disclosure ETL — archived 2026-05-18

This directory holds the Phase 1 scaffolding for an EMMA (MSRB
emma.msrb.org) continuing-disclosure ETL that was **rejected and parked
indefinitely**.

## Why parked

MSRB EMMA's Terms of Service prohibit scraping:

> "data mining, crawling, 'scraping', robot or similar automated or
> data gathering or extraction method, or any manual process, to access,
> acquire, monitor or copy any portion of the Website, Content or
> Services, or otherwise systematically download or store Content."

Politeness, rate-limiting, and "research use" do not cure this — the
prohibition is categorical. Adjacent ideas (authorizer-site audit
scrapers, school-site audit scrapers) were considered separately and
also rejected. See user memory `project_emma_blackhole.md`.

## What's in this directory

- `fetch_emma_disclosures.py` — the loader skeleton. Has a hard
  `sys.exit(2)` at the top of `main()` and a do-not-run banner in its
  module docstring.
- `migrations/a1b2c3d4e5f6_emma_disclosures.py` — Alembic migration
  that would create `emma_issuers` and `emma_disclosures`. **Never
  applied to any database.** Lives here because its revision id
  collides with the (already-applied) `a1b2c3d4e5f6_index_schools_fqhc_ein.py`
  in `migrations/versions/` — keeping it in the active versions/ dir
  causes `alembic upgrade head` to fail with "Multiple head revisions".
- `emma_etl_brief.md` — original design brief.

## If a licensed feed ever appears

If we get a path to EMMA data through a licensed feed (MSRB CD
subscription, DPC DATA, Merritt Research, SOLVE, Munistatistics,
Bloomberg/Refinitiv, or direct trustee outreach), the schema and
accessors in this directory are a starting point. Before re-activating:

1. Pick a fresh, non-colliding alembic revision id for the migration
   (the `a1b2c3d4e5f6` slot is permanently used).
2. Re-add an `emma_issuers` clause to the EIN-collection block in
   `etl/fetch_990_irs.py` (removed in the archival commit).
3. Re-add the EMMA accessor block to `db.py` (removed in the archival
   commit; see git history).
4. Verify the licensed source's ToS permits this use before resuming.

Do **not** re-propose scraping emma.msrb.org. That door is closed.
