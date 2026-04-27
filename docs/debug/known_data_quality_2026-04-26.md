---
title: Known data quality issues
date: 2026-04-26
scope: Issues surfaced by validate.py that are intentionally not fixed in code
status: Living document — update as items are closed
---

# Known data quality issues — 2026-04-26

## Why this doc exists

`validate.py` reports a small number of data WARNs that aren't
Postgres-compat bugs and aren't trivial 30-min cleanups. Captured
here so the next reader knows what's been triaged vs. what's new.

## Currently open

### irs_990 12% null tax_year (506 / 4,126 rows)

**Source breakdown** (verified 2026-04-26):
- 473 rows from ProPublica loader (`etl/fetch_990_data.py`)
- 33 rows from IRS BMF loader (`etl/fetch_bmf_eins.py`)
- 0 rows from primary IRS source (`etl/fetch_990_irs.py`)

**Why it's not a 30-min fix:** the ProPublica and IRS BMF loaders
need their tax_year extraction logic reviewed. ProPublica's CSV
columns vary by year and some 990 records lack a tax_year field
entirely. IRS BMF has a `TAX_PERIOD` field that requires parsing
(YYYYMM → year). Either the loaders should backfill tax_year
during ingest from `TAX_PERIOD` or other date fields, or the
records without tax_year should be skipped. Both options need
sample-data review and a separate workstream.

**Caller impact:** `irs_990_history` joins by EIN + tax_year. Rows
with null tax_year don't appear in trend charts but DO appear in
"latest 990" lookups via `MAX(tax_year)`. Acceptable for now —
trend charts simply have a slightly thinner trail for
ProPublica-sourced orgs.

**Recommended next step:** ~1-2 hour session to update both
loaders' tax_year extraction. Defer until prioritized.

## Closed in this cleanup pass (2026-04-26)

### lea_accountability 200 orphan lea_ids — closed

200 rows in `lea_accountability` had `lea_id` values matching the
synthetic pattern `LEA{state}{number}` (e.g., `LEAPA2275`). All
200 were leftover sample data from `etl/load_sample_data.py`. Real
lea_ids are 7-digit numeric codes (e.g., `0100005` Albertville
City).

**Action:** `DELETE FROM lea_accountability WHERE lea_id LIKE
'LEA%'` on both backends. Verified: 200 → 0 on each.

### irs_990 6 negative total_revenue — closed

6 rows had `total_revenue < 0`. Total revenue is a gross figure on
Form 990 Line 12 — it cannot legitimately be negative regardless
of the org's net result. Likely causes: parser sign-flip on
specific 990 form layouts; net-revenue accidentally written to
total_revenue column; or upstream data error.

**Action:** `UPDATE irs_990 SET total_revenue = NULL WHERE
total_revenue < 0` on both backends. Affected EINs documented in
git history (commit body of this cleanup commit) for future
investigation if the loader bug recurs.

**Recommended follow-up:** add a load-time validation in the IRS
and ProPublica 990 loaders that NULLs (and logs) any `total_revenue
< 0` value. Same shape as a `> 0 sanity check`. Not done in this
cleanup pass — it's loader-code work, separate workstream.

## Other deferred items

None at the moment.
