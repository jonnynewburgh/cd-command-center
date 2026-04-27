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

### irs_990_history empty (loader pending)

ProPublica was previously the multi-year 990 history source. After
the licensing review on 2026-04-26 (ProPublica's ToS prohibits
commercial use), all ProPublica data was deleted from `irs_990`,
`irs_990_history`, and `financial_ratios`. `irs_990_history` is
currently empty.

**Replacement plan:** Extend `etl/fetch_990_irs.py` (already
loading public-domain IRS XML for the single-year `irs_990` table)
to also write per-year rows into `irs_990_history`. The IRS XML
source has every year — adding a `--years` loop and writing to the
history table is ~1-2 hours of work.

**User-visible features blocked until this ships:** 990 trend
charts (org-detail page), financial ratios (acid, leverage,
operating cash flow). Single-latest-year 990 lookups still work.

### irs_990 33 BMF-sourced null tax_year (was 506)

Down from 506 to 33 after the ProPublica purge. IRS BMF has a
`TAX_PERIOD` field (YYYYMM) that the loader at
`etl/fetch_bmf_eins.py` doesn't currently parse into `tax_year`.
~30 min fix at the loader. Not blocking; defer until the broader
multi-year IRS history loader is built (same workstream).

## Closed in this cleanup pass (2026-04-26)

### ProPublica data source removed (licensing) — closed

Deleted all ProPublica-sourced rows from the database (646 from
`irs_990`, 3,551 from `irs_990_history`, 3,513 from
`financial_ratios`). Removed `etl/fetch_990_data.py` loader.
Updated docs (CLAUDE.md, AGENTS.md, DATA_REFRESH_SCHEDULE.md, CI
workflow) to point to `etl/fetch_990_irs.py` (public-domain IRS
XML). Schema column defaults updated from
`DEFAULT 'ProPublica'` → `DEFAULT 'IRS'`.

Reason: ProPublica's Nonprofit Explorer ToS prohibits commercial
use, and CD Command Center is a commercial deal-origination tool.


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
