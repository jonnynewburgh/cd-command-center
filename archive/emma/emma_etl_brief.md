# EMMA (MSRB) Continuing-Disclosure Audit ETL — Build Brief

You are starting a new build in the `cd-command-center` repo. Read this brief end-to-end before writing code. Read `CLAUDE.md`, `AGENTS.md`, and `DATA_REFRESH_SCHEDULE.md` for repo conventions, then skim `etl/fetch_990_irs.py` and `etl/match_bmf_eins_orgs.py` — your patterns should mirror them.

## Goal

Add a recurring ETL that pulls **annual audited financial statement PDFs** from EMMA (Electronic Municipal Market Access, `emma.msrb.org`) for **501(c)(3) conduit borrowers** that file continuing disclosures under SEC Rule 15c2-12. This supplements the IRS 990 pipeline: many CD-relevant borrowers (charter schools, FQHCs, hospitals, affordable-housing nonprofits, CDFIs, higher-ed nonprofits) file audited statements via EMMA that are richer and more current than their 990s.

## Scope (Phase 1)

- **Universe:** all nonprofit conduit borrowers on EMMA, not pre-filtered by our existing entity tables. We want a broad corpus so that EIN matches surface borrowers we don't already track.
- **Documents:** category = **Annual Financial Information / Audited Financial Statements** (EMMA continuing-disclosure category, RFAA filings). Skip operating data, event notices, official statements, and rating changes in Phase 1.
- **Years:** all available history EMMA exposes for each issuer (typically back to ~2009 when the EMMA system started accepting CD filings). Future runs are incremental.
- **PDFs:** download and store locally. **Do not build a financial-statement extractor in this phase** — that is a separate Phase 2 project. Phase 1 ends when PDFs are on disk and indexed in Postgres with EIN matches attempted.

## Access

Use the **public EMMA web** (`emma.msrb.org`). No auth, no API key. Be a polite scraper:

- Single concurrent request, sleep ≥ 1s between requests, exponential backoff on 429/5xx.
- Set a descriptive User-Agent identifying the ETL and an email.
- Cache the issuer/CUSIP list and document index on disk so re-runs don't re-hit EMMA for unchanged metadata.
- Resume cleanly: every step idempotent, every PDF download skippable if file already exists with non-zero size.

There is no documented EMMA API. The working endpoints are XHR routes used by the EMMA UI. Discover them via browser devtools on `emma.msrb.org`; do not hardcode UI HTML scraping if a JSON endpoint exists. Likely starting points (verify before using):

- Issuer/obligor search by state and sector
- Continuing-disclosure document list per issuer/CUSIP6
- Document download by EMMA document ID

If the public site changes shape or rate-limits aggressively, stop and surface that to the user before working around it.

## Repo conventions to follow

- All ETL scripts live in `etl/`. New script: `etl/fetch_emma_disclosures.py`.
- Raw inputs/downloads live in `data/raw/`. New subdir: `data/raw/emma/` with subdirs per issuer CUSIP6 to keep listings manageable.
- Schema changes go through **Alembic** (`migrations/`). Create a new revision; mirror styles of the recent revisions (e.g. `a8c3f5d9e2b1`, `9e3b590fa748`).
- `db.py` is the single source of truth for schema CREATE statements and read accessors. Add CREATE TABLE for `emma_disclosures` AND add accessors. **Use `_q()` / `adapt_sql()` for parameter placeholders** — this DB is Postgres in prod (see memory `project_cd_command_center.md`); raw `?` placeholders will silently break.
- Env vars come from `~/.bashrc`. New ones go there. Use `DATABASE_URL` for Postgres.
- Logging: structured, one line per issuer/document with status. Write a run summary at end (issuers processed, docs found, docs downloaded, EIN matches, errors).
- No silent failures. If a step is skipped because data is missing, log it as WARN with the reason.

## Proposed schema

Two tables. Both keyed independently — issuers can have many filings, filings can list multiple obligors/CUSIPs.

### `emma_issuers`

One row per **obligated person** (the borrower behind the bonds, not the conduit issuer of record).

| col | type | notes |
|---|---|---|
| obligor_id | text PK | EMMA's obligor identifier if exposed, else hash of normalized name+state |
| obligor_name | text | as listed on EMMA |
| obligor_name_normalized | text | uppercased, punctuation stripped — match key |
| state | text | 2-letter |
| sector | text | EMMA sector tag (e.g. "Education", "Health Care") if available |
| cusip6_list | text[] | all CUSIP6s associated with this obligor |
| ein | text | populated by BMF matcher; nullable |
| ein_match_confidence | float | from matcher |
| ein_match_method | text | e.g. "bmf_name_state" |
| first_seen | date | from EMMA |
| last_seen | date | most recent filing date observed |
| fetched_at | timestamptz | |

### `emma_disclosures`

One row per continuing-disclosure document.

| col | type | notes |
|---|---|---|
| emma_doc_id | text PK | EMMA's document ID |
| obligor_id | text FK → emma_issuers | |
| filing_date | date | as posted on EMMA |
| period_end_date | date | fiscal-period end the document covers, when available |
| document_category | text | "Annual Financial Information", "Audited Financial Statements", etc. |
| document_subcategory | text | finer EMMA tag |
| document_title | text | as posted |
| cusips | text[] | CUSIPs the filing is associated with |
| source_url | text | canonical EMMA URL |
| pdf_path | text | relative path under data/raw/emma/ |
| pdf_sha256 | text | computed at download |
| pdf_size_bytes | bigint | |
| download_status | text | 'ok' / 'pending' / 'failed' / 'skipped_non_pdf' |
| fetched_at | timestamptz | |
| UNIQUE(emma_doc_id) | | |

## Build steps

1. **Repo recon.** Read `CLAUDE.md`, `AGENTS.md`, `DATA_REFRESH_SCHEDULE.md`. Read `etl/fetch_990_irs.py` to internalize style. Read `etl/match_bmf_eins_orgs.py` — you'll reuse its BMF primitives.
2. **Endpoint discovery.** Spend the first hour confirming what EMMA actually exposes today. Manually walk the EMMA UI in a browser with devtools open. Identify the JSON endpoints for (a) browse/search obligors, (b) list disclosures per obligor, (c) get document metadata, (d) download PDF. Document them in the script's module docstring. Do not proceed to step 3 until this is solid.
3. **Issuer enumeration.** Build the obligor list. Strategy: iterate by state × sector to keep result pages manageable. Filter to nonprofit obligors (the EMMA sector taxonomy distinguishes governmental vs. 501c3 conduit borrowers). Persist intermediate results to `data/raw/emma/_index/` as JSON.
4. **Alembic migration.** Create both tables.
5. **Disclosure enumeration.** For each obligor, list continuing-disclosure docs filtered to annual-financial categories. Upsert `emma_disclosures` rows with `download_status='pending'`.
6. **PDF download.** Walk pending rows, download to `data/raw/emma/{cusip6}/{emma_doc_id}.pdf`, compute SHA256, update row. Skip if file exists and SHA matches. Polite rate-limiting throughout.
7. **EIN matching.** After issuer enumeration, run BMF matching using the primitives in `match_bmf_eins_orgs.py`: name + state fuzzy match against the IRS Exempt-Org BMF (~1.94M orgs). Match key is `obligor_name_normalized` + `state`. Store `ein`, `ein_match_confidence`, `ein_match_method` on `emma_issuers`. Expect low match rate (≈20–40%) — many conduit borrowers are project-specific LLCs, hospital systems with parent EIN ≠ borrower name, or governmental entities not in the 501c3 BMF.
8. **Trigger 990 re-fetch.** After EIN matching completes, the user can re-run `etl/fetch_990_irs.py`. **Update that script's EIN-collection query to also pull from `emma_issuers.ein WHERE ein IS NOT NULL`** (currently it pulls from `irs_990 + schools + fqhc + v_cde_allocations + federal_audits.auditee_ein`). Add this — it's the whole point of the EIN-matching step.
9. **db.py accessors.** Add `get_emma_disclosures_for_ein(ein)`, `get_emma_disclosures_for_obligor(obligor_id)`, `get_emma_issuer_by_ein(ein)`. Use `_q()`.
10. **API routes (optional in Phase 1).** If time permits, add `/emma/issuer/{ein}` and `/emma/disclosures/{ein}` to the FastAPI layer. Otherwise leave for later.
11. **CLI flags.** `--states`, `--sectors`, `--obligor` (single-obligor refresh), `--since YYYY-MM-DD` (incremental), `--skip-downloads` (metadata only), `--dry-run`. Mirror existing scripts.
12. **Logging + run summary.** End-of-run report to stdout AND `logs/emma_<timestamp>.log`.
13. **Add to `DATA_REFRESH_SCHEDULE.md`** with cadence: **quarterly** is reasonable (annual audits trickle in over many months after FYE; quarterly captures the bulk without daily churn).

## Things to verify with the user before building

- That an MSRB ToS prohibition on scraping does not exist or has been accepted. Phase 1 scrapes responsibly; if MSRB's terms prohibit any automated access, stop and surface this.
- Disk budget for `data/raw/emma/`. A rough envelope: ~50K nonprofit obligors × ~10 historical filings × ~2 MB/PDF = ~1 TB worst case. Probably much less in practice (many obligors don't file, many filings are <1 MB), but confirm available disk before launching the full backfill. **Default to a `--states` subset for the first run** (start with one or two states the user names) before going national.

## Out of scope (Phase 2+ — do not build now)

- Audit PDF financial-statement extractor.
- Event-notice ingestion (rating changes, defaults, missed payments) — useful for risk monitoring but not Phase 1.
- Continuing-disclosure operating-data tables (enrollment, occupancy, days-cash-on-hand) — borrower-class-specific schemas needed.
- Cross-link from `emma_issuers` to `schools` / `fqhc` / etc. beyond the EIN join.

## Definition of done (Phase 1)

- `etl/fetch_emma_disclosures.py` runs end-to-end on a `--states CA` subset without errors.
- `emma_issuers` and `emma_disclosures` tables exist via Alembic, populated.
- ≥80% of pending PDFs successfully downloaded with SHA256 recorded.
- BMF EIN match attempted on all issuers; match rate logged.
- `fetch_990_irs.py` updated to include `emma_issuers.ein` in its EIN-collection query.
- `DATA_REFRESH_SCHEDULE.md` updated.
- A one-paragraph note added to `project_cd_command_center` memory describing what was built.
