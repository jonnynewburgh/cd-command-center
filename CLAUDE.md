# CD Command Center

## What This Project Is

A Streamlit-based dashboard for community development finance deal origination. It consolidates data on charter schools, health centers (FQHCs), early care and education (ECE) centers, NMTC projects, census demographics, and 990/philanthropy data into a single geography-driven tool.

The core use case: a deal originator can look up a specific location (address, census tract, county) and see every relevant facility, demographic indicator, and financing opportunity nearby — or filter across geographies to find areas that meet specific investment criteria.

This is NOT a reporting tool. It's a working tool for finding, evaluating, and comparing community facility investment opportunities.

## Audience

- Primary: the developer (Jonny), for daily deal origination work
- Eventually: colleagues and external users (this affects auth, permissions, and deployment decisions later)

## Tech Stack

- **Frontend:** Next.js + shadcn/ui + Recharts + MapLibre GL JS (separate repo: cd-command-center-dashboard)
- **Backend API:** FastAPI (`api/` directory in this repo) — wraps `db.py`, runs on port 8000
- **Database:** SQLite for development; designed for migration to PostgreSQL for production multi-user deployment
- **Data access:** All database queries go through a shared `db.py` module so the SQLite→PostgreSQL migration is a single-file change
- **Modeling:** SCSC CPF accountability scores for GA charters
- **Deployment (future):** Render or similar

## Project Structure

```
cd-command-center/
├── CLAUDE.md              # This file
├── README.md
├── requirements.txt
├── db.py                  # All database access functions (single point of change for DB migration)
├── validate.py            # Data validation / QA script
├── api/                   # FastAPI backend
│   ├── main.py            # App entry point — run with: uvicorn api.main:app --reload --port 8000
│   ├── deps.py            # Shared helpers (df_to_records, clean_dict)
│   └── routers/
│       ├── schools.py     # GET /schools, /schools/{id}, /schools/summary, etc.
│       ├── nmtc.py        # GET /nmtc/projects, /nmtc/cdes, /nmtc/projects/{id}/peer-comps
│       ├── fqhc.py        # GET /fqhc, /fqhc/{id}, /fqhc/summary
│       ├── ece.py         # GET /ece, /ece/{id}, /ece/summary
│       ├── tracts.py      # GET /tracts, /tracts/{id}, /tracts/service-gaps
│       ├── search.py      # GET /search, /search/nearby, /search/org
│       ├── rates.py       # GET /rates/latest, /rates (history), /rates/series
│       ├── orgs.py        # GET /orgs/{ein}/990, /history, /schools, /fqhc, /ratios
│       ├── notes.py       # GET/POST/PUT/DELETE /notes/{type}/{id}, /bookmarks
│       └── cdfis.py       # GET /cdfis, /cdfis/awards, /cdfis/state-programs
├── data/
│   ├── cd_command_center.sqlite  # Main database
│   └── raw/                      # Raw source files (CSV, etc.) — NOT committed to Git
├── utils/
│   ├── geo.py             # Geography helpers (census tract lookups, distance calculations)
│   ├── maps.py            # Map rendering functions
│   ├── export.py          # CSV/report export functions
│   └── pdf_extractor.py   # PDF text extraction + financial line item regex parser
├── etl/
│   └── (data ingestion scripts per source)
└── archive/
    └── app.py             # Original Streamlit frontend (archived)
```

## Database Schema (SQLite → PostgreSQL)

The database (`cd_command_center.sqlite`) consolidates all data sources. Key tables:

- `schools` — all public schools (charter + traditional) with location, enrollment, demographics, is_charter flag
- `nmtc_projects` — NMTC awards and project-level data from CDFI Fund
- `cde_allocations` — CDE-level allocation data
- `fqhc` — HRSA health center data (UDS, site-level)
- `ece_centers` — Early care and education facility data
- `census_tracts` — ACS demographic data, NMTC eligibility indicators, OZ flag, EJScreen indicators, 5-year change columns, gap analysis population fields
- `irs_990` — 990 data (most recent year) for nonprofit facility operators; includes balance sheet fields for ratio calculation
- `irs_990_history` — multi-year 990 filings for trend charts (one row per EIN + tax_year)
- `lea_accountability` — LEA/school-level accountability scores from state DOEs
- `cdfi_directory` — certified CDFIs from the CDFI Fund
- `state_programs` — state-level financing incentive programs (historic tax credits, state NMTCs, etc.)
- `enrollment_history` — NCES historical enrollment per school per year (for trend charts)
- `cdfi_awards` — CDFI Fund Financial Assistance, BEA, CMF, and other awards by awardee and year
- `user_notes` — per-entity freetext notes (school, fqhc, ece, nmtc, tract, org_990)
- `user_bookmarks` — saved entities for quick sidebar access
- `documents` — uploaded PDF documents (audits, financials) with extracted financial data (JSON) and verified flag
- `financial_ratios` — computed ratios per EIN per year: acid ratio (990~, audit✓), leverage ratio, 3yr avg operating cash flow
- `hud_ami` — HUD Area Median Income limits by county/metro, by family size % (30/50/80/120%)
- `hud_fmr` — HUD Fair Market Rents by county/metro, 0–4 bedroom units
- `hmda_activity` — HMDA mortgage lending aggregated by census tract (denial rate, originations)
- `sba_loans` — SBA 7(a) and 504 approved loans by borrower geography
- `bls_unemployment` — BLS monthly unemployment rate by county/MSA
- `bls_qcew` — BLS quarterly employment, wages, and establishments by county/industry
- `cra_institutions` — FFIEC CRA institution registry (banks with CRA obligations)
- `cra_assessment_areas` — bank CRA service territories (county/MSA coverage by institution)
- `cra_sb_discl` — FFIEC CRA D2-1 per-bank small business disclosure (2004-2024; ~1M rows)
- `cra_sb_aggr` — FFIEC CRA A2-1 all-bank aggregate small business lending by tract (2004-2024; ~560K rows)
- `scsc_cpf` — SCSC Comprehensive Performance Framework scores for GA charter schools
- `nmtc_coalition_projects` — NMTC Coalition transaction-level project database, matched to nmtc_projects
- `federal_audits` — GSA FAC Single Audit submissions (64K+ audits, 2023-2024); EIN-keyed, joins to all entity tables
- `federal_audit_programs` — per-ALN line items from Single Audits (1.2M+ rows); federal program detail, findings, amounts
- `headstart_programs` — Head Start PIR program-level data (46K+ records, 2008-2025); enrollment, staffing, health, demographics

Every facility table has `latitude`, `longitude`, and `census_tract_id` columns for geographic joins.

## Build Phases

Build in this order. Each phase should produce a working, usable version of the dashboard.

### Phase 1: Schools + LEA data ✅
- Import all public school data (charter + traditional) from NCES via Urban Institute API
- Map view: schools by location, colored by type (charter vs. traditional)
- Filter by state, district, enrollment, demographics, school type
- Census tract assignment via batch geocoding

### Phase 2: NMTC tracker + census data ✅
- NMTC project and CDE allocation data from CDFI Fund Excel
- Census tract demographics and 4-tier NMTC eligibility (LIC / Severely Distressed / Deep Distress)
- Geographic search by address with radius filtering

### Phase 2.5: Unified GIS layout ✅
- Single-page dashboard with layer toggles (Schools, NMTC Projects, CDEs)
- Unified map with Folium FeatureGroups and LayerControl
- Global search across schools, projects, CDEs
- Side-by-side school comparison
- Data caching for performance
- Filterable/sortable data tables with CSV export

### Phase 3: FQHC/health center data ✅
- HRSA UDS data integration
- Health center markers as a new map layer (toggle on/off)
- Add to search and comparison

### Phase 4: ECE facility data ✅
- State licensing data for early care and education centers
- ECE markers as a new map layer

### Phase 5: 990/philanthropy data ✅
- IRS 990 data for nonprofit facility operators and funders
- Financial health indicators for nonprofit operators

### Phase 5.5: Deal analysis tools ✅
- **Opportunity Zone overlay:** OZ flag on census_tracts, sidebar filter, badge in context panel
- **EJScreen indicators:** EPA environmental justice scores on census_tracts, shown in detail views
- **5-year tract change:** Historical ACS data loads poverty/income deltas on census_tracts
- **Service gap analysis:** Find high-poverty tracts with zero facilities (ECE/FQHC/schools)
- **NMTC peer comps:** Comparable deals by project type, state, and QLICI size
- **Multi-site operator profiles:** All sites + 990 trend chart for orgs with EIN linked
- **990 multi-year history:** `irs_990_history` table + `--years N` flag on fetch_990_data.py
- **NMTC pro forma calculator:** Interactive deal structure calculator in Tools tab
- **CDFI directory:** Certified CDFIs from CDFI Fund, filterable by state and type
- **State incentive programs:** Historic tax credits, state NMTCs, and other programs by state

### Phase 5.6: Operator intelligence + financial analysis ✅
- **Org-first search:** New "Org Lookup" tab — search by name/EIN, see all sites, 990 trend, ratios, news
- **Financial ratios:** Acid ratio (990-approximate ~, audit-precise ✓), leverage ratio, 3-year avg operating cash flow — per EIN per fiscal year in `financial_ratios` table
- **Audit preference:** Upload audit PDFs; pdfplumber extracts cash, current liabilities, net assets, operating CF; manual override fields; saves audit-quality ratios
- **Document upload:** `data/uploads/{ein}/` storage; extracted JSON stored in `documents` table; confirmed values update `financial_ratios`
- **Enrollment trends:** NCES historical enrollment per school via Education Data API; sparkline in school detail; `enrollment_history` table
- **News feed:** Google News RSS by org name in every detail panel (school, FQHC, org lookup)
- **Notes & bookmarks:** Per-entity freetext notes and starred bookmarks persisted in SQLite; bookmarks in sidebar for quick access
- **CDFI market activity:** New Tools tab showing CDFI Fund FA/BEA/CMF award data by state/program/year; `cdfi_awards` table + `etl/fetch_cdfi_awards.py`

### Phase 6: Polish for external users
- Authentication and user permissions
- Migrate to PostgreSQL
- Performance optimization for concurrent users

## Key Features (All Phases)

These features apply across all data sources once built:

1. **Geography-first search:** Look up an address, census tract, or county → see everything nearby
2. **Map views:** All facilities plotted on maps with meaningful color coding
3. **Filtering:** By geography, facility type, demographics, scores/metrics
4. **Comparison:** Side-by-side comparison of two facilities or two geographies, including demographic similarity and feature overlap
5. **Data export:** CSV export of any filtered view
6. **Charts:** Distributions, trends, and summary statistics for filtered results

## Gotchas and Rules

- **All database access goes through `db.py`.** Never write raw SQL in page files. This is the single most important architectural rule — it makes the PostgreSQL migration possible.
- **Don't build features for future phases.** Each phase should work standalone.
- **Census tract is the geographic join key.** Every facility must resolve to a census tract.
- **The developer codes in Python at a non-expert level.** Keep code straightforward. Prefer clarity over cleverness. Use comments to explain non-obvious logic.
- **No frontend frameworks.** This is Streamlit only. Don't introduce React, Vue, or custom JS.
- **Raw data files go in `data/raw/` and are gitignored.** Only the SQLite database is committed (or, if too large, the ETL scripts that build it).
- **When suggesting changes, explain WHY.** The developer is learning and wants to understand the reasoning.

## Commands

```bash
# Run the FastAPI backend (from repo root)
uvicorn api.main:app --reload --port 8000

# Interactive API docs (once server is running)
# http://localhost:8000/docs   — Swagger UI
# http://localhost:8000/redoc  — ReDoc

# Run the app locally (archived — Streamlit frontend removed)
# streamlit run app.py

# Fetch school data (all public schools, all states)
python etl/fetch_nces_schools.py
python etl/fetch_nces_schools.py --states CA TX NY    # specific states
python etl/fetch_nces_schools.py --charter-only       # charters only
python etl/fetch_nces_schools.py --demographics       # include race data

# Assign census tracts to schools (batch geocoding)
python etl/assign_census_tracts.py
python etl/assign_census_tracts.py --states CA --limit 500

# Load census tract demographics
python etl/load_census_tracts.py --states CA TX NY
python etl/load_census_tracts.py --all
python etl/load_census_tracts.py --states CA --historical   # also load 5yr-ago data for trend columns

# Load NMTC project + CDE data from CDFI Fund Excel
python etl/load_nmtc_data.py --file data/raw/nmtc_public_data_2024.xlsx
python etl/load_nmtc_data.py --file data/raw/nmtc_public_data_2024.xlsx --sheet-names
python etl/load_nmtc_data.py --file data/raw/nmtc_public_data_2024.xlsx --project-sheet "QLICI" --cde-sheet "CDE"

# Load FQHC / health center data from HRSA (auto-downloads latest file)
python etl/fetch_fqhc.py
python etl/fetch_fqhc.py --states CA TX NY    # specific states only
python etl/fetch_fqhc.py --all-sites          # include inactive sites
python etl/fetch_fqhc.py --file data/raw/hrsa_health_centers.csv  # use local file

# Load ECE / child care data from a state licensing CSV or Excel file
# (Download source varies by state — see docstring at top of the script)
python etl/load_ece_data.py --file data/raw/ca_licensed_facilities.csv --state CA
python etl/load_ece_data.py --file data/raw/tx_childcare.xlsx --state TX --source "TX HHSC"
python etl/load_ece_data.py --file data/raw/ny_childcare.csv --state NY --all-facilities
python etl/load_ece_data.py --file data/raw/ca_licensed_facilities.csv --columns-only  # inspect columns

# Load IRS 990 data (Phase 5)
python etl/fetch_990_data.py --schools --states CA TX    # charter schools only
python etl/fetch_990_data.py --fqhc --states CA          # health centers only
python etl/fetch_990_data.py --years 3                    # load 3 years of history per org

# Load Opportunity Zone designations (Phase 5.5)
# Download from: https://www.irs.gov/pub/irs-utl/Designated_QOZ_8996.xlsx
python etl/load_opportunity_zones.py --file data/raw/opportunity_zones.csv
python etl/load_opportunity_zones.py --file data/raw/opportunity_zones.csv --columns-only

# Load EPA EJScreen environmental justice indicators (Phase 5.5)
# Download national CSV from: https://gaftp.epa.gov/EJSCREEN/2023/
python etl/load_ejscreen.py --file data/raw/EJSCREEN_2023_Tracts.csv --states CA TX
python etl/load_ejscreen.py --file data/raw/EJSCREEN_2023_Tracts.csv --columns-only

# Load CDFI directory from CDFI Fund (Phase 5.5)
# Auto-download from data.gov:
python etl/load_cdfi_directory.py --auto
python etl/load_cdfi_directory.py --auto --states CA TX
# Or use manually downloaded file from: https://www.cdfifund.gov/research-and-resources/data-resources
python etl/load_cdfi_directory.py --file data/raw/cdfi_certified_list.xlsx
python etl/load_cdfi_directory.py --file data/raw/cdfi_certified_list.xlsx --columns-only

# Load state incentive programs from seed file (Phase 5.5)
python etl/load_state_programs.py                          # uses data/raw/state_programs_seed.csv
python etl/load_state_programs.py --file data/raw/my_programs.csv  # custom file

# Load historical enrollment data from NCES (Phase 5.6)
python etl/fetch_enrollment_trends.py                      # all schools in DB, 5 years
python etl/fetch_enrollment_trends.py --states CA TX       # specific states
python etl/fetch_enrollment_trends.py --years 8            # up to 8 years
python etl/fetch_enrollment_trends.py --charter-only       # charter schools only

# Load CDFI Fund award data (Phase 5.6)
# Auto-download from data.gov:
python etl/fetch_cdfi_awards.py --auto
python etl/fetch_cdfi_awards.py --auto --states CA TX
# Or use manually downloaded file from: https://www.cdfifund.gov/research-and-resources/data-resources
python etl/fetch_cdfi_awards.py --file data/raw/cdfi_awards.xlsx
python etl/fetch_cdfi_awards.py --file data/raw/cdfi_awards.xlsx --columns-only

# Fetch FRED market rates (SOFR, 5/10/30yr Treasuries, Fed Funds)
# Get a free API key at: https://fred.stlouisfed.org/docs/api/api_key.html
python etl/fetch_fred_rates.py --api-key YOUR_KEY
python etl/fetch_fred_rates.py --api-key YOUR_KEY --days 730   # 2 years of history
python etl/fetch_fred_rates.py --api-key YOUR_KEY --latest     # quick refresh (last 7 days)
python etl/fetch_fred_rates.py --api-key YOUR_KEY --series SOFR DGS10  # specific series only
# Or set FRED_API_KEY env var and omit --api-key

# Load HUD Area Median Income (AMI) limits
# Fetches from HUD public API (no account required) or a local Excel file
python etl/fetch_hud_ami.py                              # all states, current fiscal year
python etl/fetch_hud_ami.py --year 2024                  # specific year
python etl/fetch_hud_ami.py --states CA TX NY            # specific states only
python etl/fetch_hud_ami.py --file data/raw/Section8-FY25.xlsx          # local Excel
python etl/fetch_hud_ami.py --file data/raw/Section8-FY25.xlsx --columns-only

# Load HUD Fair Market Rents (FMRs)
# Download Excel from: https://www.huduser.gov/portal/datasets/fmr.html
python etl/fetch_hud_fmr.py                              # all states, current fiscal year
python etl/fetch_hud_fmr.py --year 2025
python etl/fetch_hud_fmr.py --states CA TX NY
python etl/fetch_hud_fmr.py --file data/raw/FY2025_4050_FMRs_Final.xlsx
python etl/fetch_hud_fmr.py --file data/raw/FY2025_4050_FMRs_Final.xlsx --columns-only

# Load FFIEC CRA institution and assessment area data
# Download flat files from: https://www.ffiec.gov/cradownload.htm
# Extract zip; look for Transmittal.dat and Agg_Assessment_Area.dat
python etl/fetch_cra_data.py --year 2023 \
    --transmittal data/raw/CRA_Flat_2023_Transmittal.dat \
    --assessment-area data/raw/CRA_Flat_2023_Agg_Assessment_Area.dat
python etl/fetch_cra_data.py --year 2023 \
    --transmittal data/raw/CRA_Flat_2023_Transmittal.dat \
    --assessment-area data/raw/CRA_Flat_2023_Agg_Assessment_Area.dat \
    --states CA TX NY
python etl/fetch_cra_data.py --year 2023 \
    --transmittal data/raw/CRA_Flat_2023_Transmittal.dat --columns-only
# Auto-download CRA flat files from ffiec.gov:
python etl/fetch_cra_data.py --year 2023 --auto
python etl/fetch_cra_data.py --year 2023 --auto --states CA TX NY

# Load FFIEC CRA small business lending data (D2-1 disclosure + A2-1 aggregate, 2004-2024)
# Place downloaded flat files in data/raw/cra/ (zip-extracted; any folder depth works)
python etl/load_cra_lending.py                         # all years, all states
python etl/load_cra_lending.py --year 2023             # single year
python etl/load_cra_lending.py --year 2023 --states GA TX NY  # filtered

# Load SBA 7(a) and 504 loan data
# Auto-download from data.sba.gov via CKAN API (300+ MB files):
python etl/fetch_sba_loans.py --auto --program 7a
python etl/fetch_sba_loans.py --auto --program 504
python etl/fetch_sba_loans.py --auto --program 7a --fiscal-year 2024 --states GA TX
# Or use manually downloaded files:
python etl/fetch_sba_loans.py --file data/raw/foia-7afy2024.csv --program 7a
python etl/fetch_sba_loans.py --file data/raw/foia-504fy2024.csv --program 504

# Load HMDA mortgage lending activity by census tract (CFPB API, no key required)
python etl/fetch_hmda.py --year 2023 --states CA TX NY
python etl/fetch_hmda.py --year 2023 --all              # all states (~50 API calls, slow)

# Load BLS unemployment by state/MSA/county
# FRED API key (free): https://fred.stlouisfed.org/docs/api/api_key.html
# BLS API key (free, optional): https://data.bls.gov/registrationEngine/
python etl/fetch_bls_unemployment.py --mode fred-states --api-key YOUR_FRED_KEY
python etl/fetch_bls_unemployment.py --mode fred-states --api-key YOUR_FRED_KEY --states CA TX NY --months 36
python etl/fetch_bls_unemployment.py --mode fred-msa --api-key YOUR_FRED_KEY \
    --msa-series LAUMT064720000000003 LAUMT367400000000003
python etl/fetch_bls_unemployment.py --mode bls-county --fips 06037 17031 36061
python etl/fetch_bls_unemployment.py --mode bls-county --fips 06037 --bls-key YOUR_BLS_KEY

# Load BLS QCEW employment by county and industry (BLS API, no key required)
python etl/fetch_bls_qcew.py --fips 06037 17031 --year 2023 --quarter 4
python etl/fetch_bls_qcew.py --fips 06037 --year 2023 --annual
python etl/fetch_bls_qcew.py --fips 06037 --year 2023 --quarter 4 --totals-only
# Bulk load from downloaded CSV (https://www.bls.gov/cew/downloadable-data.htm):
python etl/fetch_bls_qcew.py --file data/raw/2023_annual.csv --year 2023 --annual --totals-only
python etl/fetch_bls_qcew.py --file data/raw/2023_annual.csv --year 2023 --states CA TX --columns-only

# Auto-download EDFacts federal LEA accountability data from Ed.gov (public domain)
python etl/fetch_edfacts_auto.py                           # most recent year (2023)
python etl/fetch_edfacts_auto.py --year 2022               # specific year
python etl/fetch_edfacts_auto.py --years 2021 2022 2023    # multiple years
python etl/fetch_edfacts_auto.py --year 2023 --states GA   # specific states only
python etl/fetch_edfacts_auto.py --year 2023 --download-only  # download files, don't load

# Load SCSC CPF accountability scores for GA charter schools
# Source: charters repo at ../charters/data/cpf_all_years.csv
python etl/load_scsc_cpf.py                                # uses default path from charters repo
python etl/load_scsc_cpf.py --file path/to/cpf_all_years.csv
python etl/load_scsc_cpf.py --dry-run                      # preview name matching
python etl/load_scsc_cpf.py --match-threshold 0.9          # stricter matching

# Load NMTC Coalition Transaction Level Report
# Download from: https://nmtccoalition.org/nmtc-fact-sheet/
python etl/load_nmtc_coalition.py --file data/raw/nmtc_transaction_report_2024.xlsx
python etl/load_nmtc_coalition.py --file data/raw/nmtc_transaction_report_2024.xlsx --columns-only
python etl/load_nmtc_coalition.py --file data/raw/nmtc_transaction_report_2024.xlsx --dry-run
python etl/load_nmtc_coalition.py --match-only              # re-run matching on loaded records

# Load Federal Audit Clearinghouse (FAC) Single Audit data
# Free API key at: https://api.data.gov/signup/ — set FAC_API_KEY env var
python etl/fetch_fac.py --state GA --year 2024             # single state test
python etl/fetch_fac.py --state GA --years 2023 2024       # multiple years
python etl/fetch_fac.py --all-states --year 2024           # all states (slow — rate limited)
python etl/fetch_fac.py --state GA --year 2024 --dry-run   # preview without writing

# Load Head Start PIR (Program Information Report) from HSES Excel export
# Requires HSES account (https://hses.ohs.acf.hhs.gov) — set HSES_USERNAME/HSES_PASSWORD env vars
python etl/load_headstart_pir.py --file data/raw/childcare/PIR_Export_2025.xlsx
python etl/load_headstart_pir.py --dir data/raw/childcare                     # batch all PIR files
python etl/load_headstart_pir.py --dir data/raw/childcare --states GA TX      # filter by state
python etl/load_headstart_pir.py --file data/raw/childcare/PIR_Export_2025.xlsx --columns-only
python etl/load_headstart_pir.py --file data/raw/childcare/PIR_Export_2025.xlsx --dry-run

# Run full ETL pipeline (auto-downloads everything it can)
python etl/run_pipeline.py                         # all auto-downloadable stages
python etl/run_pipeline.py --states GA TX          # state-filtered run
python etl/run_pipeline.py --year 2023             # specific data year
python etl/run_pipeline.py --skip sba-7a sba-504   # skip specific stages
python etl/run_pipeline.py --only schools fqhc 990 # run subset only
python etl/run_pipeline.py --dry-run               # preview without executing
python etl/run_pipeline.py --continue-on-error     # keep running after failures
# Set FRED_API_KEY env var to also run BLS unemployment via FRED

# Run tests (when they exist)
pytest tests/
```

## Related Projects

These are separate repos that feed data into or share code patterns with this project:

- **charters** — GA SCSC charter school CPF data (cpf_all_years.csv loaded by load_scsc_cpf.py)
- LEA accountability Shiny app (R/Shiny/SQLite — different stack, data may be imported)
- NMTC tracker (Python — may be absorbed into this project)
- FQHC data pipeline (Python — feeds into this project)
- ECE facility finder (Python — feeds into this project)
