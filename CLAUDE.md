# CD Command Center

## What This Project Is

A Streamlit-based dashboard for community development finance deal origination. It consolidates data on charter schools, health centers (FQHCs), early care and education (ECE) centers, NMTC projects, census demographics, and 990/philanthropy data into a single geography-driven tool.

The core use case: a deal originator can look up a specific location (address, census tract, county) and see every relevant facility, demographic indicator, and financing opportunity nearby — or filter across geographies to find areas that meet specific investment criteria.

This is NOT a reporting tool. It's a working tool for finding, evaluating, and comparing community facility investment opportunities.

## Audience

- Primary: the developer (Jonny), for daily deal origination work
- Eventually: colleagues and external users (this affects auth, permissions, and deployment decisions later)

## Tech Stack

- **Frontend:** Streamlit (Python), multi-page app
- **Database:** SQLite for development; designed for migration to PostgreSQL for production multi-user deployment
- **Data access:** All database queries go through a shared `db.py` module so the SQLite→PostgreSQL migration is a single-file change
- **Mapping:** Folium or Streamlit-native maps for geographic views
- **Modeling:** scikit-learn (charter school survival model lives here or is imported)
- **Deployment (future):** Render or similar

## Project Structure

```
cd-command-center/
├── CLAUDE.md              # This file
├── README.md
├── requirements.txt
├── app.py                 # Main Streamlit entry point
├── pages/                     # (empty — unified single-page layout in app.py)
├── data/
│   ├── cd_command_center.sqlite  # Main database
│   └── raw/                      # Raw source files (CSV, etc.) — NOT committed to Git
├── db.py                  # All database access functions (single point of change for DB migration)
├── models/
│   └── charter_survival.py  # Charter school survival prediction model
├── utils/
│   ├── geo.py             # Geography helpers (census tract lookups, distance calculations)
│   ├── maps.py            # Map rendering functions
│   ├── export.py          # CSV/report export functions
│   └── pdf_extractor.py   # PDF text extraction + financial line item regex parser
├── etl/
│   └── (data ingestion scripts per source)
└── .streamlit/
    └── config.toml
```

## Database Schema (SQLite → PostgreSQL)

The database (`cd_command_center.sqlite`) consolidates all data sources. Key tables:

- `schools` — all public schools (charter + traditional) with location, enrollment, demographics, is_charter flag, survival model predictions (charters only)
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

Every facility table has `latitude`, `longitude`, and `census_tract_id` columns for geographic joins.

## Build Phases

Build in this order. Each phase should produce a working, usable version of the dashboard.

### Phase 1: Schools + LEA data ✅
- Import all public school data (charter + traditional) from NCES via Urban Institute API
- Charter school survival model integration (heuristic scoring)
- Map view: schools by location, colored by risk/survival score (charters) or blue (traditional)
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
- Train real survival model from historical closure data

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
# Run the app locally
streamlit run app.py

# ─────────────────────────────────────────────────────────────────────
# ZERO-TOUCH FULL ETL: Run everything with no manual downloads required
# ─────────────────────────────────────────────────────────────────────

# Full national build (all states, all sources — may take hours + requires ~10 GB disk for EJScreen):
python etl/run_all.py

# Specific states only (much faster for development):
python etl/run_all.py --states CA TX NY IL

# Skip the large EJScreen download (~5 GB):
python etl/run_all.py --states CA TX --skip ejscreen

# See what would run without executing:
python etl/run_all.py --dry-run

# Re-run only failed steps:
python etl/run_all.py --only schools census

# Re-download all cached files:
python etl/run_all.py --force-download

# List all available step names:
python etl/run_all.py --list-steps

# ─────────────────────────────────────────────────────────────────────
# Individual script commands (if you need to run one step at a time):
# ─────────────────────────────────────────────────────────────────────

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

# Load NMTC project + CDE data from CDFI Fund (now auto-downloads)
python etl/load_nmtc_data.py                               # auto-download from CDFI Fund
python etl/load_nmtc_data.py --file data/raw/nmtc_public_data.xlsx  # use local file
python etl/load_nmtc_data.py --file data/raw/nmtc_public_data.xlsx --sheet-names

# Load FQHC / health center data from HRSA (auto-downloads latest file)
python etl/fetch_fqhc.py
python etl/fetch_fqhc.py --states CA TX NY    # specific states only
python etl/fetch_fqhc.py --all-sites          # include inactive sites
python etl/fetch_fqhc.py --file data/raw/hrsa_health_centers.csv  # use local file

# Load ECE / child care data (now auto-downloads from state open data portals)
python etl/load_ece_data.py --all-states                        # auto-download all supported states
python etl/load_ece_data.py --state CA                          # auto-download California only
python etl/load_ece_data.py --state TX                          # auto-download Texas only
python etl/load_ece_data.py --file data/raw/ca.csv --state CA   # use local file
python etl/load_ece_data.py --file data/raw/tx.xlsx --columns-only  # inspect columns

# Load IRS 990 data (Phase 5)
python etl/fetch_990_data.py --schools --states CA TX    # charter schools only
python etl/fetch_990_data.py --fqhc --states CA          # health centers only
python etl/fetch_990_data.py --years 3                    # load 3 years of history per org

# Load Opportunity Zone designations (now auto-downloads from IRS)
python etl/load_opportunity_zones.py                            # auto-download from IRS
python etl/load_opportunity_zones.py --file data/raw/oz.xlsx   # use local file

# Load EPA EJScreen environmental justice indicators (now auto-downloads from Zenodo)
# WARNING: downloads a ~5 GB zip file. Use --states to limit what gets loaded.
python etl/load_ejscreen.py                                     # auto-download (national, ~5 GB)
python etl/load_ejscreen.py --states CA TX NY                   # auto-download, load only these states
python etl/load_ejscreen.py --file data/raw/EJSCREEN.csv --states CA TX  # use local file

# Load CDFI directory from CDFI Fund (now auto-downloads)
python etl/load_cdfi_directory.py                               # auto-download from CDFI Fund
python etl/load_cdfi_directory.py --file data/raw/cdfi.xlsx     # use local file

# Load state incentive programs from seed file (Phase 5.5)
python etl/load_state_programs.py                          # uses data/raw/state_programs_seed.csv
python etl/load_state_programs.py --file data/raw/my_programs.csv  # custom file

# Load historical enrollment data from NCES (Phase 5.6)
python etl/fetch_enrollment_trends.py                      # all schools in DB, 5 years
python etl/fetch_enrollment_trends.py --states CA TX       # specific states
python etl/fetch_enrollment_trends.py --years 8            # up to 8 years
python etl/fetch_enrollment_trends.py --charter-only       # charter schools only

# Load CDFI Fund award data (now auto-downloads)
python etl/fetch_cdfi_awards.py                                 # auto-download from CDFI Fund
python etl/fetch_cdfi_awards.py --file data/raw/cdfi_awards.xlsx --states CA TX  # use local file

# Load EDFacts federal LEA accountability (all 50 states, now auto-downloads)
python etl/fetch_edfacts.py --year 2023                         # auto-download math/RLA/grad
python etl/fetch_edfacts.py --year 2023 --states CA TX          # auto-download, filter states

# Load state-specific accountability data (now auto-downloads for TX, CA, NY, FL, etc.)
python etl/fetch_state_accountability.py --state TX --year 2023  # auto-download TX
python etl/fetch_state_accountability.py --all-states --year 2023  # all supported states

# Run tests (when they exist)
pytest tests/
```

## Related Projects

These are separate repos that feed data into or share code patterns with this project:

- Charter school survival model (Python/scikit-learn/Streamlit)
- LEA accountability Shiny app (R/Shiny/SQLite — different stack, data may be imported)
- NMTC tracker (Python — may be absorbed into this project)
- FQHC data pipeline (Python — feeds into this project)
- ECE facility finder (Python — feeds into this project)
