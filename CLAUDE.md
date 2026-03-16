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
│   └── export.py          # CSV/report export functions
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
- `census_tracts` — ACS demographic data, NMTC eligibility indicators (poverty rate, median income, etc.)
- `irs_990` — 990 data for relevant nonprofit operators and funders
- `lea_accountability` — LEA/school-level accountability scores from state DOEs

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

### Phase 3: FQHC/health center data
- HRSA UDS data integration
- Health center markers as a new map layer (toggle on/off)
- Add to search and comparison

### Phase 4: ECE facility data ✅
- State licensing data for early care and education centers
- ECE markers as a new map layer

### Phase 5: 990/philanthropy data
- IRS 990 data for nonprofit facility operators and funders
- Financial health indicators for nonprofit operators

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
