# CD Command Center — Project Handoff & Build Instructions

## What You Are Looking At

This is a Streamlit-based deal origination dashboard for community development finance. The primary use case: a deal originator enters an address or geography and sees every nearby charter school, health center, child care facility, NMTC project, and census tract — with financial health indicators, survival scores, and financing signals layered on top.

**The code is largely complete through Phase 5.5/5.6. The database is almost empty.**

The gap is data, not code. Most ETL scripts exist and work. The task is to run them in the right order, handle the few manual downloads, and then do Phase 6 (auth + PostgreSQL migration).

---

## Current State Audit

### Code (all in good shape)
| File | Lines | Status |
|------|-------|--------|
| `app.py` | 2,291 | Complete through Phase 5.5. Four tabs: Dashboard, Site Detail, Org Lookup, Tools. |
| `db.py` | 2,616 | All DB access functions exist. Single-file migration point for SQLite → PostgreSQL. |
| `utils/geo.py` | — | Census tract lookup, distance calculation |
| `utils/maps.py` | — | Folium map rendering with FeatureGroups |
| `utils/export.py` | — | CSV export |
| `utils/pdf_extractor.py` | — | pdfplumber + regex for audit financial extraction |
| `models/charter_survival.py` | — | Heuristic survival model (trained pkl exists) |
| `etl/` | 20 scripts | All exist. See table below. |

### Database (almost empty)
| Table | Rows | Problem |
|-------|------|---------|
| `schools` | 1,420 | **Charter schools only, ~11 states.** Should be ~100k+ all public schools all 50 states. |
| `census_tracts` | 406 | **TX + CA only.** Should be all 50 states. |
| `lea_accountability` | 200 | Very limited — only a few states loaded. |
| `nmtc_projects` | 0 | Needs CDFI Fund Excel (manual download) |
| `cde_allocations` | 0 | Same file as NMTC |
| `fqhc` | 0 | Auto-downloadable via script |
| `ece_centers` | 0 | Needs state licensing CSV/Excel (varies by state) |
| `irs_990` | 0 | Auto-downloadable via script |
| `irs_990_history` | 0 | Auto-downloadable via script |
| `enrollment_history` | 0 | Auto-downloadable via script |
| `cdfi_directory` | 0 | Needs CDFI Fund Excel (manual download) |
| `cdfi_awards` | 0 | Needs CDFI Fund Excel (manual download) |
| `state_programs` | 0 | Needs seed CSV (create or download) |

---

## Step-by-Step: Load All Data

Work through these in order. Scripts that auto-download are marked **[AUTO]**. Scripts requiring a manual file download are marked **[MANUAL DOWNLOAD]**.

### Step 1 — All Public Schools (fix the biggest gap first)

The database has only charter schools in 11 states. Re-run the fetch to get all public schools for all 50 states. This will upsert — it won't duplicate the existing charters.

```bash
# All 50 states, all public schools (charter + traditional)
# Takes 15-30 minutes. Expect ~95,000 schools total.
python etl/fetch_nces_schools.py

# With race/ethnicity demographics (adds ~50% more API time but worth it):
python etl/fetch_nces_schools.py --demographics

# If you want just specific states first to test:
python etl/fetch_nces_schools.py --states GA NC TX CA NY --demographics
```

Expected result: ~95,000 schools. GA alone should show ~2,400.

### Step 2 — Census Tracts (all 50 states)

Only TX and CA are loaded. Load all states:

```bash
# All states (takes 20-40 minutes depending on ACS API speed)
python etl/load_census_tracts.py --all

# Include 5-year historical data for trend columns (poverty_rate_change, income_change_pct):
python etl/load_census_tracts.py --all --historical

# If --all is too slow, do by region:
python etl/load_census_tracts.py --states AL AR FL GA LA MS NC SC TN TX
python etl/load_census_tracts.py --states CA AZ NV OR WA
python etl/load_census_tracts.py --states NY NJ PA CT MA RI VT NH ME MD DE DC
# ... etc.
```

Expected result: ~73,000 census tracts nationwide.

### Step 3 — Assign Census Tracts to Schools

After loading both schools and tracts, geocode any schools that are missing census tract assignments:

```bash
python etl/assign_census_tracts.py
```

This uses the Census Bureau geocoder API. Expect ~1,000 schools/minute. May need to run twice if some fail on first pass.

### Step 4 — FQHC / Health Centers [AUTO]

Auto-downloads from HRSA. No file needed.

```bash
# All states
python etl/fetch_fqhc.py

# Specific states first to test:
python etl/fetch_fqhc.py --states GA NC TX CA
```

Expected result: ~15,000 health center sites nationwide.

### Step 5 — IRS 990 Data [AUTO]

Auto-downloads from IRS/ProPublica Nonprofit Explorer. Fetches the most recent 990 for each charter school and FQHC already in the database, so run Steps 1 and 4 first.

```bash
# 990s for all charter schools
python etl/fetch_990_data.py --schools

# 990s for health centers
python etl/fetch_990_data.py --fqhc

# Load 3 years of history per org (for trend charts)
python etl/fetch_990_data.py --schools --years 3
python etl/fetch_990_data.py --fqhc --years 3
```

### Step 6 — Enrollment Trends [AUTO]

Pulls historical enrollment per school from the NCES Education Data API. Requires schools to be in the DB first (Step 1).

```bash
# All schools, 5 years of history
python etl/fetch_enrollment_trends.py

# Charter schools only (faster to test):
python etl/fetch_enrollment_trends.py --charter-only --years 5
```

### Step 7 — LEA Accountability

Pulls state accountability ratings (A-F grades, index scores) from EDFacts. The current 200 rows are incomplete.

```bash
python etl/fetch_edfacts.py
# or
python etl/fetch_state_accountability.py
```

Check the docstrings in those scripts — some states may require manual files from state DOE websites.

### Step 8 — NMTC Projects + CDE Allocations [MANUAL DOWNLOAD]

Download the NMTC public data Excel from the CDFI Fund:

1. Go to: https://www.cdfifund.gov/research-and-resources/data-resources
2. Look for "NMTC Public Data Release" — download the most recent Excel file
3. Save to `data/raw/nmtc_public_data_2024.xlsx` (adjust year as needed)

```bash
# Inspect sheet names first
python etl/load_nmtc_data.py --file data/raw/nmtc_public_data_2024.xlsx --sheet-names

# Load (adjust --project-sheet and --cde-sheet to match actual sheet names)
python etl/load_nmtc_data.py --file data/raw/nmtc_public_data_2024.xlsx

# Geocode NMTC projects (assigns lat/lon and census_tract_id)
python etl/geocode_nmtc.py
```

### Step 9 — CDFI Directory [MANUAL DOWNLOAD]

1. Go to: https://www.cdfifund.gov/research-and-resources/data-resources
2. Download the "Certified CDFI List" Excel
3. Save to `data/raw/cdfi_certified_list.xlsx`

```bash
python etl/load_cdfi_directory.py --file data/raw/cdfi_certified_list.xlsx --columns-only  # inspect first
python etl/load_cdfi_directory.py --file data/raw/cdfi_certified_list.xlsx
```

### Step 10 — CDFI Awards [MANUAL DOWNLOAD]

1. Same CDFI Fund data resources page
2. Look for "Awards Data" — Financial Assistance, BEA, CMF programs
3. Save to `data/raw/cdfi_awards.xlsx`

```bash
python etl/fetch_cdfi_awards.py --file data/raw/cdfi_awards.xlsx --columns-only  # inspect first
python etl/fetch_cdfi_awards.py --file data/raw/cdfi_awards.xlsx
```

### Step 11 — ECE / Child Care Centers [MANUAL DOWNLOAD, STATE BY STATE]

No single national source. Download state licensing files individually.

Common sources:
- **Georgia:** https://www.decal.ga.gov/ — look for "Licensed Facilities" data export
- **Texas:** https://hhs.texas.gov/childcare — download "Child Care Facility Search" data
- **California:** https://www.ccld.dss.ca.gov/ — facility licensing export
- **North Carolina:** https://ncchildcare.ncdhhs.gov/

```bash
# Inspect columns before loading (column names vary by state)
python etl/load_ece_data.py --file data/raw/ga_licensed_facilities.csv --state GA --columns-only

# Load (map columns as needed based on --columns-only output)
python etl/load_ece_data.py --file data/raw/ga_licensed_facilities.csv --state GA --source "GA DECAL"
python etl/load_ece_data.py --file data/raw/tx_childcare.csv --state TX --source "TX HHSC"
```

### Step 12 — Opportunity Zone Designations [MANUAL DOWNLOAD]

1. Go to: https://www.irs.gov/pub/irs-utl/Designated_QOZ_8996.xlsx
2. Save to `data/raw/opportunity_zones.xlsx` (or .csv)

```bash
python etl/load_opportunity_zones.py --file data/raw/opportunity_zones.xlsx --columns-only
python etl/load_opportunity_zones.py --file data/raw/opportunity_zones.xlsx
```

### Step 13 — EJScreen Environmental Justice Indicators [MANUAL DOWNLOAD]

1. Go to: https://gaftp.epa.gov/EJSCREEN/2023/
2. Download the national tract-level CSV (large file, ~500MB)
3. Save to `data/raw/EJSCREEN_2023_Tracts.csv`

```bash
# Load for specific states (faster than full national load)
python etl/load_ejscreen.py --file data/raw/EJSCREEN_2023_Tracts.csv --states GA NC TX CA
```

### Step 14 — State Incentive Programs

This one uses a seed CSV. Either create `data/raw/state_programs_seed.csv` with your known programs or run the default:

```bash
python etl/load_state_programs.py
```

The seed CSV format should have columns: `state, program_name, program_type, description, max_credit_pct, notes`. Check the script docstring for exact schema.

---

## Verifying the Load

After loading, check row counts:

```python
import sqlite3
conn = sqlite3.connect('data/cd_command_center.sqlite')
cur = conn.cursor()
cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
tables = [r[0] for r in cur.fetchall()]
for t in tables:
    cur.execute(f'SELECT COUNT(*) FROM {t}')
    print(f'{t}: {cur.fetchone()[0]:,}')
```

Expected final state:
- `schools`: ~95,000 (all public schools, all states)
- `census_tracts`: ~73,000
- `fqhc`: ~15,000
- `nmtc_projects`: ~5,000
- `cde_allocations`: ~1,000
- `irs_990`: ~10,000+
- `irs_990_history`: ~30,000+
- `enrollment_history`: ~500,000+
- `cdfi_directory`: ~1,300
- `cdfi_awards`: ~5,000+

---

## Phase 6: Auth + PostgreSQL Migration (Not Built Yet)

Phase 6 is the only phase not coded. It requires:

### 6a — PostgreSQL Migration

The entire migration is designed to be a single-file change: `db.py` currently uses `sqlite3`. Replace the connection logic with `psycopg2` (or SQLAlchemy). All SQL in `db.py` is already written to be PostgreSQL-compatible (no SQLite-specific syntax was used intentionally).

Steps:
1. Provision a PostgreSQL instance (Render, Supabase, Railway, or local)
2. Run `db.init_db()` against Postgres to create all tables
3. Migrate data: `pg_restore` or re-run all ETL scripts against Postgres
4. Update `db.py`: replace `sqlite3.connect(DB_PATH)` with a `psycopg2` connection pool
5. Update `requirements.txt`: add `psycopg2-binary`
6. Set `DATABASE_URL` env var on Render

Key consideration: SQLite uses `?` for parameter placeholders; PostgreSQL uses `%s`. If db.py uses `?`, every query must be updated. Check and standardize before migrating.

### 6b — Authentication

The app currently has no auth. For internal use, Streamlit's built-in auth (added in Streamlit 1.35) is the simplest path:

```toml
# .streamlit/config.toml
[auth]
redirect_uri = "https://yourapp.onrender.com/oauth2callback"
cookie_secret = "your-secret"
```

Then add `st.login()` / `st.logout()` and `st.experimental_user` checks at the top of `app.py`. For a private internal tool, Google OAuth is the easiest provider.

For multi-user deployments with per-user notes and bookmarks (currently shared in the `user_notes` and `user_bookmarks` tables), the tables will need a `user_id` column added and queries updated to filter by the logged-in user.

### 6c — Performance

At 100k schools the map can get slow. Improvements to make before external deployment:
- Add `@st.cache_data` (TTL ~5 min) to all `db.get_*` calls in `app.py`
- Add database indexes on `state`, `census_tract_id`, `latitude`, `longitude` for all facility tables
- For the map: cluster markers at zoom < 8 using Folium's `MarkerCluster`
- For the data table: use `st.dataframe` with server-side pagination rather than loading all rows

---

## Architecture Rules (Do Not Break)

1. **All SQL goes in `db.py`.** Never write raw queries in `app.py`. This is what makes the PostgreSQL migration possible.
2. **`data/raw/` is gitignored.** Never commit raw source files. Only commit the SQLite database (or not, if it's too large — use ETL scripts as the source of truth).
3. **No frontend frameworks.** Streamlit only. No React, no custom JS.
4. **Census tract is the geographic join key.** Every facility must have `census_tract_id`. Run `assign_census_tracts.py` after any bulk load.

---

## Running the App

```bash
pip install -r requirements.txt
streamlit run app.py
```

The app runs on `http://localhost:8501`. The sidebar has layer toggles, filters, and radius search. Four tabs: Dashboard (map + data table), Site Detail (deep dive on one facility), Org Lookup (search by org name or EIN), Tools (NMTC pro forma, gap analysis, peer comps, CDFI market activity).

---

## What's Working vs. What Needs Data

| Feature | Code | Data |
|---------|------|------|
| Map with all layers | ✅ | ❌ most layers empty |
| Charter school survival scores | ✅ | ✅ (charter schools loaded) |
| Traditional public school data | ✅ | ❌ only charter schools loaded |
| NMTC projects on map | ✅ | ❌ needs CDFI Fund file |
| CDE allocations | ✅ | ❌ needs CDFI Fund file |
| FQHC health centers | ✅ | ❌ run fetch_fqhc.py |
| ECE child care centers | ✅ | ❌ needs state files |
| Census tract demographics | ✅ | ⚠️ TX+CA only, need all states |
| NMTC eligibility tier | ✅ | ⚠️ partial |
| Opportunity Zone overlay | ✅ | ❌ needs IRS file |
| EJScreen indicators | ✅ | ❌ needs EPA file |
| 990 financial data | ✅ | ❌ run fetch_990_data.py |
| Financial ratios | ✅ | ❌ depends on 990 data |
| Enrollment trends | ✅ | ❌ run fetch_enrollment_trends.py |
| LEA accountability | ✅ | ⚠️ only 200 rows |
| CDFI directory | ✅ | ❌ needs CDFI Fund file |
| CDFI awards | ✅ | ❌ needs CDFI Fund file |
| State incentive programs | ✅ | ❌ needs seed CSV |
| Org Lookup tab | ✅ | ❌ depends on 990 data |
| Notes + bookmarks | ✅ | ✅ (works, no seed data needed) |
| Document upload + PDF extraction | ✅ | ✅ (works, no seed data needed) |
| NMTC pro forma calculator | ✅ | ✅ (no data needed, interactive) |
| Service gap analysis | ✅ | ❌ needs ECE + FQHC data |
| Radius search | ✅ | ⚠️ works but limited by sparse data |
| Auth + user accounts | ❌ | — |
| PostgreSQL migration | ❌ | — |
