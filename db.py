"""
db.py — All database access for CD Command Center.

This is the ONLY place that talks to the database. All page files and ETL scripts
call functions from this module. This makes it possible to swap SQLite for PostgreSQL
later by changing only this file.

For PostgreSQL migration: replace sqlite3 with psycopg2 (or SQLAlchemy),
update get_connection(), and the SQL syntax differences (e.g., ? → %s for params).
"""

import sqlite3
import os
import pandas as pd

# Streamlit caching — imported conditionally so ETL scripts don't need Streamlit
try:
    import streamlit as st
    _HAS_STREAMLIT = True
except ImportError:
    _HAS_STREAMLIT = False

# Path to the SQLite database file
DB_PATH = os.path.join(os.path.dirname(__file__), "data", "cd_command_center.sqlite")


def get_connection():
    """Return a connection to the SQLite database."""
    conn = sqlite3.connect(DB_PATH)
    # Return rows as dicts so we can use column names (like a Postgres cursor)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    """
    Create all tables if they don't exist yet.
    Call this once at app startup or from ETL scripts.
    """
    conn = get_connection()
    cur = conn.cursor()

    # Schools — one row per school site (all public schools, not just charters)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS schools (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            nces_id TEXT UNIQUE,           -- National Center for Ed Stats school ID
            school_name TEXT NOT NULL,
            lea_name TEXT,                 -- Local education agency (district) name
            lea_id TEXT,                   -- LEA NCES ID (joins to lea_accountability)
            state TEXT,
            city TEXT,
            address TEXT,
            zip_code TEXT,
            county TEXT,
            census_tract_id TEXT,          -- 11-digit FIPS census tract
            latitude REAL,
            longitude REAL,
            enrollment INTEGER,
            grade_low TEXT,
            grade_high TEXT,
            is_charter INTEGER DEFAULT 0,  -- 1 = charter school, 0 = traditional public
            -- Demographics (shares of enrollment)
            pct_free_reduced_lunch REAL,
            pct_ell REAL,                  -- English language learners
            pct_sped REAL,                 -- Special education
            pct_black REAL,
            pct_hispanic REAL,
            pct_white REAL,
            -- Status
            school_status TEXT,            -- e.g. 'Open', 'Closed', 'Pending'
            year_opened INTEGER,
            year_closed INTEGER,
            -- Survival model output (charter schools only)
            survival_score REAL,           -- 0–1 probability of remaining open
            survival_risk_tier TEXT,       -- 'Low', 'Medium', 'High'
            -- Metadata
            data_year INTEGER,             -- School year the data represents
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Migrate from old charter_schools table if it exists
    try:
        cur.execute("SELECT COUNT(*) FROM charter_schools")
        old_count = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM schools")
        new_count = cur.fetchone()[0]
        if old_count > 0 and new_count == 0:
            # Copy data from old table, setting is_charter=1 since old table was charter-only
            cur.execute("""
                INSERT INTO schools (
                    nces_id, school_name, lea_name, lea_id, state, city, address,
                    zip_code, county, census_tract_id, latitude, longitude, enrollment,
                    grade_low, grade_high, is_charter,
                    pct_free_reduced_lunch, pct_ell, pct_sped, pct_black, pct_hispanic, pct_white,
                    school_status, year_opened, year_closed,
                    survival_score, survival_risk_tier, data_year, created_at, updated_at
                )
                SELECT
                    nces_id, school_name, lea_name, lea_id, state, city, address,
                    zip_code, county, census_tract_id, latitude, longitude, enrollment,
                    grade_low, grade_high, 1,
                    pct_free_reduced_lunch, pct_ell, pct_sped, pct_black, pct_hispanic, pct_white,
                    school_status, year_opened, year_closed,
                    survival_score, survival_risk_tier, data_year, created_at, updated_at
                FROM charter_schools
            """)
            print(f"  Migrated {old_count:,} records from charter_schools → schools table")
    except Exception:
        pass  # charter_schools doesn't exist, that's fine

    # Add is_charter column to schools table if it was created without it
    try:
        cur.execute("ALTER TABLE schools ADD COLUMN is_charter INTEGER DEFAULT 0")
    except Exception:
        pass

    # LEA (district) accountability scores
    cur.execute("""
        CREATE TABLE IF NOT EXISTS lea_accountability (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            lea_id TEXT,                   -- NCES LEA ID (joins to schools.lea_id)
            lea_name TEXT,
            state TEXT,
            accountability_score REAL,     -- State-reported composite score
            accountability_rating TEXT,    -- e.g. 'A', 'B', 'Comprehensive Support'
            proficiency_reading REAL,      -- % proficient in reading
            proficiency_math REAL,         -- % proficient in math
            graduation_rate REAL,
            data_year INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE (lea_id, data_year)
        )
    """)

    # Census tracts — demographic data and NMTC eligibility
    cur.execute("""
        CREATE TABLE IF NOT EXISTS census_tracts (
            census_tract_id TEXT PRIMARY KEY,   -- 11-digit FIPS code
            state_fips TEXT,
            county_fips TEXT,
            tract_name TEXT,
            -- ACS demographics
            total_population INTEGER,
            median_household_income REAL,       -- median household income (ACS B19013)
            median_family_income REAL,          -- median family income (ACS B19113, used for NMTC LIC)
            poverty_rate REAL,                  -- % below poverty line
            pct_minority REAL,
            unemployment_rate REAL,
            -- NMTC eligibility tiers (Low-Income Community criteria)
            is_nmtc_eligible INTEGER,           -- 1 = eligible (LIC or higher), 0 = not
            nmtc_eligibility_reason TEXT,       -- 'Poverty', 'Income', 'Both'
            nmtc_eligibility_tier TEXT,         -- 'Not Eligible', 'LIC', 'Severely Distressed', 'Deep Distress'
            -- Geography
            county_name TEXT,
            state TEXT,
            data_year INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Add columns to existing census_tracts tables that predate these
    try:
        cur.execute("ALTER TABLE census_tracts ADD COLUMN median_family_income REAL")
    except Exception:
        pass
    try:
        cur.execute("ALTER TABLE census_tracts ADD COLUMN nmtc_eligibility_tier TEXT")
    except Exception:
        pass

    # Phase A-B: new census_tract columns for OZ, EJScreen, trend analysis, and gap analysis
    new_census_cols = [
        ("is_opportunity_zone", "INTEGER DEFAULT 0"),  # 1 = Treasury-designated OZ
        ("ej_index", "REAL"),                          # EPA EJScreen composite score (0-100 percentile)
        ("pm25_percentile", "REAL"),                   # Particulate matter 2.5 (EJScreen)
        ("diesel_percentile", "REAL"),                 # Diesel particulate exposure (EJScreen)
        ("lead_paint_percentile", "REAL"),             # Lead paint indicator (EJScreen)
        ("superfund_percentile", "REAL"),              # Proximity to Superfund sites (EJScreen)
        ("wastewater_percentile", "REAL"),             # Wastewater discharge proximity (EJScreen)
        ("poverty_rate_5yr_ago", "REAL"),              # Poverty rate from 5 years prior (ACS)
        ("median_income_5yr_ago", "REAL"),             # Median income from 5 years prior (ACS)
        ("poverty_rate_change", "REAL"),               # poverty_rate - poverty_rate_5yr_ago
        ("income_change_pct", "REAL"),                 # % change in median income over 5 years
        ("pop_under_5", "INTEGER"),                    # ACS: population under 5 (ECE gap analysis)
        ("pop_under_18", "INTEGER"),                   # ACS: population under 18 (school gap analysis)
        ("pop_uninsured", "INTEGER"),                  # ACS: uninsured population (FQHC gap analysis)
        ("pop_65_plus", "INTEGER"),                    # ACS: population 65+ (elder care)
    ]
    for col, col_type in new_census_cols:
        try:
            cur.execute(f"ALTER TABLE census_tracts ADD COLUMN {col} {col_type}")
        except Exception:
            pass  # column already exists

    # NMTC projects — project-level QALICB investments from CDFI Fund public data release
    cur.execute("""
        CREATE TABLE IF NOT EXISTS nmtc_projects (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            cdfi_project_id TEXT UNIQUE,        -- CDFI Fund internal project identifier
            cde_name TEXT,                      -- Community Development Entity name
            cde_id TEXT,                        -- CDE identifier
            project_name TEXT,
            project_type TEXT,                  -- 'Real Estate' or 'Non-Real Estate'
            state TEXT,
            city TEXT,
            address TEXT,
            zip_code TEXT,
            census_tract_id TEXT,               -- 11-digit FIPS (joins to census_tracts)
            latitude REAL,
            longitude REAL,
            total_investment REAL,              -- total project investment in dollars
            qlici_amount REAL,                  -- Qualified Low-Income Community Investment amount
            allocation_year INTEGER,            -- year CDE received the NMTC allocation
            fiscal_year INTEGER,                -- fiscal year the investment was made
            jobs_created INTEGER,
            jobs_retained INTEGER,
            project_description TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # CDE allocations — CDE-level NMTC allocation awards from CDFI Fund
    cur.execute("""
        CREATE TABLE IF NOT EXISTS cde_allocations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            cde_name TEXT NOT NULL,
            cde_id TEXT,
            state TEXT,                         -- CDE headquarters state
            city TEXT,
            hq_address TEXT,
            allocation_amount REAL,             -- total NMTC allocation awarded (dollars)
            allocation_year INTEGER,            -- calendar year of award
            round_number INTEGER,               -- NMTC application round number
            amount_deployed REAL,               -- amount invested to date (from project data)
            service_areas TEXT,                 -- text description of service geography
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE (cde_name, allocation_year)
        )
    """)

    # FQHC (Federally Qualified Health Centers) — HRSA Health Center Program site-level data
    cur.execute("""
        CREATE TABLE IF NOT EXISTS fqhc (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            bhcmis_id TEXT UNIQUE,             -- HRSA unique site identifier
            health_center_name TEXT,           -- parent health center organization
            site_name TEXT,                    -- specific site name
            site_address TEXT,
            city TEXT,
            state TEXT,
            zip_code TEXT,
            county TEXT,
            census_tract_id TEXT,              -- 11-digit FIPS (joins to census_tracts)
            latitude REAL,
            longitude REAL,
            site_type TEXT,                    -- 'Health Center', 'School-Based', 'Mobile', etc.
            is_active INTEGER DEFAULT 1,       -- 1 = active, 0 = inactive/closed
            health_center_type TEXT,           -- 'FQHC', 'Look-Alike', 'Health Center Program Grantee'
            -- UDS patient data (from annual UDS report, if available)
            total_patients INTEGER,
            patients_below_200pct_poverty INTEGER,  -- patients at or below 200% federal poverty level
            data_year INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # ECE centers — state-licensed early care and education facilities
    cur.execute("""
        CREATE TABLE IF NOT EXISTS ece_centers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            license_id TEXT UNIQUE,            -- state-assigned license/credential number
            provider_name TEXT NOT NULL,       -- facility / operator name
            facility_type TEXT,                -- 'Center', 'Family Child Care Home', 'Group Home', etc.
            license_type TEXT,                 -- type of license (varies by state)
            license_status TEXT,               -- 'Active', 'Inactive', 'Revoked', 'Provisional', etc.
            capacity INTEGER,                  -- licensed capacity (max children at one time)
            ages_served TEXT,                  -- free-text description of ages served
            accepts_subsidies INTEGER,         -- 1 = accepts CCDF vouchers / subsidized care
            star_rating REAL,                  -- QRIS quality star rating (if state uses one)
            operator_name TEXT,                -- operating organization (if different from provider_name)
            -- Location
            address TEXT,
            city TEXT,
            state TEXT,
            zip_code TEXT,
            county TEXT,
            census_tract_id TEXT,              -- 11-digit FIPS (joins to census_tracts)
            latitude REAL,
            longitude REAL,
            -- Source / vintage
            data_year INTEGER,
            data_source TEXT,                  -- e.g. 'CA CCLD', 'TX HHSC', 'NY OCFS'
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # ---------------------------------------------------------------------------
    # Indexes — added after all tables so CREATE INDEX IF NOT EXISTS is safe to
    # run on an existing DB. This means re-running init_db() on startup will
    # add indexes to any DB that was created before this code was added.
    # ---------------------------------------------------------------------------

    # schools: all columns that appear in WHERE clauses or the LEA JOIN
    cur.execute("CREATE INDEX IF NOT EXISTS idx_schools_state      ON schools(state)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_schools_is_charter ON schools(is_charter)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_schools_status     ON schools(school_status)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_schools_risk_tier  ON schools(survival_risk_tier)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_schools_lea_id     ON schools(lea_id)")

    # lea_accountability: JOIN key — without this, every LEA lookup is a full scan
    cur.execute("CREATE INDEX IF NOT EXISTS idx_lea_id ON lea_accountability(lea_id)")

    # census_tracts: state + eligibility tier + poverty rate are the common filters
    cur.execute("CREATE INDEX IF NOT EXISTS idx_tracts_state       ON census_tracts(state)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_tracts_eligibility ON census_tracts(nmtc_eligibility_tier)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_tracts_poverty     ON census_tracts(poverty_rate)")

    # nmtc_projects
    cur.execute("CREATE INDEX IF NOT EXISTS idx_nmtc_state         ON nmtc_projects(state)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_nmtc_tract         ON nmtc_projects(census_tract_id)")

    # fqhc
    cur.execute("CREATE INDEX IF NOT EXISTS idx_fqhc_state         ON fqhc(state)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_fqhc_active        ON fqhc(is_active)")

    # ece_centers
    cur.execute("CREATE INDEX IF NOT EXISTS idx_ece_state          ON ece_centers(state)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_ece_status         ON ece_centers(license_status)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_ece_capacity       ON ece_centers(capacity)")

    # irs_990 — IRS Form 990 data for nonprofit facility operators and funders
    cur.execute("""
        CREATE TABLE IF NOT EXISTS irs_990 (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ein TEXT UNIQUE NOT NULL,          -- 9-digit EIN (no dashes)
            org_name TEXT,
            city TEXT,
            state TEXT,
            ntee_code TEXT,                    -- e.g. 'B29' = charter school, 'E32' = community health
            subsection_code INTEGER,           -- 3 = 501(c)(3), 4 = 501(c)(4), etc.
            -- Financial data from most recent 990 filing
            total_revenue REAL,
            total_expenses REAL,
            total_assets REAL,
            net_income REAL,                   -- total_revenue - total_expenses
            program_service_revenue REAL,      -- revenue from mission-related programs
            program_service_expenses REAL,     -- spending on mission-related programs
            officer_compensation REAL,         -- top officer/executive compensation
            tax_year INTEGER,                  -- fiscal year the financials cover
            filing_pdf_url TEXT,               -- link to the actual 990 PDF on ProPublica
            -- Metadata
            data_source TEXT DEFAULT 'ProPublica',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_990_state ON irs_990(state)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_990_ntee  ON irs_990(ntee_code)")

    # IRS 990 multi-year history — one row per (ein, tax_year).
    # The main irs_990 table keeps the most recent year per org (unchanged).
    # This companion table stores all fetched years for trend charts.
    cur.execute("""
        CREATE TABLE IF NOT EXISTS irs_990_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ein TEXT NOT NULL,
            org_name TEXT,
            total_revenue REAL,
            total_expenses REAL,
            total_assets REAL,
            net_income REAL,
            program_service_revenue REAL,
            program_service_expenses REAL,
            officer_compensation REAL,
            tax_year INTEGER NOT NULL,
            filing_pdf_url TEXT,
            data_source TEXT DEFAULT 'ProPublica',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(ein, tax_year)
        )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_990h_ein ON irs_990_history(ein)")

    # CDFI directory — certified CDFIs from the CDFI Fund
    cur.execute("""
        CREATE TABLE IF NOT EXISTS cdfi_directory (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            cdfi_name TEXT NOT NULL,
            city TEXT,
            state TEXT,
            cdfi_type TEXT,           -- 'Loan Fund', 'Credit Union', 'Community Development Bank', 'VC'
            total_assets REAL,
            primary_markets TEXT,     -- geographic service area description
            target_populations TEXT,  -- e.g. 'Rural', 'Native American', 'Women'
            certification_date TEXT,
            website TEXT,
            updated_at TEXT,
            UNIQUE(cdfi_name, state)
        )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_cdfi_state ON cdfi_directory(state)")

    # State incentive programs — manually curated reference data per state.
    # Covers historic tax credits, state NMTCs, LIHTC, and other CD finance programs.
    cur.execute("""
        CREATE TABLE IF NOT EXISTS state_programs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            state TEXT NOT NULL,
            program_name TEXT NOT NULL,
            program_type TEXT,        -- 'Historic Tax Credit', 'State NMTC', 'LIHTC', 'Grant', 'Loan'
            eligible_uses TEXT,       -- 'Real estate', 'Operating', 'Equipment'
            max_credit_pct REAL,      -- e.g. 25 means 25% state historic tax credit
            max_amount REAL,          -- dollar cap per project if applicable
            administering_agency TEXT,
            website TEXT,
            notes TEXT,
            last_verified TEXT        -- date the info was last manually reviewed
        )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_programs_state ON state_programs(state)")

    # Add ein column to schools and fqhc tables if it doesn't exist yet.
    # ALTER TABLE only runs on existing DBs — new DBs get the column from init.
    # We use try/except because SQLite has no "ADD COLUMN IF NOT EXISTS".
    for table, col in [("schools", "ein"), ("fqhc", "ein")]:
        try:
            cur.execute(f"ALTER TABLE {table} ADD COLUMN {col} TEXT")
        except Exception:
            pass  # column already exists

    # Add extra financial columns to irs_990 and irs_990_history for ratio calculations.
    # These map to Part X of the 990 form: balance sheet items.
    extra_990_cols = [
        ("total_liabilities",        "REAL"),   # Part X line 26 total liabilities
        ("unrestricted_net_assets",  "REAL"),   # Part X line 27
        ("cash_savings",             "REAL"),   # Part X line 1 (cash & savings) — acid ratio numerator
        ("accounts_payable",         "REAL"),   # Part X line 17 — proxy current liabilities
        ("accrued_expenses",         "REAL"),   # Part X line 18 — proxy current liabilities
        ("notes_payable",            "REAL"),   # Part X lines 19-20 — component of total debt
    ]
    for col, col_type in extra_990_cols:
        for tbl in ("irs_990", "irs_990_history"):
            try:
                cur.execute(f"ALTER TABLE {tbl} ADD COLUMN {col} {col_type}")
            except Exception:
                pass

    # Enrollment history — NCES historical enrollment per school, one row per year
    cur.execute("""
        CREATE TABLE IF NOT EXISTS enrollment_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            nces_id TEXT NOT NULL,
            school_year INTEGER NOT NULL,     -- e.g. 2023 = school year 2022-23
            enrollment INTEGER,
            pct_free_reduced_lunch REAL,
            pct_black REAL,
            pct_hispanic REAL,
            pct_white REAL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(nces_id, school_year)
        )
    """)
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_enroll_nces ON enrollment_history(nces_id)"
    )

    # CDFI awards — CDFI Fund Financial Assistance and other award programs by awardee
    cur.execute("""
        CREATE TABLE IF NOT EXISTS cdfi_awards (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            award_year INTEGER,
            program TEXT,               -- 'FA', 'TA', 'BEA', 'NMTC', 'CMF', 'Bond Guarantee'
            awardee_name TEXT,
            awardee_state TEXT,
            awardee_city TEXT,
            award_amount REAL,
            award_type TEXT,            -- 'Grant', 'Loan', 'Credit', 'Guarantee'
            cdfi_type TEXT,             -- 'Loan Fund', 'Credit Union', 'Community Bank', etc.
            purpose TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(award_year, program, awardee_name)
        )
    """)
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_awards_state ON cdfi_awards(awardee_state)"
    )
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_awards_year  ON cdfi_awards(award_year)"
    )

    # User notes — per-entity freetext notes saved locally
    cur.execute("""
        CREATE TABLE IF NOT EXISTS user_notes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            entity_type TEXT NOT NULL,   -- 'school', 'fqhc', 'ece', 'nmtc', 'tract'
            entity_id TEXT NOT NULL,     -- nces_id, bhcmis_id, etc.
            note_text TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_notes_entity ON user_notes(entity_type, entity_id)"
    )

    # User bookmarks — saved entities for quick access from the sidebar
    cur.execute("""
        CREATE TABLE IF NOT EXISTS user_bookmarks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            entity_type TEXT NOT NULL,
            entity_id TEXT NOT NULL,
            label TEXT,                  -- display name shown in bookmark list
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(entity_type, entity_id)
        )
    """)

    # Documents — uploaded files (audits, financial statements) stored per EIN / entity
    cur.execute("""
        CREATE TABLE IF NOT EXISTS documents (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ein TEXT,                    -- EIN of the organization (nullable)
            entity_type TEXT,            -- 'school', 'fqhc', 'general'
            entity_id TEXT,              -- nces_id, bhcmis_id, etc.
            filename TEXT NOT NULL,
            filepath TEXT NOT NULL,      -- path relative to project root (data/uploads/...)
            doc_type TEXT,               -- 'Audit', 'Financial Statements', '990', 'Other'
            fiscal_year INTEGER,         -- fiscal year the document covers
            extracted_data TEXT,         -- JSON blob of financial line items parsed from PDF
            verified INTEGER DEFAULT 0,  -- 1 = user confirmed extracted values are correct
            upload_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_docs_ein    ON documents(ein)")
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_docs_entity ON documents(entity_type, entity_id)"
    )

    # Financial ratios — computed once per EIN per fiscal year; refreshed when new data arrives
    cur.execute("""
        CREATE TABLE IF NOT EXISTS financial_ratios (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ein TEXT NOT NULL,
            fiscal_year INTEGER NOT NULL,
            -- Acid ratio = cash / current_liabilities
            --   990-based: cash_savings / (accounts_payable + accrued_expenses) — approximate
            --   audit-based: precise current asset / current liability split
            acid_ratio_990 REAL,
            acid_ratio_audit REAL,
            -- Leverage ratio = unrestricted_net_assets / total_debt
            leverage_ratio REAL,
            -- 3-year average operating cash flow (approximated from irs_990_history)
            avg_operating_cash_flow REAL,
            -- Raw inputs stored for transparency / manual override
            cash_and_equivalents REAL,
            accounts_payable REAL,
            accrued_expenses REAL,
            current_liabilities_audit REAL,   -- from audit PDF (more accurate)
            unrestricted_net_assets REAL,
            total_liabilities REAL,
            total_debt REAL,                  -- notes payable + mortgages
            has_audit_data INTEGER DEFAULT 0, -- 1 = audit PDF was used
            data_source TEXT,                 -- '990', 'Audit', 'Manual'
            calculated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(ein, fiscal_year)
        )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_ratios_ein ON financial_ratios(ein)")

    conn.commit()
    conn.close()


# ---------------------------------------------------------------------------
# Caching helper — wraps @st.cache_data when Streamlit is available
# ---------------------------------------------------------------------------

def _cached(ttl=300):
    """Decorator: applies @st.cache_data(ttl=ttl) if Streamlit is loaded."""
    def decorator(func):
        if _HAS_STREAMLIT:
            return st.cache_data(ttl=ttl, show_spinner=False)(func)
        return func
    return decorator


# ---------------------------------------------------------------------------
# School queries (formerly charter_schools)
# ---------------------------------------------------------------------------

@_cached(ttl=300)
def get_schools(
    states=None,
    min_enrollment=None,
    max_enrollment=None,
    risk_tiers=None,
    min_survival_score=None,
    max_survival_score=None,
    school_status=None,
    county=None,
    census_tract_id=None,
    charter_only=False,
    nmtc_eligible_only=False,
) -> pd.DataFrame:
    """
    Return schools matching the given filters as a DataFrame.
    All parameters are optional — omitting them returns all schools.

    Joins to census_tracts to include nmtc_eligibility_tier and
    is_nmtc_eligible on every row — useful for deal origination filtering.
    Also includes has_990 (1/0) based on whether an EIN is linked.

    Args:
        states: list of state abbreviations, e.g. ['CA', 'TX']
        min_enrollment: minimum enrollment (inclusive)
        max_enrollment: maximum enrollment (inclusive)
        risk_tiers: list of risk tier labels, e.g. ['High', 'Medium']
        min_survival_score: minimum survival score 0–1
        max_survival_score: maximum survival score 0–1
        school_status: list of status strings, e.g. ['Open']
        county: county name substring match
        census_tract_id: exact census tract FIPS code
        charter_only: if True, only return charter schools (is_charter=1)
        nmtc_eligible_only: if True, only return schools in NMTC-eligible tracts
    """
    conditions = []
    params = []

    if charter_only:
        conditions.append("s.is_charter = 1")

    if states:
        placeholders = ",".join("?" * len(states))
        conditions.append(f"s.state IN ({placeholders})")
        params.extend(states)

    if min_enrollment is not None:
        conditions.append("s.enrollment >= ?")
        params.append(min_enrollment)

    if max_enrollment is not None:
        conditions.append("s.enrollment <= ?")
        params.append(max_enrollment)

    if risk_tiers:
        placeholders = ",".join("?" * len(risk_tiers))
        conditions.append(f"s.survival_risk_tier IN ({placeholders})")
        params.extend(risk_tiers)

    if min_survival_score is not None:
        conditions.append("s.survival_score >= ?")
        params.append(min_survival_score)

    if max_survival_score is not None:
        conditions.append("s.survival_score <= ?")
        params.append(max_survival_score)

    if school_status:
        placeholders = ",".join("?" * len(school_status))
        conditions.append(f"s.school_status IN ({placeholders})")
        params.extend(school_status)

    if county:
        conditions.append("s.county LIKE ?")
        params.append(f"%{county}%")

    if census_tract_id:
        conditions.append("s.census_tract_id = ?")
        params.append(census_tract_id)

    if nmtc_eligible_only:
        # Filter to schools in NMTC-eligible census tracts (LIC, Severely Distressed, Deep Distress)
        conditions.append("ct.is_nmtc_eligible = 1")

    where_clause = "WHERE " + " AND ".join(conditions) if conditions else ""

    # Try to query from the 'schools' table; fall back to 'charter_schools' for old DBs.
    #
    # The LEA join uses a CTE (WITH latest_lea ...) instead of a correlated subquery.
    # A correlated subquery runs once per school row; the CTE runs once and is reused,
    # making it O(schools + districts) instead of O(schools * districts).
    #
    # We also LEFT JOIN census_tracts to expose NMTC eligibility tier on each row,
    # and compute has_990 (1 if the school has an EIN linked, 0 otherwise) so the
    # dashboard can quickly flag which schools have financial data available.
    for table_name in ["schools", "charter_schools"]:
        try:
            t = table_name[0]   # alias: 's' for schools, 'c' for charter_schools
            query = f"""
                WITH latest_lea AS (
                    SELECT lea_id, MAX(data_year) AS max_year
                    FROM lea_accountability
                    GROUP BY lea_id
                )
                SELECT
                    {t}.*,
                    la.accountability_score,
                    la.accountability_rating,
                    la.proficiency_reading,
                    la.proficiency_math,
                    -- Use detailed tier when available, otherwise fall back to
                    -- is_nmtc_eligible flag so schools in older/partial tract
                    -- data still show as eligible/not-eligible
                    CASE
                        WHEN ct.nmtc_eligibility_tier IS NOT NULL THEN ct.nmtc_eligibility_tier
                        WHEN ct.is_nmtc_eligible = 1 THEN 'Eligible'
                        WHEN ct.is_nmtc_eligible = 0 THEN 'Not Eligible'
                        ELSE NULL
                    END AS nmtc_eligibility_tier,
                    ct.is_nmtc_eligible,
                    ct.poverty_rate       AS tract_poverty_rate,
                    ct.median_household_income AS tract_median_income,
                    CASE WHEN {t}.ein IS NOT NULL THEN 1 ELSE 0 END AS has_990
                FROM {table_name} {t}
                LEFT JOIN latest_lea ll
                    ON {t}.lea_id = ll.lea_id
                LEFT JOIN lea_accountability la
                    ON la.lea_id = ll.lea_id
                    AND la.data_year = ll.max_year
                LEFT JOIN census_tracts ct
                    ON {t}.census_tract_id = ct.census_tract_id
                {where_clause}
                ORDER BY {t}.school_name
            """
            conn = get_connection()
            df = pd.read_sql_query(query, conn, params=params)
            conn.close()
            return df
        except Exception:
            continue

    return pd.DataFrame()


# Backward-compatible wrappers for code that still uses old names
def get_charter_schools(**kwargs) -> pd.DataFrame:
    """Backward-compatible wrapper: calls get_schools(charter_only=True)."""
    kwargs["charter_only"] = True
    return get_schools(**kwargs)


@_cached(ttl=300)
def get_school_by_id(school_id: int) -> dict:
    """Return a single school by its primary key id."""
    conn = get_connection()
    cur = conn.cursor()
    for table in ["schools", "charter_schools"]:
        try:
            cur.execute(f"SELECT * FROM {table} WHERE id = ?", (school_id,))
            row = cur.fetchone()
            if row:
                conn.close()
                return dict(row)
        except Exception:
            continue
    conn.close()
    return {}


def get_charter_school_by_id(school_id: int) -> dict:
    """Backward-compatible wrapper."""
    return get_school_by_id(school_id)


@_cached(ttl=3600)   # state lists change rarely — cache for 1 hour
def get_school_states() -> list:
    """Return sorted list of states that have school data."""
    conn = get_connection()
    cur = conn.cursor()
    for table in ["schools", "charter_schools"]:
        try:
            cur.execute(f"SELECT DISTINCT state FROM {table} WHERE state IS NOT NULL ORDER BY state")
            states = [row[0] for row in cur.fetchall()]
            conn.close()
            return states
        except Exception:
            continue
    conn.close()
    return []


def get_charter_school_states() -> list:
    """Backward-compatible wrapper."""
    return get_school_states()


@_cached(ttl=300)
def get_school_summary(charter_only=False) -> dict:
    """Return high-level summary counts for the dashboard header."""
    conn = get_connection()
    cur = conn.cursor()
    charter_filter = "WHERE is_charter = 1" if charter_only else ""

    for table in ["schools", "charter_schools"]:
        try:
            cur.execute(f"""
                SELECT
                    COUNT(*) as total_schools,
                    SUM(CASE WHEN school_status = 'Open' THEN 1 ELSE 0 END) as open_schools,
                    SUM(CASE WHEN survival_risk_tier = 'High' THEN 1 ELSE 0 END) as high_risk_schools,
                    AVG(survival_score) as avg_survival_score,
                    SUM(enrollment) as total_enrollment
                FROM {table}
                {charter_filter}
            """)
            row = cur.fetchone()
            conn.close()
            return dict(row) if row else {}
        except Exception:
            continue
    conn.close()
    return {}


def get_charter_school_summary() -> dict:
    """Backward-compatible wrapper."""
    return get_school_summary(charter_only=True)


# ---------------------------------------------------------------------------
# Census tract queries
# ---------------------------------------------------------------------------

@_cached(ttl=300)
def get_census_tract(census_tract_id: str) -> dict:
    """Return demographic data for a single census tract."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT * FROM census_tracts WHERE census_tract_id = ?", (census_tract_id,))
    row = cur.fetchone()
    conn.close()
    return dict(row) if row else {}


@_cached(ttl=300)
def get_nmtc_eligible_tracts(states=None) -> pd.DataFrame:
    """Return all NMTC-eligible census tracts, optionally filtered by state."""
    conditions = ["is_nmtc_eligible = 1"]
    params = []
    if states:
        placeholders = ",".join("?" * len(states))
        conditions.append(f"state IN ({placeholders})")
        params.extend(states)

    where_clause = "WHERE " + " AND ".join(conditions)
    query = f"SELECT * FROM census_tracts {where_clause} ORDER BY state, census_tract_id"

    conn = get_connection()
    df = pd.read_sql_query(query, conn, params=params)
    conn.close()
    return df


@_cached(ttl=300)
def get_census_tracts(
    states=None,
    min_poverty_rate=None,
    max_median_income=None,
    nmtc_eligible_only=False,
    eligibility_tiers=None,
    county_fips=None,
) -> pd.DataFrame:
    """
    Return census tracts matching the given filters as a DataFrame.

    Args:
        states: list of state abbreviations, e.g. ['CA', 'TX']
        min_poverty_rate: minimum poverty rate % (inclusive)
        max_median_income: maximum median family income in dollars (inclusive)
        nmtc_eligible_only: if True, only return tracts with is_nmtc_eligible=1
        eligibility_tiers: list of tier strings, e.g. ['Severely Distressed', 'Deep Distress']
        county_fips: 5-digit county FIPS code to filter to a single county
    """
    conditions = []
    params = []

    if states:
        placeholders = ",".join("?" * len(states))
        conditions.append(f"state IN ({placeholders})")
        params.extend(states)

    if nmtc_eligible_only:
        conditions.append("is_nmtc_eligible = 1")

    if eligibility_tiers:
        placeholders = ",".join("?" * len(eligibility_tiers))
        conditions.append(f"nmtc_eligibility_tier IN ({placeholders})")
        params.extend(eligibility_tiers)

    if min_poverty_rate is not None:
        conditions.append("poverty_rate >= ?")
        params.append(min_poverty_rate)

    if max_median_income is not None:
        conditions.append("(median_family_income <= ? OR median_family_income IS NULL)")
        params.append(max_median_income)

    if county_fips:
        conditions.append("county_fips = ?")
        params.append(county_fips)

    where_clause = "WHERE " + " AND ".join(conditions) if conditions else ""
    query = f"SELECT * FROM census_tracts {where_clause} ORDER BY state, poverty_rate DESC"

    conn = get_connection()
    df = pd.read_sql_query(query, conn, params=params)
    conn.close()
    return df


@_cached(ttl=300)
def get_census_tract_summary() -> dict:
    """Return high-level summary counts for the NMTC dashboard header."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT
            COUNT(*) as total_tracts,
            SUM(CASE WHEN is_nmtc_eligible = 1 THEN 1 ELSE 0 END) as eligible_tracts,
            SUM(CASE WHEN nmtc_eligibility_tier = 'Severely Distressed' THEN 1 ELSE 0 END) as severely_distressed,
            SUM(CASE WHEN nmtc_eligibility_tier = 'Deep Distress' THEN 1 ELSE 0 END) as deep_distress,
            SUM(total_population) as total_population_covered
        FROM census_tracts
    """)
    row = cur.fetchone()
    conn.close()
    return dict(row) if row else {}


@_cached(ttl=3600)   # state lists change rarely — cache for 1 hour
def get_census_tract_states() -> list:
    """Return sorted list of states that have census tract data."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT state FROM census_tracts WHERE state IS NOT NULL ORDER BY state")
    states = [row[0] for row in cur.fetchall()]
    conn.close()
    return states


# ---------------------------------------------------------------------------
# NMTC project queries
# ---------------------------------------------------------------------------

@_cached(ttl=300)
def get_nmtc_projects(
    states=None,
    census_tract_id=None,
    cde_name=None,
    project_type=None,
    min_year=None,
    max_year=None,
) -> pd.DataFrame:
    """
    Return NMTC projects matching the given filters as a DataFrame.

    Args:
        states: list of state abbreviations
        census_tract_id: exact census tract FIPS to filter to a single tract
        cde_name: substring match on CDE name
        project_type: 'Real Estate' or 'Non-Real Estate'
        min_year: minimum fiscal_year (inclusive)
        max_year: maximum fiscal_year (inclusive)
    """
    conditions = []
    params = []

    if states:
        placeholders = ",".join("?" * len(states))
        conditions.append(f"state IN ({placeholders})")
        params.extend(states)

    if census_tract_id:
        conditions.append("census_tract_id = ?")
        params.append(census_tract_id)

    if cde_name:
        conditions.append("cde_name LIKE ?")
        params.append(f"%{cde_name}%")

    if project_type:
        conditions.append("project_type = ?")
        params.append(project_type)

    if min_year is not None:
        conditions.append("fiscal_year >= ?")
        params.append(min_year)

    if max_year is not None:
        conditions.append("fiscal_year <= ?")
        params.append(max_year)

    where_clause = "WHERE " + " AND ".join(conditions) if conditions else ""
    query = f"""
        SELECT * FROM nmtc_projects
        {where_clause}
        ORDER BY state, fiscal_year DESC, qlici_amount DESC
    """

    conn = get_connection()
    df = pd.read_sql_query(query, conn, params=params)
    conn.close()
    return df


@_cached(ttl=300)
def get_nmtc_project_summary() -> dict:
    """Return high-level NMTC investment summary counts."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT
            COUNT(*) as total_projects,
            SUM(qlici_amount) as total_qlici,
            SUM(total_investment) as total_investment,
            SUM(jobs_created) as total_jobs_created,
            COUNT(DISTINCT cde_name) as unique_cdes,
            COUNT(DISTINCT state) as states_served
        FROM nmtc_projects
    """)
    row = cur.fetchone()
    conn.close()
    return dict(row) if row else {}


@_cached(ttl=300)
def get_cde_allocations(states=None) -> pd.DataFrame:
    """Return CDE allocation records, optionally filtered by state."""
    conditions = []
    params = []

    if states:
        placeholders = ",".join("?" * len(states))
        conditions.append(f"state IN ({placeholders})")
        params.extend(states)

    where_clause = "WHERE " + " AND ".join(conditions) if conditions else ""
    query = f"""
        SELECT * FROM cde_allocations
        {where_clause}
        ORDER BY state, allocation_amount DESC
    """

    conn = get_connection()
    df = pd.read_sql_query(query, conn, params=params)
    conn.close()
    return df


# ---------------------------------------------------------------------------
# FQHC queries
# ---------------------------------------------------------------------------

@_cached(ttl=300)
def get_fqhc(
    states=None,
    active_only=True,
    site_types=None,
) -> pd.DataFrame:
    """
    Return FQHC health center sites matching the given filters.

    Args:
        states: list of state abbreviations, e.g. ['CA', 'TX']
        active_only: if True (default), only return active sites (is_active=1)
        site_types: list of site type strings to include (e.g. ['Health Center'])
    """
    conditions = []
    params = []

    if active_only:
        conditions.append("is_active = 1")

    if states:
        placeholders = ",".join("?" * len(states))
        conditions.append(f"state IN ({placeholders})")
        params.extend(states)

    if site_types:
        placeholders = ",".join("?" * len(site_types))
        conditions.append(f"site_type IN ({placeholders})")
        params.extend(site_types)

    where_clause = "WHERE " + " AND ".join(conditions) if conditions else ""
    query = f"SELECT * FROM fqhc {where_clause} ORDER BY state, health_center_name"

    conn = get_connection()
    try:
        df = pd.read_sql_query(query, conn, params=params)
    except Exception:
        df = pd.DataFrame()
    conn.close()
    return df


@_cached(ttl=3600)   # state lists change rarely — cache for 1 hour
def get_fqhc_states() -> list:
    """Return sorted list of states that have FQHC data."""
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute("SELECT DISTINCT state FROM fqhc WHERE state IS NOT NULL ORDER BY state")
        states = [row[0] for row in cur.fetchall()]
    except Exception:
        states = []
    conn.close()
    return states


@_cached(ttl=300)
def get_fqhc_summary() -> dict:
    """Return high-level FQHC counts."""
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT
                COUNT(*) as total_sites,
                SUM(CASE WHEN is_active = 1 THEN 1 ELSE 0 END) as active_sites,
                COUNT(DISTINCT health_center_name) as unique_health_centers,
                COUNT(DISTINCT state) as states_served,
                SUM(total_patients) as total_patients
            FROM fqhc
        """)
        row = cur.fetchone()
        result = dict(row) if row else {}
    except Exception:
        result = {}
    conn.close()
    return result


# ---------------------------------------------------------------------------
# ECE center queries
# ---------------------------------------------------------------------------

@_cached(ttl=300)
def get_ece_centers(
    states=None,
    active_only=True,
    facility_types=None,
    accepts_subsidies=None,
    min_capacity=None,
) -> pd.DataFrame:
    """
    Return ECE centers matching the given filters.

    Args:
        states: list of state abbreviations, e.g. ['CA', 'TX']
        active_only: if True (default), only return active licensed facilities
        facility_types: list of facility type strings, e.g. ['Center']
        accepts_subsidies: True = subsidized care only, None = all
        min_capacity: minimum licensed capacity (integer)
    """
    conditions = []
    params = []

    if active_only:
        conditions.append("(license_status = 'Active' OR license_status IS NULL)")

    if states:
        placeholders = ",".join("?" * len(states))
        conditions.append(f"state IN ({placeholders})")
        params.extend(states)

    if facility_types:
        placeholders = ",".join("?" * len(facility_types))
        conditions.append(f"facility_type IN ({placeholders})")
        params.extend(facility_types)

    if accepts_subsidies is True:
        conditions.append("accepts_subsidies = 1")

    if min_capacity is not None:
        conditions.append("capacity >= ?")
        params.append(min_capacity)

    where_clause = "WHERE " + " AND ".join(conditions) if conditions else ""
    query = f"SELECT * FROM ece_centers {where_clause} ORDER BY state, provider_name"

    conn = get_connection()
    try:
        df = pd.read_sql_query(query, conn, params=params)
    except Exception:
        df = pd.DataFrame()
    conn.close()
    return df


@_cached(ttl=3600)   # state lists change rarely — cache for 1 hour
def get_ece_states() -> list:
    """Return sorted list of states that have ECE center data."""
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT DISTINCT state FROM ece_centers WHERE state IS NOT NULL ORDER BY state"
        )
        states = [row[0] for row in cur.fetchall()]
    except Exception:
        states = []
    conn.close()
    return states


@_cached(ttl=300)
def get_ece_summary() -> dict:
    """Return high-level ECE counts."""
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT
                COUNT(*) as total_centers,
                SUM(CASE WHEN license_status = 'Active' THEN 1 ELSE 0 END) as active_centers,
                SUM(capacity) as total_capacity,
                COUNT(DISTINCT state) as states_covered,
                SUM(CASE WHEN accepts_subsidies = 1 THEN 1 ELSE 0 END) as subsidized_centers
            FROM ece_centers
        """)
        row = cur.fetchone()
        result = dict(row) if row else {}
    except Exception:
        result = {}
    conn.close()
    return result


# ---------------------------------------------------------------------------
# Global search — across schools, NMTC projects, CDEs, FQHCs, ECE
# ---------------------------------------------------------------------------

@_cached(ttl=60)
def search_all(query_text: str) -> dict:
    """
    Search across schools, NMTC projects, CDEs, FQHCs, and ECE centers by name/city.
    Returns a dict with keys 'schools', 'projects', 'cdes', 'fqhc', 'ece' — each a DataFrame.
    """
    if not query_text or not query_text.strip():
        return {
            "schools": pd.DataFrame(),
            "projects": pd.DataFrame(),
            "cdes": pd.DataFrame(),
            "fqhc": pd.DataFrame(),
            "ece": pd.DataFrame(),
        }

    like = f"%{query_text.strip()}%"
    conn = get_connection()

    # Search schools
    school_table = "schools"
    try:
        schools_df = pd.read_sql_query(
            f"""SELECT * FROM {school_table}
                WHERE school_name LIKE ? OR city LIKE ? OR lea_name LIKE ?
                  OR nces_id LIKE ? OR state LIKE ?
                ORDER BY school_name LIMIT 200""",
            conn, params=[like, like, like, like, like],
        )
    except Exception:
        school_table = "charter_schools"
        schools_df = pd.read_sql_query(
            f"""SELECT * FROM {school_table}
                WHERE school_name LIKE ? OR city LIKE ? OR lea_name LIKE ?
                  OR nces_id LIKE ? OR state LIKE ?
                ORDER BY school_name LIMIT 200""",
            conn, params=[like, like, like, like, like],
        )

    # Search NMTC projects
    try:
        projects_df = pd.read_sql_query(
            """SELECT * FROM nmtc_projects
               WHERE project_name LIKE ? OR cde_name LIKE ? OR city LIKE ?
                 OR state LIKE ? OR census_tract_id LIKE ?
               ORDER BY project_name LIMIT 200""",
            conn, params=[like, like, like, like, like],
        )
    except Exception:
        projects_df = pd.DataFrame()

    # Search CDEs
    try:
        cdes_df = pd.read_sql_query(
            """SELECT * FROM cde_allocations
               WHERE cde_name LIKE ? OR city LIKE ? OR state LIKE ?
                 OR service_areas LIKE ?
               ORDER BY cde_name LIMIT 200""",
            conn, params=[like, like, like, like],
        )
    except Exception:
        cdes_df = pd.DataFrame()

    # Search FQHCs
    try:
        fqhc_df = pd.read_sql_query(
            """SELECT * FROM fqhc
               WHERE health_center_name LIKE ? OR site_name LIKE ?
                 OR city LIKE ? OR state LIKE ?
               ORDER BY health_center_name LIMIT 200""",
            conn, params=[like, like, like, like],
        )
    except Exception:
        fqhc_df = pd.DataFrame()

    # Search ECE centers
    try:
        ece_df = pd.read_sql_query(
            """SELECT * FROM ece_centers
               WHERE provider_name LIKE ? OR operator_name LIKE ?
                 OR city LIKE ? OR state LIKE ?
               ORDER BY provider_name LIMIT 200""",
            conn, params=[like, like, like, like],
        )
    except Exception:
        ece_df = pd.DataFrame()

    conn.close()
    return {
        "schools": schools_df,
        "projects": projects_df,
        "cdes": cdes_df,
        "fqhc": fqhc_df,
        "ece": ece_df,
    }


# ---------------------------------------------------------------------------
# Upsert functions (write — not cached)
# ---------------------------------------------------------------------------

def upsert_school(record: dict):
    """
    Insert or update a school record.
    Uses nces_id as the unique key — if a school with that ID exists,
    update it; otherwise insert a new row.
    """
    conn = get_connection()
    cur = conn.cursor()

    columns = list(record.keys())
    values = list(record.values())
    placeholders = ",".join("?" * len(values))
    update_clause = ",".join(f"{col}=excluded.{col}" for col in columns if col != "nces_id")

    # Try schools table first, fall back to charter_schools for old DBs
    for table in ["schools", "charter_schools"]:
        try:
            sql = f"""
                INSERT INTO {table} ({",".join(columns)})
                VALUES ({placeholders})
                ON CONFLICT(nces_id) DO UPDATE SET {update_clause}, updated_at=CURRENT_TIMESTAMP
            """
            cur.execute(sql, values)
            conn.commit()
            conn.close()
            return
        except Exception:
            continue
    conn.close()


def upsert_charter_school(record: dict):
    """Backward-compatible wrapper: inserts with is_charter=1."""
    record = dict(record)
    record["is_charter"] = 1
    upsert_school(record)


def upsert_nmtc_project(record: dict):
    """Insert or update an NMTC project record (keyed on cdfi_project_id)."""
    conn = get_connection()
    cur = conn.cursor()

    columns = list(record.keys())
    values = list(record.values())
    placeholders = ",".join("?" * len(values))
    update_clause = ",".join(
        f"{col}=excluded.{col}" for col in columns if col != "cdfi_project_id"
    )

    sql = f"""
        INSERT INTO nmtc_projects ({",".join(columns)})
        VALUES ({placeholders})
        ON CONFLICT(cdfi_project_id) DO UPDATE SET {update_clause}
    """
    cur.execute(sql, values)
    conn.commit()
    conn.close()


def upsert_cde_allocation(record: dict):
    """Insert or update a CDE allocation record (keyed on cde_name + allocation_year)."""
    conn = get_connection()
    cur = conn.cursor()

    columns = list(record.keys())
    values = list(record.values())
    placeholders = ",".join("?" * len(values))
    update_clause = ",".join(
        f"{col}=excluded.{col}" for col in columns if col not in ("cde_name", "allocation_year")
    )

    sql = f"""
        INSERT INTO cde_allocations ({",".join(columns)})
        VALUES ({placeholders})
        ON CONFLICT(cde_name, allocation_year) DO UPDATE SET {update_clause}
    """
    cur.execute(sql, values)
    conn.commit()
    conn.close()


# ---------------------------------------------------------------------------
# LEA accountability queries
# ---------------------------------------------------------------------------

@_cached(ttl=300)
def get_lea_accountability(lea_ids=None, states=None) -> pd.DataFrame:
    """Return LEA accountability data, optionally filtered."""
    conditions = []
    params = []

    if lea_ids:
        placeholders = ",".join("?" * len(lea_ids))
        conditions.append(f"lea_id IN ({placeholders})")
        params.extend(lea_ids)

    if states:
        placeholders = ",".join("?" * len(states))
        conditions.append(f"state IN ({placeholders})")
        params.extend(states)

    where_clause = "WHERE " + " AND ".join(conditions) if conditions else ""
    query = f"SELECT * FROM lea_accountability {where_clause} ORDER BY state, lea_name"

    conn = get_connection()
    df = pd.read_sql_query(query, conn, params=params)
    conn.close()
    return df


def upsert_lea_accountability(record: dict):
    """Insert or update an LEA accountability record (keyed on lea_id + data_year)."""
    conn = get_connection()
    cur = conn.cursor()

    columns = list(record.keys())
    values = list(record.values())
    placeholders = ",".join("?" * len(values))
    update_clause = ",".join(
        f"{col}=excluded.{col}" for col in columns if col not in ("lea_id", "data_year")
    )

    sql = f"""
        INSERT INTO lea_accountability ({",".join(columns)})
        VALUES ({placeholders})
        ON CONFLICT(lea_id, data_year) DO UPDATE SET {update_clause}
    """
    cur.execute(sql, values)
    conn.commit()
    conn.close()


def upsert_census_tract(record: dict):
    """Insert or update a census tract record (keyed on census_tract_id)."""
    conn = get_connection()
    cur = conn.cursor()

    columns = list(record.keys())
    values = list(record.values())
    placeholders = ",".join("?" * len(values))
    update_clause = ",".join(
        f"{col}=excluded.{col}" for col in columns if col != "census_tract_id"
    )

    sql = f"""
        INSERT INTO census_tracts ({",".join(columns)})
        VALUES ({placeholders})
        ON CONFLICT(census_tract_id) DO UPDATE SET {update_clause}
    """
    cur.execute(sql, values)
    conn.commit()
    conn.close()


def upsert_ece(record: dict):
    """Insert or update an ECE center record (keyed on license_id)."""
    conn = get_connection()
    cur = conn.cursor()

    columns = list(record.keys())
    values = list(record.values())
    placeholders = ",".join("?" * len(values))
    update_clause = ",".join(
        f"{col}=excluded.{col}" for col in columns if col != "license_id"
    )

    sql = f"""
        INSERT INTO ece_centers ({",".join(columns)})
        VALUES ({placeholders})
        ON CONFLICT(license_id) DO UPDATE SET {update_clause}
    """
    cur.execute(sql, values)
    conn.commit()
    conn.close()


def upsert_fqhc(record: dict):
    """Insert or update a FQHC site record (keyed on bhcmis_id)."""
    conn = get_connection()
    cur = conn.cursor()

    columns = list(record.keys())
    values = list(record.values())
    placeholders = ",".join("?" * len(values))
    update_clause = ",".join(
        f"{col}=excluded.{col}" for col in columns if col != "bhcmis_id"
    )

    sql = f"""
        INSERT INTO fqhc ({",".join(columns)})
        VALUES ({placeholders})
        ON CONFLICT(bhcmis_id) DO UPDATE SET {update_clause}
    """
    cur.execute(sql, values)
    conn.commit()
    conn.close()


def get_fqhc_by_id(bhcmis_id: str) -> dict:
    """Return a single FQHC site by its bhcmis_id. Returns empty dict if not found."""
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute("SELECT * FROM fqhc WHERE bhcmis_id = ?", (bhcmis_id,))
        row = cur.fetchone()
        conn.close()
        return dict(row) if row else {}
    except Exception:
        conn.close()
        return {}


def get_ece_by_id(license_id: str) -> dict:
    """Return a single ECE center by its license_id. Returns empty dict if not found."""
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute("SELECT * FROM ece_centers WHERE license_id = ?", (license_id,))
        row = cur.fetchone()
        conn.close()
        return dict(row) if row else {}
    except Exception:
        conn.close()
        return {}


def get_nmtc_project_by_id(cdfi_project_id: str) -> dict:
    """Return a single NMTC project by its cdfi_project_id. Returns empty dict if not found."""
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute("SELECT * FROM nmtc_projects WHERE cdfi_project_id = ?", (cdfi_project_id,))
        row = cur.fetchone()
        conn.close()
        return dict(row) if row else {}
    except Exception:
        conn.close()
        return {}


def get_nmtc_projects_by_cde(cde_name: str) -> pd.DataFrame:
    """Return all NMTC projects for a given CDE, most recent first."""
    conn = get_connection()
    try:
        df = pd.read_sql_query(
            "SELECT * FROM nmtc_projects WHERE cde_name = ? ORDER BY fiscal_year DESC",
            conn, params=[cde_name],
        )
    except Exception:
        df = pd.DataFrame()
    conn.close()
    return df


def get_nearby_facilities(lat: float, lon: float, radius_miles: float = 1.0) -> dict:
    """
    Return all facility types within radius_miles of the given coordinates.

    Uses filter_by_radius() from utils/geo.py which calculates haversine distance.
    Pulls all states (no geographic filter) since we want everything nearby regardless of state.

    Returns a dict with keys: 'schools', 'fqhc', 'ece', 'nmtc'
    Each value is a DataFrame with a 'distance_miles' column added.
    Returns empty DataFrames if any table is missing or has no data.
    """
    # Import here to avoid circular imports (geo.py doesn't import db.py, so this is safe)
    from utils.geo import filter_by_radius

    results = {"schools": pd.DataFrame(), "fqhc": pd.DataFrame(), "ece": pd.DataFrame(), "nmtc": pd.DataFrame()}

    if lat is None or lon is None:
        return results

    # Schools — pull all, then filter by radius
    try:
        schools = get_schools(active_only=False)
        if not schools.empty:
            results["schools"] = filter_by_radius(schools, lat, lon, radius_miles)
    except Exception:
        pass

    # FQHCs
    try:
        fqhc = get_fqhc(active_only=False)
        if not fqhc.empty:
            results["fqhc"] = filter_by_radius(fqhc, lat, lon, radius_miles)
    except Exception:
        pass

    # ECE centers
    try:
        ece = get_ece_centers(active_only=False)
        if not ece.empty:
            results["ece"] = filter_by_radius(ece, lat, lon, radius_miles)
    except Exception:
        pass

    # NMTC projects
    try:
        nmtc = get_nmtc_projects()
        if not nmtc.empty:
            results["nmtc"] = filter_by_radius(nmtc, lat, lon, radius_miles)
    except Exception:
        pass

    return results


def update_school_census_tract(nces_id: str, census_tract_id: str):
    """Update the census_tract_id for a single school by nces_id."""
    conn = get_connection()
    cur = conn.cursor()
    for table in ["schools", "charter_schools"]:
        try:
            cur.execute(
                f"UPDATE {table} SET census_tract_id = ?, updated_at = CURRENT_TIMESTAMP WHERE nces_id = ?",
                (census_tract_id, nces_id),
            )
            conn.commit()
            conn.close()
            return
        except Exception:
            continue
    conn.close()


# ---------------------------------------------------------------------------
# IRS 990 queries
# ---------------------------------------------------------------------------

def upsert_990(record: dict):
    """Insert or update a 990 record (keyed on ein)."""
    conn = get_connection()
    cur = conn.cursor()
    columns = list(record.keys())
    values = list(record.values())
    placeholders = ",".join("?" * len(values))
    update_clause = ",".join(
        f"{col}=excluded.{col}" for col in columns if col != "ein"
    )
    cur.execute(
        f"INSERT INTO irs_990 ({','.join(columns)}) VALUES ({placeholders}) "
        f"ON CONFLICT(ein) DO UPDATE SET {update_clause}, updated_at=CURRENT_TIMESTAMP",
        values,
    )
    conn.commit()
    conn.close()


def get_990_by_ein(ein: str) -> dict:
    """Look up a single 990 record by EIN. Returns a dict or empty dict."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT * FROM irs_990 WHERE ein = ?", (ein,))
    row = cur.fetchone()
    conn.close()
    if not row:
        return {}
    cols = [d[0] for d in cur.description]
    return dict(zip(cols, row))


def get_990_for_school(nces_id: str) -> dict:
    """
    Return the 990 record linked to a school via its ein column.
    Returns empty dict if the school has no EIN or no 990 record.
    """
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT ein FROM schools WHERE nces_id = ?", (nces_id,))
    row = cur.fetchone()
    conn.close()
    if not row or not row[0]:
        return {}
    return get_990_by_ein(row[0])


def get_990_for_fqhc(bhcmis_id: str) -> dict:
    """
    Return the 990 record linked to a FQHC site via its ein column.
    Returns empty dict if the site has no EIN or no 990 record.
    """
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT ein FROM fqhc WHERE bhcmis_id = ?", (bhcmis_id,))
    row = cur.fetchone()
    conn.close()
    if not row or not row[0]:
        return {}
    return get_990_by_ein(row[0])


def link_ein_to_school(nces_id: str, ein: str):
    """Store an EIN on a school record so it can be joined to irs_990."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        "UPDATE schools SET ein = ?, updated_at = CURRENT_TIMESTAMP WHERE nces_id = ?",
        (ein, nces_id),
    )
    conn.commit()
    conn.close()


def link_ein_to_fqhc(bhcmis_id: str, ein: str):
    """Store an EIN on a FQHC record so it can be joined to irs_990."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("UPDATE fqhc SET ein = ? WHERE bhcmis_id = ?", (ein, bhcmis_id))
    conn.commit()
    conn.close()


def get_990_summary() -> dict:
    """Return aggregate counts for the admin/about panel."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM irs_990")
    total = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM schools WHERE ein IS NOT NULL AND is_charter = 1")
    linked_schools = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM fqhc WHERE ein IS NOT NULL")
    linked_fqhc = cur.fetchone()[0]
    conn.close()
    return {
        "total_990_records": total,
        "linked_charter_schools": linked_schools,
        "linked_fqhc_sites": linked_fqhc,
    }


# ---------------------------------------------------------------------------
# NMTC peer comps
# ---------------------------------------------------------------------------

@_cached(ttl=300)
def get_peer_nmtc_projects(
    project_type: str,
    state: str,
    qlici_min: float,
    qlici_max: float,
    exclude_id: str = None,
) -> pd.DataFrame:
    """
    Return similar NMTC projects for deal comparison.

    Filters by project_type and state, within a QLICI dollar range (±50% of
    the project's QLICI amount), sorted by closest QLICI amount.

    Args:
        project_type: 'Real Estate' or 'Non-Real Estate'
        state: two-letter state abbreviation
        qlici_min: lower bound of QLICI comparison range
        qlici_max: upper bound of QLICI comparison range
        exclude_id: cdfi_project_id to exclude (the current project itself)
    """
    conditions = []
    params = []

    if project_type:
        conditions.append("project_type = ?")
        params.append(project_type)

    if state:
        conditions.append("state = ?")
        params.append(state)

    if qlici_min is not None:
        conditions.append("qlici_amount >= ?")
        params.append(qlici_min)

    if qlici_max is not None:
        conditions.append("qlici_amount <= ?")
        params.append(qlici_max)

    if exclude_id:
        conditions.append("cdfi_project_id != ?")
        params.append(exclude_id)

    where_clause = "WHERE " + " AND ".join(conditions) if conditions else ""
    midpoint = ((qlici_min or 0) + (qlici_max or 0)) / 2
    # Midpoint goes at end of params (used in ORDER BY, not WHERE)
    params.append(midpoint)

    query = f"""
        SELECT * FROM nmtc_projects
        {where_clause}
        ORDER BY ABS(qlici_amount - ?) ASC
        LIMIT 10
    """

    conn = get_connection()
    try:
        df = pd.read_sql_query(query, conn, params=params)
    except Exception:
        df = pd.DataFrame()
    conn.close()
    return df


# ---------------------------------------------------------------------------
# Operator profile queries
# ---------------------------------------------------------------------------

@_cached(ttl=300)
def get_operator_schools(ein: str) -> pd.DataFrame:
    """Return all schools operated by the organization with the given EIN."""
    conn = get_connection()
    try:
        df = pd.read_sql_query(
            "SELECT * FROM schools WHERE ein = ? ORDER BY school_name",
            conn, params=[ein],
        )
    except Exception:
        df = pd.DataFrame()
    conn.close()
    return df


@_cached(ttl=300)
def get_operator_fqhc(ein: str) -> pd.DataFrame:
    """Return all FQHC sites operated by the organization with the given EIN."""
    conn = get_connection()
    try:
        df = pd.read_sql_query(
            "SELECT * FROM fqhc WHERE ein = ? ORDER BY site_name",
            conn, params=[ein],
        )
    except Exception:
        df = pd.DataFrame()
    conn.close()
    return df


# ---------------------------------------------------------------------------
# IRS 990 multi-year history
# ---------------------------------------------------------------------------

def upsert_990_history(record: dict):
    """Insert or update a 990 history record (keyed on ein + tax_year)."""
    conn = get_connection()
    cur = conn.cursor()
    columns = list(record.keys())
    values = list(record.values())
    placeholders = ",".join("?" * len(values))
    update_clause = ",".join(
        f"{col}=excluded.{col}" for col in columns if col not in ("ein", "tax_year")
    )
    cur.execute(
        f"INSERT INTO irs_990_history ({','.join(columns)}) VALUES ({placeholders}) "
        f"ON CONFLICT(ein, tax_year) DO UPDATE SET {update_clause}",
        values,
    )
    conn.commit()
    conn.close()


@_cached(ttl=300)
def get_990_history(ein: str) -> pd.DataFrame:
    """
    Return all 990 filings for an organization, sorted by tax_year descending.
    Used for trend charts in operator profile views.
    Falls back to the main irs_990 table if no history records exist yet.
    """
    conn = get_connection()
    try:
        df = pd.read_sql_query(
            "SELECT * FROM irs_990_history WHERE ein = ? ORDER BY tax_year ASC",
            conn, params=[ein],
        )
    except Exception:
        df = pd.DataFrame()
    conn.close()

    # Fall back to single-year record if no history has been loaded yet
    if df.empty:
        single = get_990_by_ein(ein)
        if single:
            df = pd.DataFrame([single])
    return df


# ---------------------------------------------------------------------------
# CDFI directory
# ---------------------------------------------------------------------------

def upsert_cdfi(record: dict):
    """Insert or update a CDFI directory record (keyed on cdfi_name + state)."""
    conn = get_connection()
    cur = conn.cursor()
    columns = list(record.keys())
    values = list(record.values())
    placeholders = ",".join("?" * len(values))
    update_clause = ",".join(
        f"{col}=excluded.{col}" for col in columns if col not in ("cdfi_name", "state")
    )
    cur.execute(
        f"INSERT INTO cdfi_directory ({','.join(columns)}) VALUES ({placeholders}) "
        f"ON CONFLICT(cdfi_name, state) DO UPDATE SET {update_clause}",
        values,
    )
    conn.commit()
    conn.close()


@_cached(ttl=3600)
def get_cdfis(states=None, cdfi_type=None) -> pd.DataFrame:
    """Return CDFI directory entries, optionally filtered by state or type."""
    conditions = []
    params = []

    if states:
        placeholders = ",".join("?" * len(states))
        conditions.append(f"state IN ({placeholders})")
        params.extend(states)

    if cdfi_type:
        conditions.append("cdfi_type = ?")
        params.append(cdfi_type)

    where_clause = "WHERE " + " AND ".join(conditions) if conditions else ""
    query = f"SELECT * FROM cdfi_directory {where_clause} ORDER BY state, cdfi_name"

    conn = get_connection()
    try:
        df = pd.read_sql_query(query, conn, params=params)
    except Exception:
        df = pd.DataFrame()
    conn.close()
    return df


@_cached(ttl=3600)
def get_cdfi_states() -> list:
    """Return sorted list of states that have CDFI directory data."""
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT DISTINCT state FROM cdfi_directory WHERE state IS NOT NULL ORDER BY state"
        )
        states = [row[0] for row in cur.fetchall()]
    except Exception:
        states = []
    conn.close()
    return states


# ---------------------------------------------------------------------------
# State incentive programs
# ---------------------------------------------------------------------------

def upsert_state_program(record: dict):
    """Insert a state incentive program record. Ignores duplicates."""
    conn = get_connection()
    cur = conn.cursor()
    columns = list(record.keys())
    values = list(record.values())
    placeholders = ",".join("?" * len(values))
    cur.execute(
        f"INSERT OR IGNORE INTO state_programs ({','.join(columns)}) VALUES ({placeholders})",
        values,
    )
    conn.commit()
    conn.close()


@_cached(ttl=3600)
def get_state_programs(state: str = None) -> pd.DataFrame:
    """Return state incentive programs, optionally filtered to a single state."""
    conditions = []
    params = []

    if state:
        conditions.append("state = ?")
        params.append(state)

    where_clause = "WHERE " + " AND ".join(conditions) if conditions else ""
    query = (
        f"SELECT * FROM state_programs {where_clause} "
        f"ORDER BY state, program_type, program_name"
    )

    conn = get_connection()
    try:
        df = pd.read_sql_query(query, conn, params=params)
    except Exception:
        df = pd.DataFrame()
    conn.close()
    return df


@_cached(ttl=3600)
def get_program_states() -> list:
    """Return sorted list of states that have incentive program data."""
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT DISTINCT state FROM state_programs WHERE state IS NOT NULL ORDER BY state"
        )
        states = [row[0] for row in cur.fetchall()]
    except Exception:
        states = []
    conn.close()
    return states


# ---------------------------------------------------------------------------
# Service gap analysis
# ---------------------------------------------------------------------------

@_cached(ttl=300)
def get_service_gaps(
    states=None,
    asset_class: str = "ece",
    min_poverty_rate: float = 20.0,
    top_n: int = 50,
) -> pd.DataFrame:
    """
    Find census tracts with high population need but no facilities of the given type.

    A simplified gap analysis: counts facilities assigned to each census tract
    (via census_tract_id). Tracts with zero facilities and high poverty are gaps.

    Tracts are ranked by a "need score" = total_population × poverty_rate,
    so the highest-population, highest-poverty tracts come first.

    Args:
        states: list of state abbreviations to include (None = all)
        asset_class: 'ece', 'fqhc', or 'schools'
        min_poverty_rate: minimum poverty rate % to be considered high-need
        top_n: number of gap tracts to return (default 50)
    """
    # Map asset class to its database table
    asset_tables = {
        "ece": "ece_centers",
        "fqhc": "fqhc",
        "schools": "schools",
    }
    if asset_class not in asset_tables:
        return pd.DataFrame()

    facility_table = asset_tables[asset_class]

    conditions = [f"ct.poverty_rate >= {min_poverty_rate}"]
    params = []

    if states:
        placeholders = ",".join("?" * len(states))
        conditions.append(f"ct.state IN ({placeholders})")
        params.extend(states)

    where_clause = "WHERE " + " AND ".join(conditions)

    query = f"""
        SELECT
            ct.census_tract_id,
            ct.state,
            ct.county_name,
            ct.tract_name,
            ct.total_population,
            ct.poverty_rate,
            ct.median_household_income,
            ct.nmtc_eligibility_tier,
            ct.is_opportunity_zone,
            COALESCE(fac.facility_count, 0) AS facility_count,
            -- Need score: population × poverty_rate (higher = more urgent gap)
            ct.total_population * (ct.poverty_rate / 100.0) AS need_score
        FROM census_tracts ct
        LEFT JOIN (
            SELECT census_tract_id, COUNT(*) AS facility_count
            FROM {facility_table}
            GROUP BY census_tract_id
        ) fac ON fac.census_tract_id = ct.census_tract_id
        {where_clause}
          AND COALESCE(fac.facility_count, 0) = 0
          AND ct.total_population > 0
        ORDER BY need_score DESC
        LIMIT {top_n}
    """

    conn = get_connection()
    try:
        df = pd.read_sql_query(query, conn, params=params)
    except Exception:
        df = pd.DataFrame()
    conn.close()
    return df


# ---------------------------------------------------------------------------
# Enrollment history
# ---------------------------------------------------------------------------

def upsert_enrollment_history(record: dict):
    """Insert or update an enrollment history record (keyed on nces_id + school_year)."""
    conn = get_connection()
    cur = conn.cursor()
    columns = list(record.keys())
    values = list(record.values())
    placeholders = ",".join("?" * len(values))
    update_clause = ",".join(
        f"{col}=excluded.{col}" for col in columns if col not in ("nces_id", "school_year")
    )
    cur.execute(
        f"INSERT INTO enrollment_history ({','.join(columns)}) VALUES ({placeholders}) "
        f"ON CONFLICT(nces_id, school_year) DO UPDATE SET {update_clause}",
        values,
    )
    conn.commit()
    conn.close()


@_cached(ttl=300)
def get_enrollment_history(nces_id: str) -> pd.DataFrame:
    """Return enrollment history for a school, sorted by year ascending."""
    conn = get_connection()
    try:
        df = pd.read_sql_query(
            "SELECT * FROM enrollment_history WHERE nces_id = ? ORDER BY school_year ASC",
            conn, params=[nces_id],
        )
    except Exception:
        df = pd.DataFrame()
    conn.close()
    return df


# ---------------------------------------------------------------------------
# CDFI awards
# ---------------------------------------------------------------------------

def upsert_cdfi_award(record: dict):
    """Insert or update a CDFI award record (keyed on award_year + program + awardee_name)."""
    conn = get_connection()
    cur = conn.cursor()
    columns = list(record.keys())
    values = list(record.values())
    placeholders = ",".join("?" * len(values))
    update_clause = ",".join(
        f"{col}=excluded.{col}"
        for col in columns
        if col not in ("award_year", "program", "awardee_name")
    )
    cur.execute(
        f"INSERT INTO cdfi_awards ({','.join(columns)}) VALUES ({placeholders}) "
        f"ON CONFLICT(award_year, program, awardee_name) DO UPDATE SET {update_clause}",
        values,
    )
    conn.commit()
    conn.close()


@_cached(ttl=3600)
def get_cdfi_awards(states=None, programs=None, min_year=None) -> pd.DataFrame:
    """Return CDFI award records, filterable by state, program type, and year."""
    conditions = []
    params = []

    if states:
        placeholders = ",".join("?" * len(states))
        conditions.append(f"awardee_state IN ({placeholders})")
        params.extend(states)

    if programs:
        placeholders = ",".join("?" * len(programs))
        conditions.append(f"program IN ({placeholders})")
        params.extend(programs)

    if min_year is not None:
        conditions.append("award_year >= ?")
        params.append(min_year)

    where_clause = "WHERE " + " AND ".join(conditions) if conditions else ""
    query = (
        f"SELECT * FROM cdfi_awards {where_clause} "
        f"ORDER BY awardee_state, award_year DESC, award_amount DESC"
    )

    conn = get_connection()
    try:
        df = pd.read_sql_query(query, conn, params=params)
    except Exception:
        df = pd.DataFrame()
    conn.close()
    return df


@_cached(ttl=3600)
def get_cdfi_award_states() -> list:
    """Return sorted list of states with CDFI award data."""
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT DISTINCT awardee_state FROM cdfi_awards "
            "WHERE awardee_state IS NOT NULL ORDER BY awardee_state"
        )
        states = [row[0] for row in cur.fetchall()]
    except Exception:
        states = []
    conn.close()
    return states


# ---------------------------------------------------------------------------
# User notes
# ---------------------------------------------------------------------------

def get_user_notes(entity_type: str, entity_id: str) -> list:
    """Return all notes for a specific entity, newest first."""
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT * FROM user_notes WHERE entity_type = ? AND entity_id = ? "
            "ORDER BY updated_at DESC",
            (entity_type, str(entity_id)),
        )
        notes = [dict(row) for row in cur.fetchall()]
    except Exception:
        notes = []
    conn.close()
    return notes


def save_user_note(entity_type: str, entity_id: str, note_text: str) -> int:
    """Insert a new note. Returns the new note id."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO user_notes (entity_type, entity_id, note_text) VALUES (?, ?, ?)",
        (entity_type, str(entity_id), note_text),
    )
    note_id = cur.lastrowid
    conn.commit()
    conn.close()
    return note_id


def update_user_note(note_id: int, note_text: str):
    """Update the text of an existing note."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        "UPDATE user_notes SET note_text = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
        (note_text, note_id),
    )
    conn.commit()
    conn.close()


def delete_user_note(note_id: int):
    """Delete a note by its id."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("DELETE FROM user_notes WHERE id = ?", (note_id,))
    conn.commit()
    conn.close()


# ---------------------------------------------------------------------------
# Bookmarks
# ---------------------------------------------------------------------------

def get_bookmarks() -> list:
    """Return all bookmarks, newest first."""
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute("SELECT * FROM user_bookmarks ORDER BY created_at DESC")
        bookmarks = [dict(row) for row in cur.fetchall()]
    except Exception:
        bookmarks = []
    conn.close()
    return bookmarks


def save_bookmark(entity_type: str, entity_id: str, label: str):
    """Save a bookmark. Ignores duplicates (UNIQUE on entity_type + entity_id)."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        "INSERT OR IGNORE INTO user_bookmarks (entity_type, entity_id, label) VALUES (?, ?, ?)",
        (entity_type, str(entity_id), label),
    )
    conn.commit()
    conn.close()


def delete_bookmark(entity_type: str, entity_id: str):
    """Remove a bookmark by entity type + id."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        "DELETE FROM user_bookmarks WHERE entity_type = ? AND entity_id = ?",
        (entity_type, str(entity_id)),
    )
    conn.commit()
    conn.close()


def is_bookmarked(entity_type: str, entity_id: str) -> bool:
    """Return True if the entity has been bookmarked."""
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT 1 FROM user_bookmarks WHERE entity_type = ? AND entity_id = ?",
            (entity_type, str(entity_id)),
        )
        found = cur.fetchone() is not None
    except Exception:
        found = False
    conn.close()
    return found


# ---------------------------------------------------------------------------
# Documents
# ---------------------------------------------------------------------------

def save_document(record: dict) -> int:
    """Insert a document record. Returns the new document id."""
    conn = get_connection()
    cur = conn.cursor()
    columns = list(record.keys())
    values = list(record.values())
    placeholders = ",".join("?" * len(values))
    cur.execute(
        f"INSERT INTO documents ({','.join(columns)}) VALUES ({placeholders})",
        values,
    )
    doc_id = cur.lastrowid
    conn.commit()
    conn.close()
    return doc_id


def get_documents(ein: str = None, entity_type: str = None, entity_id: str = None) -> pd.DataFrame:
    """Return documents, filtered by EIN and/or entity."""
    conditions = []
    params = []

    if ein:
        conditions.append("ein = ?")
        params.append(ein)
    if entity_type:
        conditions.append("entity_type = ?")
        params.append(entity_type)
    if entity_id:
        conditions.append("entity_id = ?")
        params.append(str(entity_id))

    where_clause = "WHERE " + " AND ".join(conditions) if conditions else ""
    query = f"SELECT * FROM documents {where_clause} ORDER BY upload_date DESC"

    conn = get_connection()
    try:
        df = pd.read_sql_query(query, conn, params=params)
    except Exception:
        df = pd.DataFrame()
    conn.close()
    return df


def update_document_data(doc_id: int, extracted_data: str, verified: bool = False):
    """Store extracted financial data (JSON string) and optionally mark as verified."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        "UPDATE documents SET extracted_data = ?, verified = ? WHERE id = ?",
        (extracted_data, 1 if verified else 0, doc_id),
    )
    conn.commit()
    conn.close()


def delete_document(doc_id: int) -> str:
    """Delete a document record and return its filepath so the caller can remove the file."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT filepath FROM documents WHERE id = ?", (doc_id,))
    row = cur.fetchone()
    filepath = dict(row)["filepath"] if row else None
    cur.execute("DELETE FROM documents WHERE id = ?", (doc_id,))
    conn.commit()
    conn.close()
    return filepath


# ---------------------------------------------------------------------------
# Financial ratios
# ---------------------------------------------------------------------------

def upsert_financial_ratios(record: dict):
    """Insert or update financial ratio record (keyed on ein + fiscal_year)."""
    conn = get_connection()
    cur = conn.cursor()
    columns = list(record.keys())
    values = list(record.values())
    placeholders = ",".join("?" * len(values))
    update_clause = ",".join(
        f"{col}=excluded.{col}" for col in columns if col not in ("ein", "fiscal_year")
    )
    cur.execute(
        f"INSERT INTO financial_ratios ({','.join(columns)}) VALUES ({placeholders}) "
        f"ON CONFLICT(ein, fiscal_year) DO UPDATE SET {update_clause}, "
        f"calculated_at=CURRENT_TIMESTAMP",
        values,
    )
    conn.commit()
    conn.close()


@_cached(ttl=300)
def get_financial_ratios(ein: str) -> pd.DataFrame:
    """Return financial ratio history for an organization, newest first."""
    conn = get_connection()
    try:
        df = pd.read_sql_query(
            "SELECT * FROM financial_ratios WHERE ein = ? ORDER BY fiscal_year DESC",
            conn, params=[ein],
        )
    except Exception:
        df = pd.DataFrame()
    conn.close()
    return df


def get_latest_financial_ratios(ein: str) -> dict:
    """Return only the most recent ratio row for an organization."""
    df = get_financial_ratios(ein)
    if df.empty:
        return {}
    return df.iloc[0].to_dict()


def compute_and_store_ratios(ein: str):
    """
    Compute financial ratios from 990 history and store them.

    For each year in irs_990_history:
      - acid_ratio_990: cash_savings / (accounts_payable + accrued_expenses)
        Labeled approximate because 990 doesn't isolate current liabilities.
      - leverage_ratio: unrestricted_net_assets / total_liabilities
      - avg_operating_cash_flow: 3-year rolling average of net_income
        (net_income is the 990 approximation of operating cash flow)

    Audit-based values (acid_ratio_audit, current_liabilities_audit) are preserved
    if they were already set from an uploaded audit document.
    """
    history = get_990_history(ein)
    if history.empty:
        return

    rows = history.sort_values("tax_year", ascending=False).to_dict("records")

    for i, row in enumerate(rows):
        fiscal_year = row.get("tax_year")
        if not fiscal_year:
            continue

        cash       = row.get("cash_savings")
        ap         = row.get("accounts_payable") or 0
        accrued    = row.get("accrued_expenses") or 0
        unrest_na  = row.get("unrestricted_net_assets")
        total_liab = row.get("total_liabilities")
        net_income = row.get("net_income")

        acid_990 = None
        if cash is not None and (ap + accrued) > 0:
            acid_990 = round(cash / (ap + accrued), 3)

        leverage = None
        if unrest_na is not None and total_liab and total_liab > 0:
            leverage = round(unrest_na / total_liab, 3)

        # 3-year rolling average using up to 3 consecutive years from current row
        cf_years = [
            r.get("net_income")
            for r in rows[i:i+3]
            if r.get("net_income") is not None
        ]
        avg_cf = round(sum(cf_years) / len(cf_years), 0) if cf_years else None

        # Preserve existing audit-based acid ratio if it was previously set
        existing = {}
        try:
            existing = get_financial_ratios(ein)
            if not existing.empty:
                match = existing[existing["fiscal_year"] == fiscal_year]
                existing = match.iloc[0].to_dict() if not match.empty else {}
        except Exception:
            pass
        acid_audit = existing.get("acid_ratio_audit")
        cl_audit   = existing.get("current_liabilities_audit")
        has_audit  = existing.get("has_audit_data", 0)

        record = {
            "ein":                       ein,
            "fiscal_year":               fiscal_year,
            "acid_ratio_990":            acid_990,
            "acid_ratio_audit":          acid_audit,
            "leverage_ratio":            leverage,
            "avg_operating_cash_flow":   avg_cf,
            "cash_and_equivalents":      cash,
            "accounts_payable":          ap or None,
            "accrued_expenses":          accrued or None,
            "current_liabilities_audit": cl_audit,
            "unrestricted_net_assets":   unrest_na,
            "total_liabilities":         total_liab,
            "total_debt":                row.get("notes_payable"),
            "has_audit_data":            has_audit,
            "data_source":               "Audit" if has_audit else "990",
        }
        upsert_financial_ratios(record)


# ---------------------------------------------------------------------------
# Org search — by name or EIN across 990 records
# ---------------------------------------------------------------------------

@_cached(ttl=60)
def search_org(query_text: str) -> pd.DataFrame:
    """
    Search irs_990 by org name or EIN.
    Returns matching orgs with their most recent financial data.
    """
    if not query_text or not query_text.strip():
        return pd.DataFrame()

    like = f"%{query_text.strip()}%"
    conn = get_connection()
    try:
        df = pd.read_sql_query(
            """SELECT * FROM irs_990
               WHERE org_name LIKE ? OR ein LIKE ?
               ORDER BY org_name
               LIMIT 50""",
            conn, params=[like, like],
        )
    except Exception:
        df = pd.DataFrame()
    conn.close()
    return df
