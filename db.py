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

    # Add ein column to schools and fqhc tables if it doesn't exist yet.
    # ALTER TABLE only runs on existing DBs — new DBs get the column from init.
    # We use try/except because SQLite has no "ADD COLUMN IF NOT EXISTS".
    for table, col in [("schools", "ein"), ("fqhc", "ein")]:
        try:
            cur.execute(f"ALTER TABLE {table} ADD COLUMN {col} TEXT")
        except Exception:
            pass  # column already exists

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
) -> pd.DataFrame:
    """
    Return schools matching the given filters as a DataFrame.
    All parameters are optional — omitting them returns all schools.

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

    where_clause = "WHERE " + " AND ".join(conditions) if conditions else ""

    # Try to query from the 'schools' table; fall back to 'charter_schools' for old DBs.
    #
    # The LEA join uses a CTE (WITH latest_lea ...) instead of a correlated subquery.
    # A correlated subquery runs once per school row; the CTE runs once and is reused,
    # making it O(schools + districts) instead of O(schools * districts).
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
                    la.proficiency_math
                FROM {table_name} {t}
                LEFT JOIN latest_lea ll
                    ON {t}.lea_id = ll.lea_id
                LEFT JOIN lea_accountability la
                    ON la.lea_id = ll.lea_id
                    AND la.data_year = ll.max_year
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
