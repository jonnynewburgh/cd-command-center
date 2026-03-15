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

    # Charter schools — one row per school site
    cur.execute("""
        CREATE TABLE IF NOT EXISTS charter_schools (
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
            -- Survival model output
            survival_score REAL,           -- 0–1 probability of remaining open
            survival_risk_tier TEXT,       -- 'Low', 'Medium', 'High'
            -- Metadata
            data_year INTEGER,             -- School year the data represents
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # LEA (district) accountability scores
    cur.execute("""
        CREATE TABLE IF NOT EXISTS lea_accountability (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            lea_id TEXT,                   -- NCES LEA ID (joins to charter_schools.lea_id)
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

    # Add nmtc_eligibility_tier to existing census_tracts tables that predate this column
    try:
        cur.execute("ALTER TABLE census_tracts ADD COLUMN median_family_income REAL")
    except Exception:
        pass  # Column already exists
    try:
        cur.execute("ALTER TABLE census_tracts ADD COLUMN nmtc_eligibility_tier TEXT")
    except Exception:
        pass  # Column already exists

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

    conn.commit()
    conn.close()


# ---------------------------------------------------------------------------
# Charter School queries
# ---------------------------------------------------------------------------

def get_charter_schools(
    states=None,
    min_enrollment=None,
    max_enrollment=None,
    risk_tiers=None,
    min_survival_score=None,
    max_survival_score=None,
    school_status=None,
    county=None,
    census_tract_id=None,
) -> pd.DataFrame:
    """
    Return charter schools matching the given filters as a DataFrame.
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
    """
    conditions = []
    params = []

    if states:
        placeholders = ",".join("?" * len(states))
        conditions.append(f"cs.state IN ({placeholders})")
        params.extend(states)

    if min_enrollment is not None:
        conditions.append("cs.enrollment >= ?")
        params.append(min_enrollment)

    if max_enrollment is not None:
        conditions.append("cs.enrollment <= ?")
        params.append(max_enrollment)

    if risk_tiers:
        placeholders = ",".join("?" * len(risk_tiers))
        conditions.append(f"cs.survival_risk_tier IN ({placeholders})")
        params.extend(risk_tiers)

    if min_survival_score is not None:
        conditions.append("cs.survival_score >= ?")
        params.append(min_survival_score)

    if max_survival_score is not None:
        conditions.append("cs.survival_score <= ?")
        params.append(max_survival_score)

    if school_status:
        placeholders = ",".join("?" * len(school_status))
        conditions.append(f"cs.school_status IN ({placeholders})")
        params.extend(school_status)

    if county:
        conditions.append("cs.county LIKE ?")
        params.append(f"%{county}%")

    if census_tract_id:
        conditions.append("cs.census_tract_id = ?")
        params.append(census_tract_id)

    where_clause = "WHERE " + " AND ".join(conditions) if conditions else ""
    query = f"""
        SELECT
            cs.*,
            la.accountability_score,
            la.accountability_rating,
            la.proficiency_reading,
            la.proficiency_math
        FROM charter_schools cs
        LEFT JOIN lea_accountability la
            ON cs.lea_id = la.lea_id
            AND la.data_year = (
                SELECT MAX(data_year) FROM lea_accountability WHERE lea_id = cs.lea_id
            )
        {where_clause}
        ORDER BY cs.school_name
    """

    conn = get_connection()
    df = pd.read_sql_query(query, conn, params=params)
    conn.close()
    return df


def get_charter_school_by_id(school_id: int) -> dict:
    """Return a single charter school by its primary key id."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT * FROM charter_schools WHERE id = ?", (school_id,))
    row = cur.fetchone()
    conn.close()
    return dict(row) if row else {}


def get_charter_school_states() -> list:
    """Return sorted list of states that have charter school data."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT state FROM charter_schools WHERE state IS NOT NULL ORDER BY state")
    states = [row[0] for row in cur.fetchall()]
    conn.close()
    return states


def get_charter_school_summary() -> dict:
    """Return high-level summary counts for the dashboard header."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT
            COUNT(*) as total_schools,
            SUM(CASE WHEN school_status = 'Open' THEN 1 ELSE 0 END) as open_schools,
            SUM(CASE WHEN survival_risk_tier = 'High' THEN 1 ELSE 0 END) as high_risk_schools,
            AVG(survival_score) as avg_survival_score,
            SUM(enrollment) as total_enrollment
        FROM charter_schools
    """)
    row = cur.fetchone()
    conn.close()
    return dict(row) if row else {}


# ---------------------------------------------------------------------------
# Census tract queries
# ---------------------------------------------------------------------------

def get_census_tract(census_tract_id: str) -> dict:
    """Return demographic data for a single census tract."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT * FROM census_tracts WHERE census_tract_id = ?", (census_tract_id,))
    row = cur.fetchone()
    conn.close()
    return dict(row) if row else {}


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


def upsert_charter_school(record: dict):
    """
    Insert or update a charter school record.
    Uses nces_id as the unique key — if a school with that ID exists,
    update it; otherwise insert a new row.
    """
    conn = get_connection()
    cur = conn.cursor()

    columns = list(record.keys())
    values = list(record.values())
    placeholders = ",".join("?" * len(values))
    update_clause = ",".join(f"{col}=excluded.{col}" for col in columns if col != "nces_id")

    sql = f"""
        INSERT INTO charter_schools ({",".join(columns)})
        VALUES ({placeholders})
        ON CONFLICT(nces_id) DO UPDATE SET {update_clause}, updated_at=CURRENT_TIMESTAMP
    """
    cur.execute(sql, values)
    conn.commit()
    conn.close()


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
