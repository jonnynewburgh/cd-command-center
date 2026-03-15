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
            median_household_income REAL,
            poverty_rate REAL,                  -- % below poverty line
            pct_minority REAL,
            unemployment_rate REAL,
            -- NMTC eligibility (Low-Income Community criteria)
            is_nmtc_eligible INTEGER,           -- 1 = eligible, 0 = not
            nmtc_eligibility_reason TEXT,       -- 'Poverty', 'Income', 'Unemployment', etc.
            -- Geography
            county_name TEXT,
            state TEXT,
            data_year INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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
