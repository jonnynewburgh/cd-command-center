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

# DATABASE_URL controls which backend is used:
#   - Not set (default): SQLite at data/cd_command_center.sqlite
#   - postgres://...    : PostgreSQL via psycopg2 (production / GitHub Actions)
DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    os.path.join(os.path.dirname(__file__), "data", "cd_command_center.sqlite"),
)

# True when DATABASE_URL is a Postgres connection string
_IS_POSTGRES = DATABASE_URL.startswith("postgres://") or DATABASE_URL.startswith("postgresql://")


def get_connection():
    """
    Return a database connection.

    SQLite (default): used locally for development.
    PostgreSQL: used in production when DATABASE_URL is a postgres:// URL.

    NOTE: All queries in this file use ? as the parameter placeholder (SQLite style).
    When Postgres is active, _placeholder() and adapt_sql() convert ? → %s automatically.
    """
    if _IS_POSTGRES:
        import psycopg2
        import psycopg2.extras
        conn = psycopg2.connect(DATABASE_URL)
        return conn
    else:
        conn = sqlite3.connect(DATABASE_URL)
        conn.row_factory = sqlite3.Row
        return conn


def adapt_sql(sql: str) -> str:
    """Convert SQLite-style SQL to PostgreSQL-compatible SQL."""
    if _IS_POSTGRES:
        sql = sql.replace("?", "%s")
        sql = sql.replace("INTEGER PRIMARY KEY AUTOINCREMENT", "SERIAL PRIMARY KEY")
    return sql


def _try_exec(cur, sql: str):
    """Execute a statement that might fail (e.g. ADD COLUMN on an existing table).

    PostgreSQL aborts the entire transaction when any statement fails, so we use
    SAVEPOINTs to isolate the failure. SQLite just uses try/except.
    """
    sql = adapt_sql(sql)
    if _IS_POSTGRES:
        cur.execute("SAVEPOINT _safe")
        try:
            cur.execute(sql)
            cur.execute("RELEASE SAVEPOINT _safe")
        except Exception:
            cur.execute("ROLLBACK TO SAVEPOINT _safe")
    else:
        try:
            cur.execute(sql)
        except Exception:
            pass


def init_db():
    """
    Create all tables if they don't exist yet.
    Call this once at app startup or from ETL scripts.
    """
    conn = get_connection()
    _raw_cur = conn.cursor()

    # Wrap the cursor so every cur.execute() call in this function automatically
    # adapts SQL for the active backend (e.g. AUTOINCREMENT → SERIAL for PostgreSQL).
    class _Cur:
        def execute(self, sql, params=None):
            sql = adapt_sql(sql)
            return _raw_cur.execute(sql, params) if params is not None else _raw_cur.execute(sql)
        def fetchone(self):
            return _raw_cur.fetchone()
        def fetchall(self):
            return _raw_cur.fetchall()

    cur = _Cur()

    # Schools — one row per school site (all public schools, not just charters)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS schools (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            nces_id TEXT UNIQUE,           -- National Center for Ed Stats school ID
            seasch TEXT,                   -- State-assigned school ID (used for state-level joins)
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
            -- Metadata
            data_year INTEGER,             -- School year the data represents
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Migrate from old charter_schools table if it exists (SQLite only)
    if not _IS_POSTGRES:
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
                        data_year, created_at, updated_at
                    )
                    SELECT
                        nces_id, school_name, lea_name, lea_id, state, city, address,
                        zip_code, county, census_tract_id, latitude, longitude, enrollment,
                        grade_low, grade_high, 1,
                        pct_free_reduced_lunch, pct_ell, pct_sped, pct_black, pct_hispanic, pct_white,
                        school_status, year_opened, year_closed,
                        data_year, created_at, updated_at
                    FROM charter_schools
                """)
                print(f"  Migrated {old_count:,} records from charter_schools → schools table")
        except Exception:
            pass  # charter_schools doesn't exist, that's fine

    # Add is_charter column to schools table if it was created without it
    _try_exec(cur, "ALTER TABLE schools ADD COLUMN is_charter INTEGER DEFAULT 0")

    # Add demographic columns added in later ETL runs
    for _col, _type in [
        ("pct_asian",      "REAL"),
        ("pct_multiracial","REAL"),
        ("seasch",         "TEXT"),
    ]:
        _try_exec(cur, f"ALTER TABLE schools ADD COLUMN {_col} {_type}")

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
    _try_exec(cur, "ALTER TABLE census_tracts ADD COLUMN median_family_income REAL")
    _try_exec(cur, "ALTER TABLE census_tracts ADD COLUMN nmtc_eligibility_tier TEXT")

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
        _try_exec(cur, f"ALTER TABLE census_tracts ADD COLUMN {col} {col_type}")

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
    cur.execute("CREATE INDEX IF NOT EXISTS idx_schools_lea_id     ON schools(lea_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_schools_name       ON schools(school_name)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_schools_city       ON schools(city)")

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
    cur.execute("CREATE INDEX IF NOT EXISTS idx_fqhc_name          ON fqhc(health_center_name)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_fqhc_city          ON fqhc(city)")

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
    for table, col in [("schools", "ein"), ("fqhc", "ein"), ("cde_allocations", "ein")]:
        _try_exec(cur, f"ALTER TABLE {table} ADD COLUMN {col} TEXT")

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
            _try_exec(cur, f"ALTER TABLE {tbl} ADD COLUMN {col} {col_type}")

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

    # ETL run log — one row per pipeline execution, written by log_load_start/finish()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS data_loads (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            pipeline TEXT NOT NULL,        -- e.g. 'census', 'fqhc', 'nmtc'
            status TEXT NOT NULL,          -- 'running', 'success', 'error'
            rows_loaded INTEGER,           -- count of rows upserted in this run
            error_message TEXT,            -- populated if status = 'error'
            started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            finished_at TIMESTAMP
        )
    """)

    # Market rates — FRED daily rate observations (SOFR, Treasuries, Fed Funds)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS market_rates (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            series_id TEXT NOT NULL,       -- FRED series ID, e.g. 'SOFR', 'DGS10'
            series_name TEXT,              -- Human-readable label
            rate_date TEXT NOT NULL,       -- ISO date (YYYY-MM-DD)
            rate_value REAL,               -- Rate as a percentage (e.g. 5.33 = 5.33%)
            fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(series_id, rate_date)
        )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_rates_series ON market_rates(series_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_rates_date   ON market_rates(rate_date)")

    # HUD Area Median Income — annual income limits by county/metro, family size 4 (standard benchmark)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS hud_ami (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            fiscal_year INTEGER NOT NULL,
            state TEXT,
            fips TEXT NOT NULL,            -- HUD area FIPS code (county or metro)
            area_name TEXT,                -- Metro area or county name
            county_name TEXT,
            median_income REAL,            -- 100% AMI for 4-person family
            limit_30_pct REAL,             -- 30% AMI — Extremely Low Income
            limit_50_pct REAL,             -- 50% AMI — Very Low Income
            limit_80_pct REAL,             -- 80% AMI — Low Income (most common threshold)
            limit_120_pct REAL,            -- 120% AMI — middle-income programs (computed: median_income * 1.2)
            -- Full limits by family size stored as JSON for less common lookups
            -- Format: {"1": {"30": x, "50": y, "80": z}, "2": {...}, ...}
            limits_json TEXT,
            fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(fiscal_year, fips)
        )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_ami_state ON hud_ami(state)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_ami_year  ON hud_ami(fiscal_year)")

    # HUD Fair Market Rents — annual FMRs by county/metro area
    cur.execute("""
        CREATE TABLE IF NOT EXISTS hud_fmr (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            fiscal_year INTEGER NOT NULL,
            state TEXT,
            fips TEXT NOT NULL,            -- HUD FMR area FIPS
            area_name TEXT,
            county_name TEXT,
            fmr_0br REAL,                  -- Studio / efficiency FMR
            fmr_1br REAL,
            fmr_2br REAL,                  -- 2-bedroom is standard benchmark for most programs
            fmr_3br REAL,
            fmr_4br REAL,
            fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(fiscal_year, fips)
        )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_fmr_state ON hud_fmr(state)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_fmr_year  ON hud_fmr(fiscal_year)")

    # CRA institutions — bank CRA exam register from FFIEC.
    # Shows which banks operate in which states; used to identify CRA-motivated capital demand.
    cur.execute("""
        CREATE TABLE IF NOT EXISTS cra_institutions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            respondent_id TEXT NOT NULL,       -- FFIEC respondent ID (unique per institution per year)
            institution_name TEXT,
            city TEXT,
            state TEXT,
            zip_code TEXT,
            asset_size_indicator TEXT,         -- 'Large', 'Intermediate Small', 'Small'
            report_year INTEGER NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(respondent_id, report_year)
        )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_cra_inst_state ON cra_institutions(state)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_cra_inst_year  ON cra_institutions(report_year)")

    # CRA assessment areas — the counties/MSAs each bank covers in its CRA plan.
    # Drives where banks are obligated to deploy capital, which creates NMTC/CDFI demand.
    cur.execute("""
        CREATE TABLE IF NOT EXISTS cra_assessment_areas (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            respondent_id TEXT NOT NULL,       -- joins to cra_institutions.respondent_id
            institution_name TEXT,
            report_year INTEGER NOT NULL,
            state TEXT,
            assessment_area_name TEXT,         -- e.g. "Chicago-Naperville-Elgin, IL"
            area_type TEXT,                    -- 'MSA', 'Non-MSA', 'Statewide'
            county_fips TEXT,                  -- 5-digit FIPS when area is a single county
            msa_code TEXT,                     -- MSA code when area is a metro area
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(respondent_id, report_year, assessment_area_name)
        )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_cra_area_state ON cra_assessment_areas(state)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_cra_area_inst  ON cra_assessment_areas(respondent_id)")

    # CRA small business disclosure — D2-1 flat file.
    # Per-bank, per-census-tract small business lending. The key table for identifying
    # which banks are actively lending (vs. just having assessment area obligations)
    # in a specific geography. One row per bank × tract × row_code × year.
    #
    # Row codes: 101=total SB loans, 102=to biz w/ rev≤$1M, 103=loans≤$100K,
    #            104=loans $100K-$250K, 105=loans $250K-$1M, 106=small farm
    # Amounts are in $thousands. loan_type: S=small biz, L=community dev.
    cur.execute("""
        CREATE TABLE IF NOT EXISTS cra_sb_discl (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            respondent_id TEXT NOT NULL,
            agency_code TEXT NOT NULL,
            year INTEGER NOT NULL,
            state_fips TEXT NOT NULL,
            county_fips TEXT NOT NULL,
            msa_code TEXT NOT NULL,
            census_tract TEXT NOT NULL,   -- 4-char FFIEC code (e.g. '0301')
            census_tract_id TEXT,         -- 11-digit GEOID (state+county+tract+'00')
            row_code TEXT NOT NULL,       -- '101'-'106'
            loan_type TEXT,               -- 'S' or 'L'
            n_total INTEGER,              -- f1: total loan count
            amt_total INTEGER,            -- f2: total amount ($K)
            n_small_biz INTEGER,          -- f3: to businesses with revenues ≤$1M
            amt_small_biz INTEGER,        -- f4: to small biz ($K)
            n_orig INTEGER,               -- f5
            amt_orig INTEGER,             -- f6
            n_orig_sb INTEGER,            -- f7
            amt_orig_sb INTEGER,          -- f8
            n_purch INTEGER,              -- f9
            amt_purch INTEGER,            -- f10
            UNIQUE(respondent_id, agency_code, year, state_fips, county_fips,
                   msa_code, census_tract, row_code, loan_type)
        )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_cra_sbd_tract  ON cra_sb_discl(census_tract_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_cra_sbd_state  ON cra_sb_discl(state_fips, county_fips)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_cra_sbd_inst   ON cra_sb_discl(respondent_id, year)")

    # CRA small business aggregate — A2-1 flat file.
    # All-bank totals by census tract. Shows how much small business lending
    # happened in a tract (regardless of which bank), useful for identifying
    # credit deserts and high-activity markets.
    #
    # census_tract is 7-char FFIEC format with decimal (e.g. '0301.01').
    # Amounts are in $thousands.
    cur.execute("""
        CREATE TABLE IF NOT EXISTS cra_sb_aggr (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            year INTEGER NOT NULL,
            state_fips TEXT NOT NULL,
            county_fips TEXT NOT NULL,
            msa_code TEXT NOT NULL,
            census_tract TEXT NOT NULL,   -- 7-char FFIEC format (e.g. '0301.01')
            census_tract_id TEXT,         -- 11-digit GEOID
            row_code TEXT NOT NULL,       -- '101'-'106'
            n_orig INTEGER,               -- f1: originations count
            amt_orig INTEGER,             -- f2: originations amount ($K)
            n_orig_sb INTEGER,            -- f3: originations to biz w/ rev ≤$1M
            amt_orig_sb INTEGER,          -- f4: amount to small biz ($K)
            n_prev INTEGER,               -- f5: prior year originations count
            amt_prev INTEGER,             -- f6: prior year amount ($K)
            n_prev_sb INTEGER,            -- f7: prior year small biz count
            amt_prev_sb INTEGER,          -- f8: prior year small biz amount ($K)
            UNIQUE(year, state_fips, county_fips, msa_code, census_tract, row_code)
        )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_cra_sba_tract  ON cra_sb_aggr(census_tract_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_cra_sba_state  ON cra_sb_aggr(state_fips, county_fips)")

    # SBA loans — approved 7(a) and 504 loans by borrower geography.
    # Shows existing small-business lending activity; useful for identifying credit deserts
    # and understanding prior SBA penetration in a target market.
    cur.execute("""
        CREATE TABLE IF NOT EXISTS sba_loans (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            loan_number TEXT UNIQUE,
            program TEXT NOT NULL,             -- '7a' or '504'
            borrower_name TEXT,
            borrower_city TEXT,
            borrower_state TEXT,
            borrower_zip TEXT,
            borrower_county TEXT,
            census_tract_id TEXT,              -- 11-digit FIPS (when available from source)
            naics_code TEXT,
            business_type TEXT,               -- 'Corporation', 'Partnership', etc.
            approval_date TEXT,                -- ISO date
            approval_year INTEGER,
            gross_approval REAL,               -- total loan amount approved
            sba_guaranteed_portion REAL,       -- SBA-guaranteed amount
            lender_name TEXT,
            lender_state TEXT,
            jobs_supported INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_sba_state  ON sba_loans(borrower_state)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_sba_year   ON sba_loans(approval_year)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_sba_tract  ON sba_loans(census_tract_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_sba_zip    ON sba_loans(borrower_zip)")

    # HMDA activity — HMDA lending aggregated by census tract and year.
    # Stored as tract-level summaries (not individual loan records) to keep size manageable.
    # Use to identify credit deserts: high-poverty tracts with low origination rates.
    cur.execute("""
        CREATE TABLE IF NOT EXISTS hmda_activity (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            census_tract_id TEXT NOT NULL,     -- 11-digit FIPS
            report_year INTEGER NOT NULL,
            state TEXT,
            county_fips TEXT,                  -- 5-digit FIPS
            -- Application counts
            total_applications INTEGER,
            total_originations INTEGER,
            total_denials INTEGER,
            total_withdrawn INTEGER,
            -- Loan purpose breakdown
            home_purchase_originations INTEGER,
            refinance_originations INTEGER,
            home_improvement_originations INTEGER,
            -- Loan type breakdown
            conventional_originations INTEGER,
            fha_originations INTEGER,
            va_originations INTEGER,
            -- Computed metrics
            denial_rate REAL,                  -- total_denials / total_applications
            origination_rate REAL,             -- total_originations / total_applications
            median_loan_amount REAL,
            total_loan_amount REAL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(census_tract_id, report_year)
        )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_hmda_tract  ON hmda_activity(census_tract_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_hmda_year   ON hmda_activity(report_year)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_hmda_state  ON hmda_activity(state)")

    # BLS unemployment — monthly unemployment rate by MSA or county from BLS/FRED.
    # Provides economic context for deal underwriting and impact reporting.
    cur.execute("""
        CREATE TABLE IF NOT EXISTS bls_unemployment (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            area_fips TEXT NOT NULL,           -- 5-digit county FIPS or MSA code
            area_name TEXT,
            area_type TEXT,                    -- 'county' or 'msa'
            state TEXT,
            period TEXT NOT NULL,              -- YYYY-MM (e.g. '2024-11')
            unemployment_rate REAL,            -- % unemployed
            labor_force INTEGER,
            employed INTEGER,
            unemployed INTEGER,
            fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(area_fips, period)
        )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_unemp_fips   ON bls_unemployment(area_fips)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_unemp_state  ON bls_unemployment(state)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_unemp_period ON bls_unemployment(period)")

    # BLS QCEW — Quarterly Census of Employment and Wages by county and industry.
    # Shows job counts, wages, and establishment counts by NAICS sector.
    # Useful for job impact analysis and understanding the economic base of a target market.
    cur.execute("""
        CREATE TABLE IF NOT EXISTS bls_qcew (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            area_fips TEXT NOT NULL,           -- 5-digit county FIPS (or 'US000' for national)
            area_name TEXT,
            state TEXT,
            year INTEGER NOT NULL,
            quarter INTEGER NOT NULL,          -- 1-4; use quarter=0 for annual averages
            industry_code TEXT NOT NULL,       -- NAICS code or '10' for total all industries
            industry_title TEXT,
            ownership_code TEXT,               -- '0' = total, '5' = private, '1' = federal govt
            establishments INTEGER,
            employment INTEGER,                -- average monthly employment in period
            total_wages REAL,                  -- total quarterly wages
            avg_weekly_wage REAL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(area_fips, year, quarter, industry_code, ownership_code)
        )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_qcew_fips    ON bls_qcew(area_fips)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_qcew_state   ON bls_qcew(state)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_qcew_year    ON bls_qcew(year)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_qcew_naics   ON bls_qcew(industry_code)")

    # SCSC Comprehensive Performance Framework — GA charter school evaluations.
    # School-level CPF scores from the State Charter Schools Commission of Georgia.
    # Loaded from the charters repo (cpf_all_years.csv).
    cur.execute("""
        CREATE TABLE IF NOT EXISTS scsc_cpf (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            nces_id TEXT,                      -- joined from schools table (may be NULL if no match)
            school_name TEXT NOT NULL,
            school_year TEXT NOT NULL,         -- e.g. '2023-24'
            academic_designation TEXT,         -- 'Exceeds', 'Meets', 'Approaches', 'Does Not Meet'
            financial_designation TEXT,        -- same scale
            financial_indicator_1 REAL,        -- fin_ind1 score
            financial_indicator_2 REAL,        -- fin_ind2 score
            operations_score REAL,
            operations_designation TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(school_name, school_year)
        )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_scsc_nces ON scsc_cpf(nces_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_scsc_year ON scsc_cpf(school_year)")

    # NMTC Coalition transaction-level project database.
    # More detailed than CDFI Fund public data: includes project address, total costs, jobs.
    # Matched to nmtc_projects via coalition_id FK (stored on nmtc_projects).
    cur.execute("""
        CREATE TABLE IF NOT EXISTS nmtc_coalition_projects (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            coalition_project_id TEXT UNIQUE,  -- Coalition's own project identifier (if any)
            cdfi_project_id TEXT,              -- CDFI Fund project ID (when known)
            project_name TEXT,
            cde_name TEXT,
            address TEXT,
            city TEXT,
            state TEXT,
            zip_code TEXT,
            census_tract_id TEXT,
            -- Financials
            total_project_costs REAL,
            nmtc_allocation_used REAL,         -- QLICI equivalent from Coalition data
            -- Impact
            jobs_created INTEGER,
            jobs_retained INTEGER,
            -- Classification
            project_type TEXT,                 -- 'Real Estate', 'Operating Business', etc.
            investment_year INTEGER,
            -- Match metadata
            nmtc_project_id INTEGER,           -- FK to nmtc_projects.id when matched
            match_confidence REAL,             -- 0.0–1.0 fuzzy match score
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_coalition_state ON nmtc_coalition_projects(state)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_coalition_cde   ON nmtc_coalition_projects(cde_name)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_coalition_tract ON nmtc_coalition_projects(census_tract_id)")

    # Add coalition_id column to nmtc_projects if not already present (idempotent migration)
    _try_exec(cur, "ALTER TABLE nmtc_projects ADD COLUMN coalition_id INTEGER")

    # ------------------------------------------------------------------
    # Federal Audit Clearinghouse (FAC / Single Audit) — horizontal source
    # Layered across schools, fqhc, cdfi_directory, ece_centers, etc. via EIN.
    # Source: api.fac.gov (GSA, since Oct 2023). Federal public-domain data.
    # See etl/fetch_fac.py.
    # ------------------------------------------------------------------

    # federal_audits — one row per Single Audit submission
    cur.execute("""
        CREATE TABLE IF NOT EXISTS federal_audits (
            report_id TEXT PRIMARY KEY,        -- e.g., '2024-02-GSAFAC-0000045331'

            -- Auditee identity (the join keys to other tables)
            auditee_ein TEXT,                  -- 9-digit; joins to irs_990, fqhc, schools
            auditee_uei TEXT,                  -- SAM.gov UEI (newer federal identifier)
            auditee_name TEXT NOT NULL,
            entity_type TEXT,                  -- 'non-profit', 'state', 'local', 'tribal', 'higher-ed'
            is_multiple_eins BOOLEAN,          -- True → use /additional_eins to find others

            -- Auditee location
            auditee_address_line_1 TEXT,
            auditee_city TEXT,
            auditee_state TEXT,
            auditee_zip TEXT,

            -- Auditee contact (deal-origination outreach data — KEEP)
            auditee_contact_name TEXT,
            auditee_contact_title TEXT,
            auditee_email TEXT,
            auditee_phone TEXT,
            auditee_certify_name TEXT,         -- whoever signed off (often CFO/ED)
            auditee_certify_title TEXT,
            auditee_certified_date DATE,

            -- Audit period
            audit_year INTEGER,
            fy_start_date DATE,
            fy_end_date DATE,
            audit_period_covered TEXT,         -- 'annual', 'biennial', etc.
            audit_type TEXT,                   -- 'single-audit', 'program-specific'

            -- Financial scale
            total_amount_expended BIGINT,      -- total federal $ expended in audit period (BIGINT: state govs can exceed $2.1B INT cap)
            dollar_threshold INTEGER,          -- 750000 (pre-FY25) or 1000000

            -- Audit opinion + findings (normalized from 'Yes'/'No' strings to booleans)
            gaap_results TEXT,                 -- 'unmodified_opinion', 'qualified_opinion', etc.
            is_going_concern BOOLEAN,
            is_material_weakness BOOLEAN,
            is_significant_deficiency BOOLEAN,
            is_material_noncompliance BOOLEAN,
            is_low_risk_auditee BOOLEAN,
            agencies_with_prior_findings TEXT, -- comma-separated agency codes (recurrence signal)

            -- Federal oversight
            cognizant_agency TEXT,
            oversight_agency TEXT,

            -- Auditor (the firm doing the audit)
            auditor_firm_name TEXT,
            auditor_ein TEXT,
            auditor_state TEXT,
            auditor_city TEXT,
            auditor_zip TEXT,
            auditor_address_line_1 TEXT,
            auditor_country TEXT,
            auditor_contact_name TEXT,
            auditor_contact_title TEXT,
            auditor_email TEXT,
            auditor_phone TEXT,
            auditor_certify_name TEXT,
            auditor_certify_title TEXT,
            auditor_certified_date DATE,

            -- Submission timestamps
            submitted_date DATE,
            fac_accepted_date DATE,
            resubmission_version INTEGER,      -- detect updated audits

            -- Provenance
            fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_federal_audits_ein        ON federal_audits(auditee_ein)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_federal_audits_uei        ON federal_audits(auditee_uei)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_federal_audits_state_year ON federal_audits(auditee_state, audit_year)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_federal_audits_entity     ON federal_audits(entity_type)")

    # federal_audit_programs — one row per ALN per audit (line items from /federal_awards)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS federal_audit_programs (
            report_id TEXT NOT NULL,           -- FK to federal_audits.report_id
            award_reference TEXT NOT NULL,     -- e.g., 'AWARD-0001'

            -- ALN (constructed at load time from prefix + extension)
            aln TEXT NOT NULL,                 -- e.g., '14.126'
            federal_agency_prefix TEXT,        -- '14'
            federal_award_extension TEXT,      -- '126'
            federal_program_name TEXT,

            -- Money (BIGINT for all $ fields — federal program totals can exceed $2.1B INT cap)
            amount_expended BIGINT,
            federal_program_total BIGINT,

            -- Type flags
            is_loan BOOLEAN,
            loan_balance BIGINT,
            is_direct BOOLEAN,                 -- direct from feds vs. passthrough
            is_passthrough_award BOOLEAN,
            passthrough_amount BIGINT,
            is_major BOOLEAN,                  -- audited as a major program

            -- Cluster grouping
            cluster_name TEXT,
            other_cluster_name TEXT,
            state_cluster_name TEXT,
            cluster_total BIGINT,

            -- Findings on this specific program
            findings_count INTEGER,
            audit_report_type TEXT,            -- 'U'/'Q'/'A'/'D' (unmodified/qualified/adverse/disclaimer)

            PRIMARY KEY (report_id, award_reference)
        )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_fap_aln       ON federal_audit_programs(aln)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_fap_report_id ON federal_audit_programs(report_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_fap_agency    ON federal_audit_programs(federal_agency_prefix)")

    # ------------------------------------------------------------------
    # Head Start PIR (Program Information Report) — HSES export
    # Program-level data on Head Start / Early Head Start programs.
    # Source: https://hses.ohs.acf.hhs.gov (requires account + DUA).
    # See etl/load_headstart_pir.py.
    # ------------------------------------------------------------------

    cur.execute("""
        CREATE TABLE IF NOT EXISTS headstart_programs (
            grant_number TEXT NOT NULL,
            program_number TEXT NOT NULL,
            pir_year INTEGER NOT NULL,

            -- Program identity
            region TEXT,
            state TEXT,
            program_type TEXT,              -- 'HS', 'EHS', 'Migrant', 'AIAN'
            grantee_name TEXT,
            program_name TEXT,
            agency_type TEXT,               -- 'Community Action Agency (CAA)', 'School System', etc.
            agency_description TEXT,

            -- Location
            address TEXT,
            city TEXT,
            zip_code TEXT,
            phone TEXT,
            email TEXT,

            -- Geocoding (populated later)
            latitude REAL,
            longitude REAL,
            census_tract_id TEXT,

            -- Section A: Enrollment & capacity (the deal-origination core)
            funded_enrollment INTEGER,      -- A.1.a: ACF funded slots
            non_acf_enrollment INTEGER,     -- A.1.b
            total_cumulative_enrollment INTEGER,  -- A.12
            total_slots_center_based INTEGER,     -- A.7
            slots_at_child_care_partner INTEGER,  -- A.7.a
            total_classes INTEGER,          -- A.9
            home_based_slots INTEGER,       -- A.3
            family_child_care_slots INTEGER, -- A.4

            -- Age breakdown
            children_lt1 INTEGER,           -- A.10.a
            children_1yr INTEGER,           -- A.10.b
            children_2yr INTEGER,           -- A.10.c
            children_3yr INTEGER,           -- A.10.d
            children_4yr INTEGER,           -- A.10.e
            children_5plus INTEGER,         -- A.10.f
            pregnant_women INTEGER,         -- A.11

            -- Eligibility
            eligible_income INTEGER,        -- A.13.a: at/below 100% FPL
            eligible_public_assist INTEGER, -- A.13.b
            eligible_foster INTEGER,        -- A.13.c
            eligible_homeless INTEGER,      -- A.13.d

            -- Turnover
            children_left_program INTEGER,  -- A.16 (preschool) or A.18 (EHS)
            children_end_of_year INTEGER,   -- A.17

            -- Demographics
            dual_language_learners INTEGER, -- A.27
            children_transported INTEGER,   -- A.28
            children_with_subsidy INTEGER,  -- A.24

            -- Section B: Staffing
            total_staff INTEGER,            -- B.1-1
            total_contracted_staff INTEGER, -- B.1-2
            classroom_teachers INTEGER,     -- B.3-1
            assistant_teachers INTEGER,     -- B.3-2
            teachers_ba_or_higher INTEGER,  -- B.3.a-1 + B.3.b-1
            volunteers INTEGER,             -- B.2

            -- Section C: Health (FQHC cross-ref signal)
            children_with_insurance_start INTEGER,   -- C.1-1
            children_with_insurance_end INTEGER,     -- C.1-2
            children_medicaid_start INTEGER,          -- C.1.a-1
            children_no_insurance_start INTEGER,      -- C.2-1
            children_with_medical_home_start INTEGER, -- C.5-1
            children_at_fqhc_start INTEGER,           -- C.5.a-1

            -- Section D: Administration
            child_care_partners INTEGER,    -- D.6
            leas_in_service_area INTEGER,   -- D.7

            -- Provenance
            data_source TEXT DEFAULT 'HSES PIR Export',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

            PRIMARY KEY (grant_number, program_number, pir_year)
        )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_hs_state      ON headstart_programs(state)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_hs_type       ON headstart_programs(program_type)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_hs_grantee    ON headstart_programs(grantee_name)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_hs_tract      ON headstart_programs(census_tract_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_hs_zip        ON headstart_programs(zip_code)")

    conn.commit()
    conn.close()


# ---------------------------------------------------------------------------
# Caching helper — no-op; kept so all @_cached decorators still work without
# change. If a caching layer is added later (e.g. functools.lru_cache or
# Redis), swap the implementation here.
# ---------------------------------------------------------------------------

def _cached(ttl=300):
    """No-op decorator. Preserves the @_cached(ttl=...) call signature."""
    def decorator(func):
        return func
    return decorator


# ---------------------------------------------------------------------------
# ETL helpers — used by all pipeline scripts
# ---------------------------------------------------------------------------

def upsert_rows(table: str, rows: list[dict], unique_cols: list[str]) -> int:
    """
    Insert or update rows in a table. Returns the number of rows processed.

    How it works:
    - For each row, attempt an INSERT.
    - If a row already exists (based on unique_cols), UPDATE the non-unique columns.
    - This makes every pipeline script idempotent: safe to re-run without duplicating data.

    Args:
        table:       Table name, e.g. 'schools' or 'census_tracts'
        rows:        List of dicts, where each dict is one row (keys = column names)
        unique_cols: List of column names that uniquely identify a row, e.g. ['nces_id']

    Example:
        upsert_rows('schools', school_records, unique_cols=['nces_id'])
    """
    if not rows:
        return 0

    conn = get_connection()
    cur = conn.cursor()
    count = 0

    for row in rows:
        cols = list(row.keys())
        vals = list(row.values())
        placeholders = ", ".join(["?"] * len(cols))
        col_names = ", ".join(cols)

        # Build the UPDATE clause for non-unique columns
        update_cols = [c for c in cols if c not in unique_cols]
        if update_cols:
            update_clause = ", ".join(f"{c} = excluded.{c}" for c in update_cols)
            conflict_clause = f"ON CONFLICT ({', '.join(unique_cols)}) DO UPDATE SET {update_clause}"
        else:
            conflict_clause = f"ON CONFLICT ({', '.join(unique_cols)}) DO NOTHING"

        sql = adapt_sql(
            f"INSERT INTO {table} ({col_names}) VALUES ({placeholders}) {conflict_clause}"
        )
        cur.execute(sql, vals)
        count += 1

    conn.commit()
    conn.close()
    return count


def log_load_start(pipeline: str) -> int:
    """
    Record the start of a pipeline run. Returns the run ID.
    Call this at the top of every ETL script, then pass the ID to log_load_finish().

    Example:
        run_id = db.log_load_start('census')
        ... do work ...
        db.log_load_finish(run_id, rows_loaded=n)
    """
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        adapt_sql("INSERT INTO data_loads (pipeline, status) VALUES (?, 'running')"),
        [pipeline],
    )
    run_id = cur.lastrowid
    conn.commit()
    conn.close()
    return run_id


def log_load_finish(run_id: int, rows_loaded: int = 0, error: str = None):
    """
    Update a pipeline run log with the result.

    Args:
        run_id:      ID returned by log_load_start()
        rows_loaded: How many rows were inserted/updated
        error:       If the pipeline failed, pass the error message here
    """
    status = "error" if error else "success"
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        adapt_sql("""
            UPDATE data_loads
            SET status = ?, rows_loaded = ?, error_message = ?,
                finished_at = CURRENT_TIMESTAMP
            WHERE id = ?
        """),
        [status, rows_loaded, error, run_id],
    )
    conn.commit()
    conn.close()


# ---------------------------------------------------------------------------
# School queries (formerly charter_schools)
# ---------------------------------------------------------------------------

@_cached(ttl=300)
def get_schools(
    states=None,
    min_enrollment=None,
    max_enrollment=None,
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
                    SUM(enrollment) as total_enrollment
                FROM {table}
                {charter_filter}
            """)
            row = cur.fetchone()
            conn.close()
            if not row:
                return {}
            cols = [d[0] for d in cur.description]
            return dict(zip(cols, row))
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
    if not row:
        return {}
    cols = [d[0] for d in cur.description]
    return dict(zip(cols, row))


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
        if not row:
            result = {}
        else:
            cols = [d[0] for d in cur.description]
            result = dict(zip(cols, row))
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
        if row:
            cols = [d[0] for d in cur.description]
            result = dict(zip(cols, row))
        else:
            result = {}
    except Exception:
        result = {}
    conn.close()
    return result


# ---------------------------------------------------------------------------
# Global search — across schools, NMTC projects, CDEs, FQHCs, ECE
# ---------------------------------------------------------------------------

def _search_table(conn, table: str, columns: list, term: str,
                  order_by: str = None) -> pd.DataFrame:
    """Run a LIKE search across columns in a table, returning up to 200 rows.

    Returns an empty DataFrame if the query fails (e.g. table doesn't exist).
    Wraps SQL through adapt_sql() so ? placeholders are converted to %s on Postgres.
    """
    where = " OR ".join(f"{col} LIKE ?" for col in columns)
    order = order_by or columns[0]
    sql = adapt_sql(f"SELECT * FROM {table} WHERE {where} ORDER BY {order} LIMIT 200")
    params = [term] * len(columns)
    try:
        return pd.read_sql_query(sql, conn, params=params)
    except Exception:
        return pd.DataFrame()


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

    schools_df = _search_table(conn, "schools",
        ["school_name", "city", "lea_name", "nces_id", "state"], like, order_by="school_name")
    projects_df = _search_table(conn, "nmtc_projects",
        ["project_name", "cde_name", "city", "state", "census_tract_id"], like, order_by="project_name")
    cdes_df = _search_table(conn, "cde_allocations",
        ["cde_name", "city", "state", "service_areas"], like, order_by="cde_name")
    fqhc_df = _search_table(conn, "fqhc",
        ["health_center_name", "site_name", "city", "state"], like, order_by="health_center_name")
    ece_df = _search_table(conn, "ece_centers",
        ["provider_name", "operator_name", "city", "state"], like, order_by="provider_name")

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
            cur.execute(adapt_sql(sql), values)
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
    """Insert or update a census tract record (keyed on census_tract_id).

    Columns populated by separate ETL steps (EJScreen, pct_minority, county_name,
    pop_uninsured, pop_65_plus, historical change columns) are preserved when the
    incoming record has NULL for those fields — so re-running the main ACS load
    does not wipe enrichment data written by other pipelines.
    """
    conn = get_connection()
    cur = conn.cursor()

    # These columns are populated by separate ETL steps (not load_census_tracts.py).
    # On conflict, keep the existing non-null value rather than overwriting with NULL.
    preserve_if_null = {
        "pct_minority", "county_name",
        "pop_uninsured", "pop_65_plus",
        "ej_index", "pm25_percentile", "diesel_percentile",
        "lead_paint_percentile", "superfund_percentile", "wastewater_percentile",
        "poverty_rate_5yr_ago", "median_income_5yr_ago",
        "poverty_rate_change", "income_change_pct",
        "is_opportunity_zone",
    }

    columns = list(record.keys())
    values = list(record.values())
    placeholders = ",".join("?" * len(values))
    update_parts = []
    for col in columns:
        if col == "census_tract_id":
            continue
        if col in preserve_if_null:
            # COALESCE: keep existing value if incoming is NULL
            update_parts.append(f"{col}=COALESCE(excluded.{col}, census_tracts.{col})")
        else:
            update_parts.append(f"{col}=excluded.{col}")
    update_clause = ",".join(update_parts)

    sql = f"""
        INSERT INTO census_tracts ({",".join(columns)})
        VALUES ({placeholders})
        ON CONFLICT(census_tract_id) DO UPDATE SET {update_clause}
    """
    cur.execute(adapt_sql(sql), values)
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
    cur.execute(adapt_sql(sql), values)
    conn.commit()
    conn.close()


def batch_update_ece_geo(records: list[dict]):
    """
    Update latitude, longitude, and census_tract_id for many ECE centers at once.

    Args:
        records: list of dicts, each with 'license_id', 'latitude', 'longitude',
                 and optionally 'census_tract_id'
    """
    if not records:
        return
    conn = get_connection()
    cur = conn.cursor()
    cur.executemany(
        adapt_sql("""
            UPDATE ece_centers
            SET latitude = ?, longitude = ?, census_tract_id = ?
            WHERE license_id = ?
        """),
        [(r["latitude"], r["longitude"], r.get("census_tract_id"), r["license_id"])
         for r in records],
    )
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
    cur.execute(adapt_sql(sql), values)
    conn.commit()
    conn.close()


def batch_update_fqhc_geo(records: list[dict]):
    """
    Update latitude, longitude, and census_tract_id for many FQHC sites at once.

    Args:
        records: list of dicts, each with 'bhcmis_id', 'latitude', 'longitude',
                 and optionally 'census_tract_id'
    """
    if not records:
        return
    conn = get_connection()
    cur = conn.cursor()
    cur.executemany(
        adapt_sql("""
            UPDATE fqhc
            SET latitude = ?, longitude = ?, census_tract_id = ?
            WHERE bhcmis_id = ?
        """),
        [(r["latitude"], r["longitude"], r.get("census_tract_id"), r["bhcmis_id"])
         for r in records],
    )
    conn.commit()
    conn.close()


def get_fqhc_by_id(bhcmis_id: str) -> dict:
    """Return a single FQHC site by its bhcmis_id. Returns empty dict if not found."""
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(adapt_sql("SELECT * FROM fqhc WHERE bhcmis_id = ?"), (bhcmis_id,))
        row = cur.fetchone()
        if not row:
            conn.close()
            return {}
        cols = [d[0] for d in cur.description]
        result = dict(zip(cols, row))
        conn.close()
        return result
    except Exception:
        conn.close()
        return {}


def get_ece_by_id(license_id: str) -> dict:
    """Return a single ECE center by its license_id. Returns empty dict if not found."""
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(adapt_sql("SELECT * FROM ece_centers WHERE license_id = ?"), (license_id,))
        row = cur.fetchone()
        if not row:
            conn.close()
            return {}
        cols = [d[0] for d in cur.description]
        result = dict(zip(cols, row))
        conn.close()
        return result
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
    batch_update_school_census_tracts([{"nces_id": nces_id, "census_tract_id": census_tract_id}])


def batch_update_school_census_tracts(records: list[dict]):
    """
    Update census_tract_id for many schools in a single transaction.
    Much faster than calling update_school_census_tract() in a loop.

    Args:
        records: list of dicts, each with 'nces_id' and 'census_tract_id' keys

    Example:
        batch_update_school_census_tracts([
            {"nces_id": "123456", "census_tract_id": "06037201300"},
            {"nces_id": "789012", "census_tract_id": "06037202000"},
        ])
    """
    if not records:
        return
    conn = get_connection()
    cur = conn.cursor()
    for table in ["schools", "charter_schools"]:
        try:
            cur.executemany(
                adapt_sql(
                    f"UPDATE {table} SET census_tract_id = ?, updated_at = CURRENT_TIMESTAMP WHERE nces_id = ?"
                ),
                [(r["census_tract_id"], r["nces_id"]) for r in records],
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

        # Preserve existing audit-based values if already set.
        # Use a direct (uncached) DB read so we get fresh data — the cached
        # get_financial_ratios() would return stale rows during a compute run.
        acid_audit = None
        cl_audit   = None
        has_audit  = 0
        try:
            _conn = get_connection()
            _cur  = _conn.cursor()
            _cur.execute(
                "SELECT acid_ratio_audit, current_liabilities_audit, has_audit_data "
                "FROM financial_ratios WHERE ein = ? AND fiscal_year = ?",
                (ein, fiscal_year),
            )
            _row = _cur.fetchone()
            _conn.close()
            if _row:
                acid_audit = _row[0]
                cl_audit   = _row[1]
                has_audit  = _row[2] or 0
        except Exception:
            pass

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
# Market rates — FRED daily observations
# ---------------------------------------------------------------------------

def get_market_rates(
    series_ids=None,
    start_date: str = None,
    end_date: str = None,
    days: int = None,
) -> pd.DataFrame:
    """
    Return market rate observations from the market_rates table.

    Args:
        series_ids: list of FRED series IDs to include (None = all series)
        start_date: ISO date string 'YYYY-MM-DD' — inclusive lower bound
        end_date: ISO date string 'YYYY-MM-DD' — inclusive upper bound
        days: if provided, return the last N calendar days (overrides start_date)
    """
    conditions = []
    params = []

    if series_ids:
        placeholders = ",".join("?" * len(series_ids))
        conditions.append(f"series_id IN ({placeholders})")
        params.extend(series_ids)

    if days is not None:
        # SQLite: date('now', '-N days'); PostgreSQL: CURRENT_DATE - INTERVAL 'N days'
        if _IS_POSTGRES:
            conditions.append("rate_date >= CURRENT_DATE - INTERVAL '%s days'")
            params.append(days)
        else:
            conditions.append(f"rate_date >= date('now', '-{int(days)} days')")

    else:
        if start_date:
            conditions.append("rate_date >= ?")
            params.append(start_date)
        if end_date:
            conditions.append("rate_date <= ?")
            params.append(end_date)

    where_clause = "WHERE " + " AND ".join(conditions) if conditions else ""
    query = f"""
        SELECT series_id, series_name, rate_date, rate_value
        FROM market_rates
        {where_clause}
        ORDER BY series_id, rate_date DESC
    """

    conn = get_connection()
    try:
        df = pd.read_sql_query(adapt_sql(query), conn, params=params)
    except Exception:
        df = pd.DataFrame()
    conn.close()
    return df


@_cached(ttl=300)
def get_latest_rates() -> pd.DataFrame:
    """
    Return the single most recent observation for every rate series.
    Used for the dashboard rate ticker / summary cards.
    """
    conn = get_connection()
    try:
        df = pd.read_sql_query(
            """
            SELECT m.series_id, m.series_name, m.rate_date, m.rate_value
            FROM market_rates m
            INNER JOIN (
                SELECT series_id, MAX(rate_date) AS max_date
                FROM market_rates
                GROUP BY series_id
            ) latest ON m.series_id = latest.series_id
                    AND m.rate_date  = latest.max_date
            ORDER BY m.series_id
            """,
            conn,
        )
    except Exception:
        df = pd.DataFrame()
    conn.close()
    return df


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


# ---------------------------------------------------------------------------
# HUD Area Median Income (AMI)
# ---------------------------------------------------------------------------

def get_hud_ami(fiscal_year=None, state=None, fips=None):
    """
    Return HUD Area Median Income records.

    Args:
        fiscal_year: HUD fiscal year (e.g. 2024)
        state:       2-letter state abbreviation
        fips:        HUD area FIPS code (county or metro)
    """
    conditions, params = [], []
    if fiscal_year is not None:
        conditions.append("fiscal_year = ?"); params.append(fiscal_year)
    if state:
        conditions.append("state = ?"); params.append(state.upper())
    if fips:
        conditions.append("fips = ?"); params.append(fips)
    where = "WHERE " + " AND ".join(conditions) if conditions else ""
    query = f"""
        SELECT fiscal_year, state, fips, area_name, county_name,
               median_income, limit_30_pct, limit_50_pct, limit_80_pct, limit_120_pct
        FROM hud_ami {where}
        ORDER BY state, area_name
    """
    conn = get_connection()
    try:
        df = pd.read_sql_query(adapt_sql(query), conn, params=params)
    except Exception:
        df = pd.DataFrame()
    conn.close()
    return df


# ---------------------------------------------------------------------------
# HUD Fair Market Rents (FMR)
# ---------------------------------------------------------------------------

def get_hud_fmr(fiscal_year=None, state=None, fips=None):
    """
    Return HUD Fair Market Rent records.

    Args:
        fiscal_year: HUD fiscal year (e.g. 2025)
        state:       2-letter state abbreviation
        fips:        HUD FMR area FIPS code
    """
    conditions, params = [], []
    if fiscal_year is not None:
        conditions.append("fiscal_year = ?"); params.append(fiscal_year)
    if state:
        conditions.append("state = ?"); params.append(state.upper())
    if fips:
        conditions.append("fips = ?"); params.append(fips)
    where = "WHERE " + " AND ".join(conditions) if conditions else ""
    query = f"""
        SELECT fiscal_year, state, fips, area_name, county_name,
               fmr_0br, fmr_1br, fmr_2br, fmr_3br, fmr_4br
        FROM hud_fmr {where}
        ORDER BY state, area_name
    """
    conn = get_connection()
    try:
        df = pd.read_sql_query(adapt_sql(query), conn, params=params)
    except Exception:
        df = pd.DataFrame()
    conn.close()
    return df


# ---------------------------------------------------------------------------
# CRA Institutions and Assessment Areas
# ---------------------------------------------------------------------------

def get_cra_institutions(state=None, report_year=None, asset_size=None, search=None, limit=None):
    """
    Return CRA institution records from the FFIEC exam register.

    Args:
        state:       2-letter state abbreviation
        report_year: CRA exam report year
        asset_size:  "Large", "Intermediate Small", or "Small"
        search:      substring search on institution_name
        limit:       max rows to return
    """
    conditions, params = [], []
    if state:
        conditions.append("state = ?"); params.append(state.upper())
    if report_year is not None:
        conditions.append("report_year = ?"); params.append(report_year)
    if asset_size:
        conditions.append("asset_size_indicator = ?"); params.append(asset_size)
    if search:
        conditions.append("institution_name LIKE ?"); params.append(f"%{search}%")
    where = "WHERE " + " AND ".join(conditions) if conditions else ""
    limit_clause = f"LIMIT {int(limit)}" if limit else ""
    query = f"""
        SELECT respondent_id, institution_name, city, state, zip_code,
               asset_size_indicator, report_year
        FROM cra_institutions {where}
        ORDER BY state, institution_name {limit_clause}
    """
    conn = get_connection()
    try:
        df = pd.read_sql_query(adapt_sql(query), conn, params=params)
    except Exception:
        df = pd.DataFrame()
    conn.close()
    return df


def get_cra_assessment_areas(state=None, report_year=None, respondent_id=None, county_fips=None):
    """
    Return CRA assessment areas — geographies each bank covers in its CRA plan.
    Joined to cra_institutions for asset size context.

    Args:
        state:          2-letter state abbreviation
        report_year:    CRA exam year
        respondent_id:  FFIEC respondent ID to get all areas for one bank
        county_fips:    5-digit county FIPS to find all banks serving that county
    """
    conditions, params = [], []
    if state:
        conditions.append("a.state = ?"); params.append(state.upper())
    if report_year is not None:
        conditions.append("a.report_year = ?"); params.append(report_year)
    if respondent_id:
        conditions.append("a.respondent_id = ?"); params.append(respondent_id)
    if county_fips:
        conditions.append("a.county_fips = ?"); params.append(county_fips)
    where = "WHERE " + " AND ".join(conditions) if conditions else ""
    query = f"""
        SELECT a.respondent_id, a.institution_name, a.report_year,
               a.state, a.assessment_area_name, a.area_type,
               a.county_fips, a.msa_code,
               i.asset_size_indicator, i.city AS inst_city
        FROM cra_assessment_areas a
        LEFT JOIN cra_institutions i
               ON a.respondent_id = i.respondent_id
              AND a.report_year    = i.report_year
        {where}
        ORDER BY a.state, a.institution_name
    """
    conn = get_connection()
    try:
        df = pd.read_sql_query(adapt_sql(query), conn, params=params)
    except Exception:
        df = pd.DataFrame()
    conn.close()
    return df


# ---------------------------------------------------------------------------
# SBA Loans
# ---------------------------------------------------------------------------

def get_sba_loans(state=None, year=None, program=None, census_tract_id=None,
                  zip_code=None, naics_code=None, limit=500):
    """
    Return SBA 7(a) and 504 loan records.

    Args:
        state:           borrower state (2-letter)
        year:            approval year
        program:         "7a" or "504"
        census_tract_id: 11-digit FIPS
        zip_code:        5-digit borrower ZIP
        naics_code:      NAICS code prefix (e.g. "72" matches all hospitality)
        limit:           max rows (default 500)
    """
    conditions, params = [], []
    if state:
        conditions.append("borrower_state = ?"); params.append(state.upper())
    if year is not None:
        conditions.append("approval_year = ?"); params.append(year)
    if program:
        conditions.append("program = ?"); params.append(program.lower())
    if census_tract_id:
        conditions.append("census_tract_id = ?"); params.append(census_tract_id)
    if zip_code:
        conditions.append("borrower_zip = ?"); params.append(zip_code)
    if naics_code:
        conditions.append("naics_code LIKE ?"); params.append(f"{naics_code}%")
    where = "WHERE " + " AND ".join(conditions) if conditions else ""
    limit_clause = f"LIMIT {int(limit)}" if limit else ""
    query = f"""
        SELECT loan_number, program, borrower_name, borrower_city,
               borrower_state, borrower_zip, borrower_county,
               census_tract_id, naics_code, business_type,
               approval_date, approval_year,
               gross_approval, sba_guaranteed_portion,
               lender_name, lender_state, jobs_supported
        FROM sba_loans {where}
        ORDER BY approval_date DESC {limit_clause}
    """
    conn = get_connection()
    try:
        df = pd.read_sql_query(adapt_sql(query), conn, params=params)
    except Exception:
        df = pd.DataFrame()
    conn.close()
    return df


def get_sba_summary(state=None, year=None):
    """Return aggregate SBA stats: count, total amount, jobs supported."""
    conditions, params = [], []
    if state:
        conditions.append("borrower_state = ?"); params.append(state.upper())
    if year is not None:
        conditions.append("approval_year = ?"); params.append(year)
    where = "WHERE " + " AND ".join(conditions) if conditions else ""
    query = f"""
        SELECT COUNT(*) AS total_loans, SUM(gross_approval) AS total_amount,
               SUM(jobs_supported) AS total_jobs,
               COUNT(DISTINCT borrower_state) AS states
        FROM sba_loans {where}
    """
    conn = get_connection()
    try:
        row = pd.read_sql_query(adapt_sql(query), conn, params=params).iloc[0]
        result = {k: (None if pd.isna(v) else v) for k, v in row.items()}
    except Exception:
        result = {}
    conn.close()
    return result


# ---------------------------------------------------------------------------
# HMDA Activity
# ---------------------------------------------------------------------------

def get_hmda_activity(census_tract_id=None, state=None, county_fips=None,
                      report_year=None, min_denial_rate=None, limit=500):
    """
    Return HMDA mortgage lending activity aggregated by census tract.

    Args:
        census_tract_id: 11-digit FIPS (exact match)
        state:           2-letter state abbreviation
        county_fips:     5-digit county FIPS
        report_year:     HMDA report year
        min_denial_rate: minimum denial rate 0-1 (credit desert filter)
        limit:           max rows
    """
    conditions, params = [], []
    if census_tract_id:
        conditions.append("census_tract_id = ?"); params.append(census_tract_id)
    if state:
        conditions.append("state = ?"); params.append(state.upper())
    if county_fips:
        conditions.append("county_fips = ?"); params.append(county_fips)
    if report_year is not None:
        conditions.append("report_year = ?"); params.append(report_year)
    if min_denial_rate is not None:
        conditions.append("denial_rate >= ?"); params.append(min_denial_rate)
    where = "WHERE " + " AND ".join(conditions) if conditions else ""
    limit_clause = f"LIMIT {int(limit)}" if limit else ""
    query = f"""
        SELECT census_tract_id, report_year, state, county_fips,
               total_applications, total_originations, total_denials,
               home_purchase_originations, refinance_originations,
               conventional_originations, fha_originations, va_originations,
               denial_rate, origination_rate, median_loan_amount, total_loan_amount
        FROM hmda_activity {where}
        ORDER BY state, census_tract_id {limit_clause}
    """
    conn = get_connection()
    try:
        df = pd.read_sql_query(adapt_sql(query), conn, params=params)
    except Exception:
        df = pd.DataFrame()
    conn.close()
    return df


# ---------------------------------------------------------------------------
# BLS Unemployment
# ---------------------------------------------------------------------------

def get_bls_unemployment(area_fips=None, state=None, area_type=None,
                         start_period=None, end_period=None, months=None):
    """
    Return BLS unemployment rate records by geography and period.

    Args:
        area_fips:     5-digit county FIPS or MSA code
        state:         2-letter state abbreviation
        area_type:     "county" or "msa"
        start_period:  "YYYY-MM" inclusive lower bound
        end_period:    "YYYY-MM" inclusive upper bound
        months:        return the last N months (overrides start_period)
    """
    conditions, params = [], []
    if area_fips:
        conditions.append("area_fips = ?"); params.append(area_fips)
    if state:
        conditions.append("state = ?"); params.append(state.upper())
    if area_type:
        conditions.append("area_type = ?"); params.append(area_type)
    if months is not None:
        if _IS_POSTGRES:
            conditions.append("period >= TO_CHAR(CURRENT_DATE - INTERVAL %s, 'YYYY-MM')")
            params.append(f"{int(months)} months")
        else:
            conditions.append(f"period >= strftime('%Y-%m', date('now', '-{int(months)} months'))")
    else:
        if start_period:
            conditions.append("period >= ?"); params.append(start_period)
        if end_period:
            conditions.append("period <= ?"); params.append(end_period)
    where = "WHERE " + " AND ".join(conditions) if conditions else ""
    query = f"""
        SELECT area_fips, area_name, area_type, state, period,
               unemployment_rate, labor_force, employed, unemployed
        FROM bls_unemployment {where}
        ORDER BY area_fips, period DESC
    """
    conn = get_connection()
    try:
        df = pd.read_sql_query(adapt_sql(query), conn, params=params)
    except Exception:
        df = pd.DataFrame()
    conn.close()
    return df


# ---------------------------------------------------------------------------
# BLS QCEW (Quarterly Census of Employment and Wages)
# ---------------------------------------------------------------------------

def get_bls_qcew(area_fips=None, state=None, year=None, quarter=None,
                 industry_code=None, ownership_code=None, limit=500):
    """
    Return BLS QCEW employment and wage records.

    Args:
        area_fips:      5-digit county FIPS
        state:          2-letter state abbreviation
        year:           calendar year
        quarter:        1-4 for quarterly data; 0 for annual averages
        industry_code:  NAICS code or "10" for all-industry total
        ownership_code: "0" = total, "5" = private sector, "1" = federal govt
        limit:          max rows (default 500)
    """
    conditions, params = [], []
    if area_fips:
        conditions.append("area_fips = ?"); params.append(area_fips)
    if state:
        conditions.append("state = ?"); params.append(state.upper())
    if year is not None:
        conditions.append("year = ?"); params.append(year)
    if quarter is not None:
        conditions.append("quarter = ?"); params.append(quarter)
    if industry_code:
        conditions.append("industry_code = ?"); params.append(industry_code)
    if ownership_code:
        conditions.append("ownership_code = ?"); params.append(ownership_code)
    where = "WHERE " + " AND ".join(conditions) if conditions else ""
    limit_clause = f"LIMIT {int(limit)}" if limit else ""
    query = f"""
        SELECT area_fips, area_name, state, year, quarter,
               industry_code, industry_title, ownership_code,
               establishments, employment, total_wages, avg_weekly_wage
        FROM bls_qcew {where}
        ORDER BY area_fips, year DESC, quarter DESC, industry_code
        {limit_clause}
    """
    conn = get_connection()
    try:
        df = pd.read_sql_query(adapt_sql(query), conn, params=params)
    except Exception:
        df = pd.DataFrame()
    conn.close()
    return df


# ---------------------------------------------------------------------------
# SCSC Comprehensive Performance Framework (GA charter schools)
# ---------------------------------------------------------------------------

def upsert_scsc_cpf(record: dict):
    """Insert or update an SCSC CPF record (keyed on school_name + school_year)."""
    columns = list(record.keys())
    values  = list(record.values())
    placeholders = ",".join("?" * len(values))
    update_cols  = [c for c in columns if c not in ("school_name", "school_year")]
    update_clause = ",".join(f"{c}=excluded.{c}" for c in update_cols)
    sql = f"""
        INSERT INTO scsc_cpf ({",".join(columns)})
        VALUES ({placeholders})
        ON CONFLICT(school_name, school_year) DO UPDATE SET {update_clause}
    """
    conn = get_connection()
    conn.cursor().execute(adapt_sql(sql), values)
    conn.commit()
    conn.close()


def get_scsc_cpf(school_year=None, nces_id=None, school_name=None, designation=None):
    """
    Return SCSC CPF scores for GA charter schools.

    Args:
        school_year:  e.g. "2023-24"
        nces_id:      NCES school identifier
        school_name:  substring match on school name
        designation:  filter by academic or ops designation (e.g. "Exceeds")
    """
    conditions, params = [], []
    if school_year:
        conditions.append("school_year = ?"); params.append(school_year)
    if nces_id:
        conditions.append("nces_id = ?"); params.append(nces_id)
    if school_name:
        conditions.append("school_name LIKE ?"); params.append(f"%{school_name}%")
    if designation:
        conditions.append("(academic_designation = ? OR operations_designation = ?)")
        params.extend([designation, designation])
    where = "WHERE " + " AND ".join(conditions) if conditions else ""
    query = f"""
        SELECT nces_id, school_name, school_year,
               academic_designation, financial_designation,
               financial_indicator_1, financial_indicator_2,
               operations_score, operations_designation
        FROM scsc_cpf {where}
        ORDER BY school_year DESC, school_name
    """
    conn = get_connection()
    try:
        df = pd.read_sql_query(adapt_sql(query), conn, params=params)
    except Exception:
        df = pd.DataFrame()
    conn.close()
    return df


# ---------------------------------------------------------------------------
# NMTC Coalition project database
# ---------------------------------------------------------------------------

def upsert_nmtc_coalition_project(record: dict):
    """Insert or update an NMTC Coalition project (keyed on coalition_project_id)."""
    columns = list(record.keys())
    values  = list(record.values())
    placeholders = ",".join("?" * len(values))
    update_cols  = [c for c in columns if c != "coalition_project_id"]
    update_clause = ",".join(f"{c}=excluded.{c}" for c in update_cols)
    sql = f"""
        INSERT INTO nmtc_coalition_projects ({",".join(columns)})
        VALUES ({placeholders})
        ON CONFLICT(coalition_project_id) DO UPDATE SET {update_clause}
    """
    conn = get_connection()
    conn.cursor().execute(adapt_sql(sql), values)
    conn.commit()
    conn.close()


def get_nmtc_coalition_projects(state=None, cde_name=None, investment_year=None,
                                 matched_only=False, limit=500):
    """
    Return NMTC Coalition project records.

    Args:
        state:           2-letter state abbreviation
        cde_name:        substring match on CDE name
        investment_year: year of NMTC closing
        matched_only:    if True, only return projects matched to nmtc_projects
        limit:           max rows
    """
    conditions, params = [], []
    if state:
        conditions.append("state = ?"); params.append(state.upper())
    if cde_name:
        conditions.append("cde_name LIKE ?"); params.append(f"%{cde_name}%")
    if investment_year is not None:
        conditions.append("investment_year = ?"); params.append(investment_year)
    if matched_only:
        conditions.append("nmtc_project_id IS NOT NULL")
    where = "WHERE " + " AND ".join(conditions) if conditions else ""
    limit_clause = f"LIMIT {int(limit)}" if limit else ""
    query = f"""
        SELECT coalition_project_id, cdfi_project_id, project_name,
               cde_name, address, city, state, zip_code, census_tract_id,
               total_project_costs, nmtc_allocation_used,
               jobs_created, jobs_retained, project_type, investment_year,
               nmtc_project_id, match_confidence
        FROM nmtc_coalition_projects {where}
        ORDER BY state, investment_year DESC {limit_clause}
    """
    conn = get_connection()
    try:
        df = pd.read_sql_query(adapt_sql(query), conn, params=params)
    except Exception:
        df = pd.DataFrame()
    conn.close()
    return df


def link_nmtc_coalition_to_projects():
    """
    Backfill nmtc_projects.coalition_id from matched Coalition records.
    Call after load_nmtc_coalition.py has run matching.
    Returns the number of projects updated.
    """
    conn = get_connection()
    cur  = conn.cursor()
    cur.execute(adapt_sql("""
        UPDATE nmtc_projects
        SET coalition_id = (
            SELECT cp.id
            FROM nmtc_coalition_projects cp
            WHERE cp.nmtc_project_id = nmtc_projects.id
            ORDER BY cp.match_confidence DESC
            LIMIT 1
        )
        WHERE EXISTS (
            SELECT 1 FROM nmtc_coalition_projects cp
            WHERE cp.nmtc_project_id = nmtc_projects.id
        )
    """))
    conn.commit()
    n = cur.rowcount
    conn.close()
    return n


# ---------------------------------------------------------------------------
# Federal Audit Clearinghouse (Single Audit)
# ---------------------------------------------------------------------------

def get_federal_audits(state=None, audit_year=None, ein=None, entity_type=None,
                       has_findings=None, is_going_concern=None, limit=500):
    """
    Return federal Single Audit records.

    Args:
        state:            2-letter state abbreviation
        audit_year:       fiscal year of audit
        ein:              auditee EIN
        entity_type:      'non-profit', 'state', 'local', 'tribal', 'higher-ed'
        has_findings:     if True, only audits with material weakness or noncompliance
        is_going_concern: if True, only going-concern opinions
        limit:            max rows
    """
    conditions, params = [], []
    if state:
        conditions.append("auditee_state = ?"); params.append(state.upper())
    if audit_year is not None:
        conditions.append("audit_year = ?"); params.append(audit_year)
    if ein:
        conditions.append("auditee_ein = ?"); params.append(ein)
    if entity_type:
        conditions.append("entity_type = ?"); params.append(entity_type)
    if has_findings:
        conditions.append("(is_material_weakness = 1 OR is_material_noncompliance = 1)")
    if is_going_concern:
        conditions.append("is_going_concern = 1")
    where = "WHERE " + " AND ".join(conditions) if conditions else ""
    limit_clause = f"LIMIT {int(limit)}" if limit else ""
    query = f"""
        SELECT report_id, auditee_ein, auditee_uei, auditee_name, entity_type,
               auditee_city, auditee_state, auditee_zip,
               auditee_contact_name, auditee_email, auditee_phone,
               audit_year, fy_start_date, fy_end_date,
               total_amount_expended, gaap_results,
               is_going_concern, is_material_weakness,
               is_significant_deficiency, is_material_noncompliance,
               is_low_risk_auditee, auditor_firm_name,
               submitted_date, fac_accepted_date
        FROM federal_audits {where}
        ORDER BY audit_year DESC, auditee_state, auditee_name
        {limit_clause}
    """
    conn = get_connection()
    try:
        df = pd.read_sql_query(adapt_sql(query), conn, params=params)
    except Exception:
        df = pd.DataFrame()
    conn.close()
    return df


def get_federal_audit_by_id(report_id):
    """Return full detail for a single audit by report_id."""
    conn = get_connection()
    try:
        df = pd.read_sql_query(
            adapt_sql("SELECT * FROM federal_audits WHERE report_id = ?"),
            conn, params=[report_id],
        )
    except Exception:
        df = pd.DataFrame()
    conn.close()
    if df.empty:
        return None
    return df.iloc[0].to_dict()


def get_federal_audit_programs(report_id):
    """Return program-level line items for a single audit."""
    conn = get_connection()
    try:
        df = pd.read_sql_query(
            adapt_sql("""
                SELECT award_reference, aln, federal_program_name,
                       amount_expended, federal_program_total,
                       is_major, is_loan, loan_balance,
                       is_direct, is_passthrough_award, passthrough_amount,
                       cluster_name, findings_count, audit_report_type
                FROM federal_audit_programs
                WHERE report_id = ?
                ORDER BY amount_expended DESC
            """),
            conn, params=[report_id],
        )
    except Exception:
        df = pd.DataFrame()
    conn.close()
    return df


# ---------------------------------------------------------------------------
# Head Start PIR
# ---------------------------------------------------------------------------

def get_headstart_programs(state=None, program_type=None, pir_year=None,
                           grantee_name=None, zip_code=None,
                           census_tract_id=None, limit=500):
    """
    Return Head Start / Early Head Start programs.

    Args:
        state:           2-letter state abbreviation
        program_type:    'HS', 'EHS', 'Migrant', 'AIAN'
        pir_year:        PIR reporting year
        grantee_name:    substring match on grantee name
        zip_code:        ZIP code filter
        census_tract_id: census tract filter
        limit:           max rows
    """
    conditions, params = [], []
    if state:
        conditions.append("state = ?"); params.append(state.upper())
    if program_type:
        conditions.append("program_type = ?"); params.append(program_type)
    if pir_year is not None:
        conditions.append("pir_year = ?"); params.append(pir_year)
    if grantee_name:
        conditions.append("grantee_name LIKE ?"); params.append(f"%{grantee_name}%")
    if zip_code:
        conditions.append("zip_code = ?"); params.append(zip_code)
    if census_tract_id:
        conditions.append("census_tract_id = ?"); params.append(census_tract_id)
    where = "WHERE " + " AND ".join(conditions) if conditions else ""
    limit_clause = f"LIMIT {int(limit)}" if limit else ""
    query = f"""
        SELECT grant_number, program_number, pir_year,
               region, state, program_type, grantee_name, program_name,
               agency_type, city, zip_code, census_tract_id,
               latitude, longitude,
               funded_enrollment, total_cumulative_enrollment,
               total_slots_center_based, total_classes,
               home_based_slots, family_child_care_slots,
               total_staff, classroom_teachers,
               children_with_insurance_end, children_no_insurance_start,
               children_at_fqhc_start, child_care_partners, leas_in_service_area
        FROM headstart_programs {where}
        ORDER BY state, grantee_name, pir_year DESC
        {limit_clause}
    """
    conn = get_connection()
    try:
        df = pd.read_sql_query(adapt_sql(query), conn, params=params)
    except Exception:
        df = pd.DataFrame()
    conn.close()
    return df


def get_headstart_by_id(grant_number, program_number, pir_year):
    """Return full detail for a single Head Start program."""
    conn = get_connection()
    try:
        df = pd.read_sql_query(
            adapt_sql("""
                SELECT * FROM headstart_programs
                WHERE grant_number = ? AND program_number = ? AND pir_year = ?
            """),
            conn, params=[grant_number, program_number, pir_year],
        )
    except Exception:
        df = pd.DataFrame()
    conn.close()
    if df.empty:
        return None
    return df.iloc[0].to_dict()


# ---------------------------------------------------------------------------
# Tear sheet — aggregated data for a single school PDF report
# ---------------------------------------------------------------------------

def get_school_tearsheet_data(nces_id: str) -> dict:
    """
    Return all data needed to render a one-page tear sheet for a school.

    Queries multiple tables and returns a single dict with keys:
      school, enrollment_history, demographics, accountability,
      cpf_scores, financials, nearby_schools, census_tract
    Each section degrades gracefully if data is missing (returns None or empty list).
    """
    from utils.geo import filter_by_radius
    import sqlite3 as _sqlite3

    conn = get_connection()
    conn.row_factory = _sqlite3.Row
    cur = conn.cursor()

    result = {}

    # --- School record ---
    cur.execute("SELECT * FROM schools WHERE nces_id = ?", (nces_id,))
    row = cur.fetchone()
    if not row:
        conn.close()
        return None  # School not found
    school = dict(row)
    result["school"] = school

    # --- Enrollment history ---
    try:
        df = pd.read_sql_query(
            "SELECT school_year, enrollment, pct_free_reduced_lunch "
            "FROM enrollment_history WHERE nces_id = ? ORDER BY school_year ASC",
            conn, params=[nces_id],
        )
        result["enrollment_history"] = df.to_dict("records") if not df.empty else []
    except Exception:
        result["enrollment_history"] = []

    # --- Demographics (from current school record) ---
    result["demographics"] = {
        "pct_free_reduced_lunch": school.get("pct_free_reduced_lunch"),
        "pct_sped": school.get("pct_sped"),
        "pct_ell": school.get("pct_ell"),
        "pct_black": school.get("pct_black"),
        "pct_hispanic": school.get("pct_hispanic"),
        "pct_white": school.get("pct_white"),
        "pct_asian": school.get("pct_asian"),
        "pct_multiracial": school.get("pct_multiracial"),
    }

    # --- LEA accountability (proficiency + graduation) ---
    lea_id = school.get("lea_id")
    if lea_id:
        try:
            df = pd.read_sql_query(
                "SELECT * FROM lea_accountability WHERE lea_id = ? ORDER BY data_year DESC",
                conn, params=[lea_id],
            )
            result["accountability"] = df.to_dict("records") if not df.empty else []
        except Exception:
            result["accountability"] = []
    else:
        result["accountability"] = []

    # --- State and district comparison baselines ---
    try:
        cur.execute("""
            SELECT AVG(proficiency_reading) as state_avg_reading,
                   AVG(proficiency_math) as state_avg_math,
                   AVG(graduation_rate) as state_avg_graduation
            FROM lea_accountability WHERE state = ?
        """, (school.get("state", "GA"),))
        state_row = cur.fetchone()
        result["state_averages"] = dict(state_row) if state_row else {}
    except Exception:
        result["state_averages"] = {}

    # --- SCSC CPF scores (GA charter accountability) ---
    try:
        df = pd.read_sql_query(
            "SELECT school_year, academic_designation, financial_designation, "
            "operations_score, operations_designation "
            "FROM scsc_cpf WHERE nces_id = ? ORDER BY school_year DESC",
            conn, params=[nces_id],
        )
        result["cpf_scores"] = df.to_dict("records") if not df.empty else []
    except Exception:
        result["cpf_scores"] = []

    # --- Financial data (990 + ratios) ---
    ein = school.get("ein")
    if ein:
        try:
            df = pd.read_sql_query(
                "SELECT tax_year, total_revenue, total_expenses, total_assets, "
                "total_liabilities, unrestricted_net_assets, cash_savings "
                "FROM irs_990_history WHERE ein = ? ORDER BY tax_year DESC",
                conn, params=[ein],
            )
            result["financials_990"] = df.to_dict("records") if not df.empty else []
        except Exception:
            result["financials_990"] = []

        try:
            df = pd.read_sql_query(
                "SELECT * FROM financial_ratios WHERE ein = ? ORDER BY fiscal_year DESC LIMIT 1",
                conn, params=[ein],
            )
            result["financial_ratios"] = df.iloc[0].to_dict() if not df.empty else {}
        except Exception:
            result["financial_ratios"] = {}
    else:
        result["financials_990"] = []
        result["financial_ratios"] = {}

    # --- Nearby schools (within 10 miles) ---
    lat, lon = school.get("latitude"), school.get("longitude")
    if lat and lon:
        try:
            all_schools = pd.read_sql_query(
                "SELECT nces_id, school_name, enrollment, is_charter, "
                "pct_free_reduced_lunch, pct_black, pct_hispanic, pct_white, "
                "latitude, longitude FROM schools "
                "WHERE state = ? AND school_status = 'Open' AND nces_id != ?",
                conn, params=[school.get("state", "GA"), nces_id],
            )
            if not all_schools.empty:
                nearby = filter_by_radius(all_schools, lat, lon, radius_miles=10.0)
                if not nearby.empty:
                    nearby = nearby.nsmallest(8, "distance_miles")
                    nearby_list = nearby.to_dict("records")
                    for ns in nearby_list:
                        ns_nces = ns.get("nces_id")
                        if ns_nces:
                            cur.execute(
                                "SELECT s.lea_id FROM schools s WHERE s.nces_id = ?",
                                (ns_nces,),
                            )
                            ns_row = cur.fetchone()
                            if ns_row:
                                ns_lea_id = dict(ns_row).get("lea_id")
                                if ns_lea_id:
                                    cur.execute(
                                        "SELECT accountability_rating, graduation_rate "
                                        "FROM lea_accountability WHERE lea_id = ? "
                                        "ORDER BY data_year DESC LIMIT 1",
                                        (ns_lea_id,),
                                    )
                                    acct = cur.fetchone()
                                    if acct:
                                        acct_d = dict(acct)
                                        ns["accountability_rating"] = acct_d.get("accountability_rating")
                                        ns["graduation_rate"] = acct_d.get("graduation_rate")
                    result["nearby_schools"] = nearby_list
                else:
                    result["nearby_schools"] = []
            else:
                result["nearby_schools"] = []
        except Exception:
            result["nearby_schools"] = []
    else:
        result["nearby_schools"] = []

    # --- Census tract context ---
    tract_id = school.get("census_tract_id")
    if tract_id:
        try:
            cur.execute(
                "SELECT total_population, median_household_income, poverty_rate, "
                "pct_minority, unemployment_rate, is_nmtc_eligible, "
                "nmtc_eligibility_tier, is_opportunity_zone "
                "FROM census_tracts WHERE census_tract_id = ?",
                (tract_id,),
            )
            tract_row = cur.fetchone()
            result["census_tract"] = dict(tract_row) if tract_row else {}
        except Exception:
            result["census_tract"] = {}
    else:
        result["census_tract"] = {}

    conn.close()
    return result
