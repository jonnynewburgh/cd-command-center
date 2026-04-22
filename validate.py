"""
validate.py — Post-pipeline data validation report.

Checks that each table has reasonable data: row counts, null rates on critical
fields, value ranges, and referential integrity. Run this after any pipeline
to confirm the output looks correct before trusting the data.

Usage:
    python validate.py                  # check all tables
    python validate.py census           # check only census_tracts
    python validate.py schools fqhc     # check multiple tables
    python validate.py --strict         # exit with code 1 if any issues found

The report prints to the console. When run in GitHub Actions, it appears in
the Action log so you can see data quality at a glance.
"""

import sys
import io
import argparse
import db

# Windows consoles default to cp1252 which can't print some Unicode characters.
# Wrap stdout in utf-8 so the report prints cleanly on all platforms.
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

issues = []   # collected across all checks; used for --strict exit code

def _conn():
    return db.get_connection()

def _scalar(sql, params=None):
    """Run a query that returns a single value."""
    conn = _conn()
    cur = conn.cursor()
    cur.execute(db.adapt_sql(sql), params or [])
    row = cur.fetchone()
    conn.close()
    return row[0] if row else None

def ok(msg):
    print(f"  OK   {msg}")

def info(msg):
    """Informational note — not counted as an issue (used for known optional pipeline steps)."""
    print(f"  INFO {msg}")

def warn(msg):
    print(f"  WARN {msg}")
    issues.append(msg)

def fail(msg):
    print(f"  FAIL {msg}")
    issues.append(msg)

def section(title):
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}")

def check_row_count(table, min_rows=1, label=None):
    label = label or table
    n = _scalar(f"SELECT COUNT(*) FROM {table}")
    if n is None:
        fail(f"{label}: table does not exist or query failed")
    elif n == 0:
        warn(f"{label}: 0 rows — pipeline may not have run")
    elif n < min_rows:
        warn(f"{label}: only {n:,} rows (expected at least {min_rows:,})")
    else:
        ok(f"{label}: {n:,} rows")
    return n or 0

def check_null_rate(table, col, max_pct=0.10, label=None):
    """Warn if more than max_pct of rows have NULL in col."""
    label = label or f"{table}.{col}"
    total = _scalar(f"SELECT COUNT(*) FROM {table}")
    if not total:
        return
    nulls = _scalar(f"SELECT COUNT(*) FROM {table} WHERE {col} IS NULL")
    pct = nulls / total
    if pct > max_pct:
        warn(f"{label}: {pct:.0%} null ({nulls:,}/{total:,} rows)")
    else:
        ok(f"{label}: {pct:.0%} null ({nulls:,}/{total:,} rows)")

def check_value_range(table, col, min_val=None, max_val=None, label=None):
    """Warn if any values fall outside the expected range."""
    label = label or f"{table}.{col}"
    if min_val is not None:
        bad = _scalar(f"SELECT COUNT(*) FROM {table} WHERE {col} IS NOT NULL AND {col} < ?", [min_val])
        if bad:
            warn(f"{label}: {bad:,} values below {min_val}")
        else:
            ok(f"{label}: all values >= {min_val}")
    if max_val is not None:
        bad = _scalar(f"SELECT COUNT(*) FROM {table} WHERE {col} IS NOT NULL AND {col} > ?", [max_val])
        if bad:
            warn(f"{label}: {bad:,} values above {max_val}")
        else:
            ok(f"{label}: all values <= {max_val}")

def check_foreign_key(child_table, child_col, parent_table, parent_col, label=None):
    """Warn if child rows reference values that don't exist in the parent table."""
    label = label or f"{child_table}.{child_col} -> {parent_table}.{parent_col}"
    orphans = _scalar(f"""
        SELECT COUNT(*) FROM {child_table}
        WHERE {child_col} IS NOT NULL
          AND {child_col} NOT IN (SELECT {parent_col} FROM {parent_table})
    """)
    if orphans:
        warn(f"{label}: {orphans:,} orphan rows (no matching {parent_col})")
    else:
        ok(f"{label}: referential integrity OK")

def check_geo(table, lat_col="latitude", lon_col="longitude"):
    """Check that lat/lng are present and plausible for the US and territories."""
    check_null_rate(table, lat_col, max_pct=0.05)
    check_null_rate(table, lon_col, max_pct=0.05)
    # Bounds cover US states, territories, and Compact of Free Association nations:
    #   lat: -15 (American Samoa ~-14.3) to 72 (northern Alaska)
    #   lon: -180 to 170 (Marshall Islands ~166.4, CNMI ~145.7)
    check_value_range(table, lat_col, min_val=-15.0, max_val=72.0)
    check_value_range(table, lon_col, min_val=-180.0, max_val=170.0)

def check_census_tract_format(table, col="census_tract_id"):
    """Census tract IDs must be exactly 11 digits."""
    bad = _scalar(f"""
        SELECT COUNT(*) FROM {table}
        WHERE {col} IS NOT NULL
          AND (LENGTH({col}) != 11 OR {col} GLOB '*[^0-9]*')
    """)
    if bad:
        warn(f"{table}.{col}: {bad:,} rows with invalid format (expected 11-digit FIPS)")
    else:
        ok(f"{table}.{col}: all non-null values are valid 11-digit FIPS codes")

# ---------------------------------------------------------------------------
# Per-table checks
# ---------------------------------------------------------------------------

def check_census():
    section("census_tracts")
    n = check_row_count("census_tracts", min_rows=70000)  # ~74k tracts nationally
    if n == 0:
        return

    # Core ACS variables — these are always populated by load_census_tracts.py
    check_null_rate("census_tracts", "total_population", max_pct=0.01)
    check_null_rate("census_tracts", "poverty_rate", max_pct=0.05)
    check_null_rate("census_tracts", "median_household_income", max_pct=0.05)
    check_null_rate("census_tracts", "unemployment_rate", max_pct=0.05)
    check_null_rate("census_tracts", "median_family_income", max_pct=0.05)

    # Gap-analysis population fields — always fetched alongside core ACS variables
    check_null_rate("census_tracts", "pop_under_5", max_pct=0.01)
    check_null_rate("census_tracts", "pop_under_18", max_pct=0.01)

    # poverty_rate is stored as a percentage (e.g. 15.3 = 15.3%), not a decimal
    check_value_range("census_tracts", "poverty_rate", min_val=0.0, max_val=100.0)
    check_value_range("census_tracts", "unemployment_rate", min_val=0.0, max_val=100.0)
    check_value_range("census_tracts", "median_household_income", min_val=0)
    check_value_range("census_tracts", "median_family_income", min_val=0)
    check_value_range("census_tracts", "total_population", min_val=0)

    # NMTC eligibility — tier must be set whenever eligibility flags are set
    check_null_rate("census_tracts", "nmtc_eligibility_tier", max_pct=0.001)

    # Consistency: is_nmtc_eligible and nmtc_eligibility_tier must agree
    bad_eligible = _scalar("""
        SELECT COUNT(*) FROM census_tracts
        WHERE is_nmtc_eligible = 1
          AND (nmtc_eligibility_tier IS NULL OR nmtc_eligibility_tier = 'Not Eligible')
    """)
    if bad_eligible:
        warn(f"census_tracts: {bad_eligible:,} rows where is_nmtc_eligible=1 but tier is NULL/Not Eligible")
    else:
        ok("census_tracts: is_nmtc_eligible / nmtc_eligibility_tier are consistent")

    bad_ineligible = _scalar("""
        SELECT COUNT(*) FROM census_tracts
        WHERE is_nmtc_eligible = 0
          AND nmtc_eligibility_tier IN ('LIC', 'Severely Distressed', 'Deep Distress')
    """)
    if bad_ineligible:
        warn(f"census_tracts: {bad_ineligible:,} rows where is_nmtc_eligible=0 but tier is LIC or better")
    else:
        ok("census_tracts: is_nmtc_eligible=0 rows have consistent tier (Not Eligible)")

    # Spot-check: at least some tracts should be NMTC-eligible
    eligible = _scalar("SELECT COUNT(*) FROM census_tracts WHERE is_nmtc_eligible = 1")
    if eligible and eligible > 0:
        ok(f"census_tracts: {eligible:,} NMTC-eligible tracts")
    else:
        warn("census_tracts: 0 NMTC-eligible tracts — eligibility flags may not be set")

    # NMTC tier distribution sanity check (LIC-or-better should be 20–35% of all tracts)
    total = _scalar("SELECT COUNT(*) FROM census_tracts")
    lic_plus = _scalar("SELECT COUNT(*) FROM census_tracts WHERE nmtc_eligibility_tier != 'Not Eligible'")
    if total and lic_plus is not None:
        pct = lic_plus / total
        if 0.15 < pct < 0.45:
            ok(f"census_tracts: {lic_plus:,} tracts LIC-or-better ({pct:.0%} of total)")
        else:
            warn(f"census_tracts: {lic_plus:,} tracts LIC-or-better ({pct:.0%}) — expected 15–45%")

    # census_tract_id must be exactly 11 digits
    check_census_tract_format("census_tracts", col="census_tract_id")

    # These columns are populated by the main ACS load (load_census_tracts.py)
    check_null_rate("census_tracts", "pct_minority", max_pct=0.05)
    check_value_range("census_tracts", "pct_minority", min_val=0.0, max_val=100.0)
    check_null_rate("census_tracts", "county_name", max_pct=0.01)
    check_null_rate("census_tracts", "pop_uninsured", max_pct=0.05)
    check_value_range("census_tracts", "pop_uninsured", min_val=0)
    check_null_rate("census_tracts", "pop_65_plus", max_pct=0.05)
    check_value_range("census_tracts", "pop_65_plus", min_val=0)

    # EJScreen columns: separate opt-in ETL step (load_ejscreen.py + manual file download)
    ej_null = _scalar("SELECT COUNT(*) FROM census_tracts WHERE ej_index IS NULL")
    if ej_null == n:
        info("census_tracts.ej_index: not yet loaded — run etl/load_ejscreen.py with EPA CSV to populate EJScreen indicators")
    else:
        ok(f"census_tracts.ej_index: populated for {n - ej_null:,} tracts")

    # Historical change columns: separate opt-in step (load_census_tracts.py --historical)
    hist_null = _scalar("SELECT COUNT(*) FROM census_tracts WHERE poverty_rate_5yr_ago IS NULL")
    if hist_null == n:
        info("census_tracts.poverty_rate_5yr_ago: not yet loaded — run load_census_tracts.py --historical to populate 5-year trend columns")
    else:
        ok(f"census_tracts.poverty_rate_5yr_ago: populated for {n - hist_null:,} tracts")

def check_schools():
    section("schools")
    n = check_row_count("schools", min_rows=50000)  # NCES: ~100k public schools
    if n == 0:
        return
    check_null_rate("schools", "nces_id", max_pct=0.0)
    check_null_rate("schools", "school_name", max_pct=0.0)
    check_null_rate("schools", "state", max_pct=0.01)
    # census_tract_id is assigned by assign_census_tracts.py — not run for all states yet
    ct_null = _scalar("SELECT COUNT(*) FROM schools WHERE census_tract_id IS NULL")
    ct_total = _scalar("SELECT COUNT(*) FROM schools")
    ct_pct = ct_null / ct_total if ct_total else 0
    if ct_pct > 0.20:
        info(f"schools.census_tract_id: {ct_pct:.0%} null ({ct_null:,}/{ct_total:,} rows) — run assign_census_tracts.py to populate")
    else:
        check_null_rate("schools", "census_tract_id", max_pct=0.15)
    check_geo("schools")
    check_census_tract_format("schools")
    check_value_range("schools", "enrollment", min_val=0)
    # pct_free_reduced_lunch is stored as a percentage (e.g. 75.3 = 75.3%), not a decimal
    check_value_range("schools", "pct_free_reduced_lunch", min_val=0.0, max_val=100.0)
    # Known: GU, MP, AS, VI territory tracts are not in our census_tracts table (~131 expected)
    orphans = _scalar("""
        SELECT COUNT(*) FROM schools s
        LEFT JOIN census_tracts ct ON s.census_tract_id = ct.census_tract_id
        WHERE s.census_tract_id IS NOT NULL AND ct.census_tract_id IS NULL
    """)
    if orphans > 200:
        warn(f"schools.census_tract_id -> census_tracts: {orphans:,} orphan rows (expected ~131 for GU/MP/AS/VI territories)")
    else:
        ok(f"schools.census_tract_id: {orphans} unmatched rows (within expected range for territory tracts)")
    # Charters should be a fraction of total
    charters = _scalar("SELECT COUNT(*) FROM schools WHERE is_charter = 1")
    pct = charters / n if n else 0
    if 0.02 < pct < 0.25:
        ok(f"schools: {charters:,} charters ({pct:.0%} of total)")
    else:
        warn(f"schools: {charters:,} charters ({pct:.0%} of total) — expected 2–25%")

def check_fqhc():
    section("fqhc")
    n = check_row_count("fqhc", min_rows=1000)  # HRSA: ~1,400 health center orgs, many sites
    if n == 0:
        return
    check_null_rate("fqhc", "bhcmis_id", max_pct=0.0)
    check_null_rate("fqhc", "health_center_name", max_pct=0.0)
    check_null_rate("fqhc", "state", max_pct=0.01)
    check_null_rate("fqhc", "census_tract_id", max_pct=0.15)
    check_geo("fqhc")
    check_census_tract_format("fqhc")
    # Known: ~469 CT sites use pre-2022 county FIPS (09001-09015); our ACS table uses
    # the new CT planning-region FIPS (09110-09190). Also ~20 territory sites (AS/VI/GU/MP).
    # Using a count-based threshold instead of check_foreign_key.
    orphans = _scalar("""
        SELECT COUNT(*) FROM fqhc f
        LEFT JOIN census_tracts ct ON f.census_tract_id = ct.census_tract_id
        WHERE f.census_tract_id IS NOT NULL AND ct.census_tract_id IS NULL
    """)
    if orphans > 600:
        warn(f"fqhc.census_tract_id -> census_tracts: {orphans:,} orphan rows (expected ~490 due to CT county restructuring + territories)")
    else:
        ok(f"fqhc.census_tract_id: {orphans} unmatched rows (within expected range for CT restructuring + territories)")
    active = _scalar("SELECT COUNT(*) FROM fqhc WHERE is_active = 1")
    ok(f"fqhc: {active:,} active sites")

def check_ece():
    section("ece_centers")
    n = check_row_count("ece_centers", min_rows=100)
    if n == 0:
        return
    check_null_rate("ece_centers", "provider_name", max_pct=0.0)
    check_null_rate("ece_centers", "state", max_pct=0.01)
    # ~1,305 ECE centers have no address and cannot be geocoded; ~30% null expected
    check_null_rate("ece_centers", "census_tract_id", max_pct=0.40)
    check_null_rate("ece_centers", "latitude", max_pct=0.40)
    check_null_rate("ece_centers", "longitude", max_pct=0.40)
    check_value_range("ece_centers", "latitude", min_val=-15.0, max_val=72.0)
    check_value_range("ece_centers", "longitude", min_val=-180.0, max_val=170.0)
    check_value_range("ece_centers", "capacity", min_val=1)

def check_nmtc():
    section("nmtc_projects")
    n = check_row_count("nmtc_projects", min_rows=5000)
    if n == 0:
        return
    check_null_rate("nmtc_projects", "state", max_pct=0.02)
    check_null_rate("nmtc_projects", "qlici_amount", max_pct=0.05)
    check_null_rate("nmtc_projects", "census_tract_id", max_pct=0.20)
    check_geo("nmtc_projects")
    check_value_range("nmtc_projects", "qlici_amount", min_val=0)
    # Known issue: NMTC public data uses 2010 census tract definitions; census_tracts
    # holds 2020 ACS data. CT had boundary changes between censuses, leaving ~41 CT
    # tracts with no match. Using a count-based check instead of check_foreign_key.
    orphans = _scalar("""
        SELECT COUNT(*) FROM nmtc_projects n
        LEFT JOIN census_tracts ct ON n.census_tract_id = ct.census_tract_id
        WHERE ct.census_tract_id IS NULL
    """)
    if orphans > 50:
        warn(f"nmtc_projects: {orphans} rows with no matching census tract (expected <=50 due to 2010/2020 vintage mismatch)")
    else:
        ok(f"nmtc_projects.census_tract_id: {orphans} unmatched rows (within expected range for 2010/2020 vintage mismatch)")

    section("cde_allocations")
    check_row_count("cde_allocations", min_rows=100)

def check_990():
    section("irs_990")
    n = check_row_count("irs_990", min_rows=100)
    if n == 0:
        return
    check_null_rate("irs_990", "ein", max_pct=0.0)
    check_null_rate("irs_990", "org_name", max_pct=0.01)
    check_null_rate("irs_990", "tax_year", max_pct=0.05)
    check_value_range("irs_990", "total_revenue", min_val=0)
    check_value_range("irs_990", "total_assets", min_val=0)

    section("irs_990_history")
    check_row_count("irs_990_history", min_rows=100)

def check_lea():
    section("lea_accountability")
    check_row_count("lea_accountability", min_rows=1000)
    check_null_rate("lea_accountability", "lea_id", max_pct=0.0)
    check_null_rate("lea_accountability", "state", max_pct=0.01)
    check_foreign_key("lea_accountability", "lea_id", "schools", "lea_id")

def check_data_loads():
    section("data_loads (pipeline run history)")
    n = check_row_count("data_loads", min_rows=1)
    if n == 0:
        return
    recent = _scalar("""
        SELECT COUNT(*) FROM data_loads
        WHERE status = 'error'
          AND started_at > datetime('now', '-7 days')
    """)
    if recent:
        warn(f"data_loads: {recent} pipeline runs ended in error in the last 7 days")
    else:
        ok("data_loads: no errors in the last 7 days")
    last = _scalar("SELECT pipeline || ' (' || status || ') at ' || started_at FROM data_loads ORDER BY id DESC LIMIT 1")
    if last:
        ok(f"data_loads: most recent run — {last}")

# ---------------------------------------------------------------------------
# Table registry
# ---------------------------------------------------------------------------

TABLE_CHECKS = {
    "census":    check_census,
    "schools":   check_schools,
    "fqhc":      check_fqhc,
    "ece":       check_ece,
    "nmtc":      check_nmtc,
    "990":       check_990,
    "lea":       check_lea,
    "loads":     check_data_loads,
}

# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Validate CD Command Center database")
    parser.add_argument(
        "tables", nargs="*",
        help=f"Tables to check: {', '.join(TABLE_CHECKS)}. Omit to check all.",
    )
    parser.add_argument(
        "--strict", action="store_true",
        help="Exit with code 1 if any warnings or failures are found (for CI).",
    )
    args = parser.parse_args()

    targets = args.tables if args.tables else list(TABLE_CHECKS.keys())
    unknown = [t for t in targets if t not in TABLE_CHECKS]
    if unknown:
        print(f"Unknown table(s): {', '.join(unknown)}")
        print(f"Valid options: {', '.join(TABLE_CHECKS)}")
        sys.exit(1)

    print("\nCD Command Center — Data Validation Report")
    print(f"Database: {db.DATABASE_URL}")

    # Initialize DB so tables exist even on a fresh database
    try:
        db.init_db()
    except Exception as e:
        print(f"\nFAIL: Could not connect to database: {e}")
        sys.exit(1)

    for target in targets:
        try:
            TABLE_CHECKS[target]()
        except Exception as e:
            fail(f"{target}: unexpected error — {e}")

    # Summary
    print(f"\n{'='*60}")
    if issues:
        print(f"  {len(issues)} issue(s) found:")
        for i in issues:
            print(f"    - {i}")
        if args.strict:
            print("\nExiting with code 1 (--strict mode)")
            sys.exit(1)
    else:
        print("  All checks passed.")
    print()


if __name__ == "__main__":
    main()
