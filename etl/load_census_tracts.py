"""
etl/load_census_tracts.py — Load real census tract demographics via Census ACS API.

Data source: US Census Bureau American Community Survey 5-Year Estimates
  https://api.census.gov/data/{year}/acs/acs5

No API key required for basic use (Census Bureau allows unauthenticated requests
with a rate limit). Get a free key at api.census.gov/data/key_signup.html and pass
it via --api-key to avoid rate limits on large runs.

Variables fetched per tract:
  B01001_001E  — Total population
  B17001_001E  — Poverty status universe (people for whom poverty is determined)
  B17001_002E  — People below poverty level
  B19013_001E  — Median household income
  B19113_001E  — Median family income (used for NMTC LIC income test)
  B23025_005E  — Civilian labor force unemployed
  B23025_003E  — Civilian labor force total

NMTC Eligibility Tiers (computed from ACS data):
  Deep Distress:       poverty_rate ≥ 40%  OR  median_family_income ≤ $35,250 (50% of ~$70,500)
  Severely Distressed: poverty_rate ≥ 30%  OR  median_family_income ≤ $42,300 (60% of ~$70,500)
  LIC (Low-Income Community): poverty_rate ≥ 20%  OR  median_family_income ≤ $56,400 (80% of ~$70,500)
  Not Eligible: does not meet any threshold

Tiers are hierarchical — a tract qualifies at the highest tier it meets.

Usage:
    python etl/load_census_tracts.py --states CA TX NY
    python etl/load_census_tracts.py --all           # all 50 states + DC
    python etl/load_census_tracts.py --states CA --api-key YOUR_KEY
    python etl/load_census_tracts.py --year 2021     # use a different ACS year
"""

import argparse
import sys
import os
import time
import requests
import pandas as pd
import numpy as np

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import db

# ---------------------------------------------------------------------------
# State FIPS codes
# ---------------------------------------------------------------------------

# Maps 2-letter abbreviation → 2-digit FIPS string (Census API uses string format)
STATE_FIPS = {
    "AL": "01", "AK": "02", "AZ": "04", "AR": "05", "CA": "06",
    "CO": "08", "CT": "09", "DE": "10", "DC": "11", "FL": "12",
    "GA": "13", "HI": "15", "ID": "16", "IL": "17", "IN": "18",
    "IA": "19", "KS": "20", "KY": "21", "LA": "22", "ME": "23",
    "MD": "24", "MA": "25", "MI": "26", "MN": "27", "MS": "28",
    "MO": "29", "MT": "30", "NE": "31", "NV": "32", "NH": "33",
    "NJ": "34", "NM": "35", "NY": "36", "NC": "37", "ND": "38",
    "OH": "39", "OK": "40", "OR": "41", "PA": "42", "RI": "44",
    "SC": "45", "SD": "46", "TN": "47", "TX": "48", "UT": "49",
    "VT": "50", "VA": "51", "WA": "53", "WV": "54", "WI": "55",
    "WY": "56",
    # Territories with ACS data
    "PR": "72",
}

# Reverse map: FIPS string → abbreviation
FIPS_STATE = {v: k for k, v in STATE_FIPS.items()}

# ACS variables to fetch
ACS_VARIABLES = [
    "B01001_001E",   # Total population
    "B17001_001E",   # Poverty universe (people for whom poverty is determined)
    "B17001_002E",   # People below poverty level
    "B19013_001E",   # Median household income
    "B19113_001E",   # Median family income (used for NMTC income test)
    "B23025_003E",   # Civilian labor force total
    "B23025_005E",   # Civilian labor force unemployed
    # Gap analysis variables (Phase B3)
    "B01001_003E",   # Male population under 5 years
    "B01001_027E",   # Female population under 5 years
    "B09001_001E",   # Population under 18 in households (proxy for school-age)
    "NAME",          # Tract name
]

# Historical run only needs these two (for 5-year trend columns)
ACS_HISTORICAL_VARIABLES = [
    "B17001_001E",   # Poverty universe
    "B17001_002E",   # People below poverty level
    "B19013_001E",   # Median household income
]

# National median family income (approximate, 2022 ACS)
# Used as the denominator for NMTC income-based eligibility
NATIONAL_MEDIAN_FAMILY_INCOME = 70_500

# NMTC income thresholds (% of national MFI)
NMTC_INCOME_LIC = NATIONAL_MEDIAN_FAMILY_INCOME * 0.80          # $56,400
NMTC_INCOME_SEVERELY = NATIONAL_MEDIAN_FAMILY_INCOME * 0.60     # $42,300
NMTC_INCOME_DEEP = NATIONAL_MEDIAN_FAMILY_INCOME * 0.50         # $35,250

BASE_URL = "https://api.census.gov/data"
PAGE_SLEEP = 0.5   # seconds between API calls


def classify_nmtc_tier(poverty_rate, median_family_income) -> tuple[int, str, str]:
    """
    Classify a census tract into an NMTC eligibility tier.

    Returns:
        (is_eligible, tier_label, reason)
        is_eligible: 1 or 0
        tier_label: 'Deep Distress', 'Severely Distressed', 'LIC', or 'Not Eligible'
        reason: 'Poverty', 'Income', 'Both', or ''
    """
    if poverty_rate is None and median_family_income is None:
        return 0, "Not Eligible", ""

    pov = poverty_rate or 0
    mfi = median_family_income  # may be None

    # Check criteria for each tier, highest first
    for tier, pov_threshold, income_threshold in [
        ("Deep Distress",       40.0, NMTC_INCOME_DEEP),
        ("Severely Distressed", 30.0, NMTC_INCOME_SEVERELY),
        ("LIC",                 20.0, NMTC_INCOME_LIC),
    ]:
        meets_poverty = pov >= pov_threshold
        meets_income = (mfi is not None and mfi <= income_threshold)

        if meets_poverty and meets_income:
            return 1, tier, "Both"
        elif meets_poverty:
            return 1, tier, "Poverty"
        elif meets_income:
            return 1, tier, "Income"

    return 0, "Not Eligible", ""


def fetch_state_tracts(state_fips: str, year: int, api_key: str = None) -> list[dict]:
    """
    Fetch all census tract ACS data for a single state.

    Returns a list of dicts, one per tract, with raw ACS variable values.
    """
    variables = ",".join(ACS_VARIABLES)
    url = f"{BASE_URL}/{year}/acs/acs5"
    params = {
        "get": variables,
        "for": "tract:*",
        "in": f"state:{state_fips}",
    }
    if api_key:
        params["key"] = api_key

    try:
        resp = requests.get(url, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
    except requests.RequestException as e:
        print(f"    Census API error for state {state_fips}: {e}")
        return []

    if not data or len(data) < 2:
        return []

    # First row is column headers, subsequent rows are data
    headers = data[0]
    rows = data[1:]

    results = []
    for row in rows:
        record = dict(zip(headers, row))
        results.append(record)

    return results


def parse_tract_record(raw: dict, state_abbr: str, year: int) -> dict:
    """
    Convert a raw Census API row dict into our census_tracts schema.

    Handles:
    - Building 11-digit census tract FIPS from state + county + tract parts
    - Computing poverty_rate and unemployment_rate from raw counts
    - Classifying NMTC eligibility tier
    - Treating Census sentinel values (-666666666) as None
    """

    def safe_int(val) -> int | None:
        """Parse a Census API value; return None for missing/sentinel values."""
        if val is None:
            return None
        try:
            v = int(val)
            return None if v < 0 else v   # Census uses large negatives for N/A
        except (TypeError, ValueError):
            return None

    def safe_float(val) -> float | None:
        if val is None:
            return None
        try:
            v = float(val)
            return None if v < 0 else v
        except (TypeError, ValueError):
            return None

    state_fips = raw.get("state", "")
    county_fips_short = raw.get("county", "")
    tract_code = raw.get("tract", "")

    # 11-digit census tract FIPS = state(2) + county(3) + tract(6)
    census_tract_id = f"{state_fips}{county_fips_short}{tract_code}"
    county_fips_full = f"{state_fips}{county_fips_short}"

    total_pop = safe_int(raw.get("B01001_001E"))
    pov_universe = safe_int(raw.get("B17001_001E"))
    pov_count = safe_int(raw.get("B17001_002E"))
    median_hh_income = safe_float(raw.get("B19013_001E"))
    median_fam_income = safe_float(raw.get("B19113_001E"))
    labor_total = safe_int(raw.get("B23025_003E"))
    labor_unemployed = safe_int(raw.get("B23025_005E"))

    # Compute rates
    poverty_rate = None
    if pov_universe and pov_universe > 0 and pov_count is not None:
        poverty_rate = round(pov_count / pov_universe * 100, 2)

    unemployment_rate = None
    if labor_total and labor_total > 0 and labor_unemployed is not None:
        unemployment_rate = round(labor_unemployed / labor_total * 100, 2)

    # NMTC eligibility classification
    is_eligible, tier, reason = classify_nmtc_tier(poverty_rate, median_fam_income)

    # Tract name from Census (e.g., "Census Tract 1234.56, Los Angeles County, California")
    tract_name = raw.get("NAME", "")

    # Gap analysis population variables
    pop_male_under5  = safe_int(raw.get("B01001_003E"))
    pop_fem_under5   = safe_int(raw.get("B01001_027E"))
    pop_under_18     = safe_int(raw.get("B09001_001E"))

    pop_under_5 = None
    if pop_male_under5 is not None or pop_fem_under5 is not None:
        pop_under_5 = (pop_male_under5 or 0) + (pop_fem_under5 or 0)

    return {
        "census_tract_id": census_tract_id,
        "state_fips": state_fips,
        "county_fips": county_fips_full,
        "tract_name": tract_name,
        "total_population": total_pop,
        "median_household_income": median_hh_income,
        "median_family_income": median_fam_income,
        "poverty_rate": poverty_rate,
        "pct_minority": None,           # not fetching this variable for now
        "unemployment_rate": unemployment_rate,
        "is_nmtc_eligible": is_eligible,
        "nmtc_eligibility_reason": reason,
        "nmtc_eligibility_tier": tier,
        "county_name": None,            # we have county FIPS; name can be added later
        "state": state_abbr,
        "data_year": year,
        "pop_under_5": pop_under_5,
        "pop_under_18": pop_under_18,
    }


def fetch_state_tracts_historical(state_fips: str, year: int, api_key: str = None) -> list[dict]:
    """
    Fetch only poverty + income variables for a historical year.
    Used to populate *_5yr_ago columns for tract change-over-time analysis.
    Returns list of dicts with census_tract_id, poverty_rate, median_hh_income.
    """
    variables = ",".join(ACS_HISTORICAL_VARIABLES + ["NAME"])
    url = f"{BASE_URL}/{year}/acs/acs5"
    params = {
        "get": variables,
        "for": "tract:*",
        "in": f"state:{state_fips}",
    }
    if api_key:
        params["key"] = api_key

    try:
        resp = requests.get(url, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
    except requests.RequestException as e:
        print(f"    Census API error for state {state_fips} (year {year}): {e}")
        return []

    if not data or len(data) < 2:
        return []

    headers = data[0]
    rows = data[1:]
    results = []
    for row in rows:
        raw = dict(zip(headers, row))
        state_f = raw.get("state", "")
        county_f = raw.get("county", "")
        tract_c = raw.get("tract", "")
        census_tract_id = f"{state_f}{county_f}{tract_c}"

        def safe_int(val):
            try:
                v = int(val)
                return None if v < 0 else v
            except (TypeError, ValueError):
                return None

        def safe_float(val):
            try:
                v = float(val)
                return None if v < 0 else v
            except (TypeError, ValueError):
                return None

        pov_universe = safe_int(raw.get("B17001_001E"))
        pov_count    = safe_int(raw.get("B17001_002E"))
        hh_income    = safe_float(raw.get("B19013_001E"))

        poverty_rate = None
        if pov_universe and pov_universe > 0 and pov_count is not None:
            poverty_rate = round(pov_count / pov_universe * 100, 2)

        results.append({
            "census_tract_id": census_tract_id,
            "poverty_rate_5yr_ago": poverty_rate,
            "median_income_5yr_ago": hh_income,
        })

    return results


def apply_historical_data(historical_records: list[dict]):
    """
    Update census_tracts with 5-year-ago values and compute change columns.
    Reads current poverty_rate and median_household_income to compute deltas.
    Only updates tracts that already exist in the database.
    """
    import sqlite3
    conn = db.get_connection()
    cur = conn.cursor()

    updated = 0
    for rec in historical_records:
        tid = rec["census_tract_id"]
        pov_old = rec.get("poverty_rate_5yr_ago")
        inc_old = rec.get("median_income_5yr_ago")

        # Look up current values to compute deltas
        cur.execute(
            "SELECT poverty_rate, median_household_income FROM census_tracts WHERE census_tract_id = ?",
            (tid,),
        )
        row = cur.fetchone()
        if not row:
            continue  # tract not in DB yet, skip

        pov_now, inc_now = row[0], row[1]

        # Compute delta columns
        pov_change = None
        if pov_now is not None and pov_old is not None:
            pov_change = round(pov_now - pov_old, 2)

        inc_change_pct = None
        if inc_now is not None and inc_old is not None and inc_old > 0:
            inc_change_pct = round((inc_now - inc_old) / inc_old * 100, 2)

        cur.execute(
            """UPDATE census_tracts
               SET poverty_rate_5yr_ago = ?,
                   median_income_5yr_ago = ?,
                   poverty_rate_change = ?,
                   income_change_pct = ?
               WHERE census_tract_id = ?""",
            (pov_old, inc_old, pov_change, inc_change_pct, tid),
        )
        updated += 1

    conn.commit()
    conn.close()
    return updated


def main():
    parser = argparse.ArgumentParser(
        description="Load ACS census tract demographics into SQLite"
    )
    parser.add_argument(
        "--states",
        nargs="+",
        metavar="STATE",
        help="2-letter state abbreviations (e.g. CA TX NY)",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        dest="all_states",
        help="Fetch all states + DC + Puerto Rico",
    )
    parser.add_argument(
        "--year",
        type=int,
        default=2022,
        help="ACS 5-year estimate year (default: 2022, most recent with full tract coverage)",
    )
    parser.add_argument(
        "--api-key",
        default=None,
        help="Census Bureau API key (optional; get one free at api.census.gov/data/key_signup.html)",
    )
    parser.add_argument(
        "--historical",
        action="store_true",
        help=(
            "Also load data from 5 years prior (year - 5) and compute change columns. "
            "Requires current-year data to already be loaded. Populates "
            "poverty_rate_5yr_ago, median_income_5yr_ago, poverty_rate_change, income_change_pct."
        ),
    )
    args = parser.parse_args()

    if not args.states and not args.all_states:
        parser.error("Specify --states CA TX ... or --all")

    if args.all_states:
        states_to_fetch = STATE_FIPS
    else:
        unknown = [s for s in args.states if s.upper() not in STATE_FIPS]
        if unknown:
            print(f"Error: unknown state(s): {', '.join(unknown)}")
            sys.exit(1)
        states_to_fetch = {s.upper(): STATE_FIPS[s.upper()] for s in args.states}

    print(f"CD Command Center — Census Tract ACS Data Load")
    print(f"  ACS year: {args.year} (5-year estimates)")
    print(f"  States: {len(states_to_fetch)} ({', '.join(states_to_fetch.keys())})")
    print(f"  API key: {'provided' if args.api_key else 'not provided (rate limited)'}")
    print()

    db.init_db()

    total_loaded = 0
    total_errors = 0

    for abbr, fips in states_to_fetch.items():
        print(f"  Fetching {abbr}...", end="", flush=True)
        raw_records = fetch_state_tracts(fips, args.year, args.api_key)

        if not raw_records:
            print(f" no data returned, skipping")
            continue

        print(f" {len(raw_records):,} tracts", end="", flush=True)

        loaded = 0
        errors = 0
        for raw in raw_records:
            try:
                record = parse_tract_record(raw, abbr, args.year)
                db.upsert_census_tract(record)
                loaded += 1
            except Exception as e:
                errors += 1

        total_loaded += loaded
        total_errors += errors
        print(f" → {loaded:,} loaded" + (f", {errors} errors" if errors else ""))
        time.sleep(PAGE_SLEEP)

    print()
    print(f"Census tract load complete.")
    print(f"  Total loaded: {total_loaded:,}")
    if total_errors:
        print(f"  Errors: {total_errors:,}")

    # Show NMTC summary
    summary = db.get_census_tract_summary()
    print()
    print(f"Database now contains:")
    print(f"  Total tracts:         {summary.get('total_tracts', 0):,}")
    print(f"  LIC or better:        {summary.get('eligible_tracts', 0):,}")
    print(f"  Severely Distressed:  {summary.get('severely_distressed', 0):,}")
    print(f"  Deep Distress:        {summary.get('deep_distress', 0):,}")

    # --- Historical mode: load 5-years-ago data and compute change columns ---
    if args.historical:
        historical_year = args.year - 5
        print()
        print(f"Loading historical ACS data for year {historical_year} (5 years prior)...")

        total_historical = 0
        for abbr, fips in states_to_fetch.items():
            print(f"  Fetching {abbr} ({historical_year})...", end="", flush=True)
            hist_records = fetch_state_tracts_historical(fips, historical_year, args.api_key)
            if not hist_records:
                print(" no data returned, skipping")
                continue
            updated = apply_historical_data(hist_records)
            total_historical += updated
            print(f" {updated:,} tracts updated with historical data")
            time.sleep(PAGE_SLEEP)

        print(f"Historical data load complete. {total_historical:,} tracts updated with 5-year trend.")


if __name__ == "__main__":
    main()
