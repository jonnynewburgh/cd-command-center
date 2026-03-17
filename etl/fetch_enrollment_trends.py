"""
etl/fetch_enrollment_trends.py — Load historical enrollment data for schools.

Uses the Urban Institute Education Data API, the same source as fetch_nces_schools.py.
Fetches annual enrollment by school NCES ID for the last N years and stores it
in the enrollment_history table for sparkline trend charts in the dashboard.

API docs: https://educationdata.urban.org/documentation/

WHY this data:
  Enrollment trends are a key financial risk indicator for charter schools.
  A school losing enrollment year-over-year may be facing closure risk even
  if its most recent 990 looks fine. This data complements the survival model.

Usage:
    python etl/fetch_enrollment_trends.py                      # all schools in DB, 5 years
    python etl/fetch_enrollment_trends.py --states CA TX       # specific states
    python etl/fetch_enrollment_trends.py --years 8            # up to 8 years of history
    python etl/fetch_enrollment_trends.py --limit 200          # test on 200 schools
    python etl/fetch_enrollment_trends.py --charter-only       # charter schools only
"""

import argparse
import sys
import os
import time

import requests
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import db

# Urban Institute Education Data API base URL
ED_DATA_URL = "https://educationdata.urban.org/api/v1/schools/ccd/enrollment/{year}/grade-pk/"

# We fetch data going back this many school years from the most recent available
DEFAULT_YEARS = 5

# Most recent school year typically available in the API (update annually)
MOST_RECENT_YEAR = 2022   # SY 2021-22 is usually the most recent

# Seconds to sleep between API pages to be polite
API_SLEEP = 0.2


def fetch_enrollment_for_year(year: int, nces_ids: list, verbose: bool = False) -> list:
    """
    Fetch enrollment data for a list of NCES school IDs for a given school year.
    The API returns one row per school per grade — we aggregate to total enrollment.

    Returns a list of dicts: {nces_id, school_year, enrollment}
    """
    if not nces_ids:
        return []

    # The API supports filtering by ncessch (NCES school ID) but only as a
    # single value per request. For bulk fetches we use state-level queries
    # and filter locally — much faster than one request per school.
    # We'll fetch by state instead, then filter to our known NCES IDs.
    # Since we already have the schools in our DB, we pass the list directly.

    # Split into chunks of 100 to stay within URL length limits
    results = []
    nces_set = set(str(n) for n in nces_ids)

    # Fetch all schools for this year from the API (paginated)
    page = 1
    while True:
        try:
            resp = requests.get(
                ED_DATA_URL.format(year=year),
                params={"page": page, "per_page": 5000},
                timeout=30,
            )
            if resp.status_code != 200:
                if verbose:
                    print(f"    API error {resp.status_code} for year {year} page {page}")
                break

            data = resp.json()
            records = data.get("results", [])
            if not records:
                break

            # Aggregate enrollment by school (sum across grades)
            for rec in records:
                nces_id = str(rec.get("ncessch", "")).zfill(12)
                if nces_id not in nces_set:
                    continue
                enrollment = rec.get("enrollment") or 0
                # Find or create running total for this school
                existing = next((r for r in results if r["nces_id"] == nces_id), None)
                if existing:
                    existing["enrollment"] += enrollment
                else:
                    results.append({
                        "nces_id":      nces_id,
                        "school_year":  year,
                        "enrollment":   enrollment,
                    })

            count = data.get("count", 0)
            fetched = page * 5000
            if verbose:
                print(f"    Year {year} page {page}: {len(records)} records, {len(results)} matched so far")

            if fetched >= count:
                break
            page += 1
            time.sleep(API_SLEEP)

        except Exception as e:
            if verbose:
                print(f"    Error fetching year {year} page {page}: {e}")
            break

    return results


def fetch_enrollment_by_state(year: int, state: str, verbose: bool = False) -> pd.DataFrame:
    """
    Fetch all school enrollment for a state and year from the API.
    Returns a DataFrame with ncessch, enrollment (aggregated across grades).
    """
    # Use a different endpoint that accepts state filter
    url = f"https://educationdata.urban.org/api/v1/schools/ccd/enrollment/{year}/grade-pk/"
    all_records = []
    page = 1

    while True:
        try:
            resp = requests.get(
                url,
                params={"fips": _state_to_fips(state), "page": page, "per_page": 5000},
                timeout=30,
            )
            if resp.status_code != 200:
                break

            data = resp.json()
            records = data.get("results", [])
            if not records:
                break

            all_records.extend(records)
            count = data.get("count", 0)
            if verbose:
                print(f"      {state} year {year} page {page}: {len(records)} rows, {count} total")

            if page * 5000 >= count:
                break
            page += 1
            time.sleep(API_SLEEP)

        except Exception as e:
            if verbose:
                print(f"      Error: {e}")
            break

    if not all_records:
        return pd.DataFrame()

    df = pd.DataFrame(all_records)
    if "ncessch" not in df.columns or "enrollment" not in df.columns:
        return pd.DataFrame()

    # Aggregate enrollment across grades for each school
    df["ncessch"] = df["ncessch"].astype(str).str.zfill(12)
    df["enrollment"] = pd.to_numeric(df["enrollment"], errors="coerce").fillna(0)
    agg = df.groupby("ncessch")["enrollment"].sum().reset_index()
    agg.columns = ["nces_id", "enrollment"]
    agg["school_year"] = year
    return agg


# FIPS codes for US states (needed for state-level API filter)
_STATE_FIPS = {
    "AL": 1, "AK": 2, "AZ": 4, "AR": 5, "CA": 6, "CO": 8, "CT": 9, "DE": 10,
    "DC": 11, "FL": 12, "GA": 13, "HI": 15, "ID": 16, "IL": 17, "IN": 18,
    "IA": 19, "KS": 20, "KY": 21, "LA": 22, "ME": 23, "MD": 24, "MA": 25,
    "MI": 26, "MN": 27, "MS": 28, "MO": 29, "MT": 30, "NE": 31, "NV": 32,
    "NH": 33, "NJ": 34, "NM": 35, "NY": 36, "NC": 37, "ND": 38, "OH": 39,
    "OK": 40, "OR": 41, "PA": 42, "RI": 44, "SC": 45, "SD": 46, "TN": 47,
    "TX": 48, "UT": 49, "VT": 50, "VA": 51, "WA": 53, "WV": 54, "WI": 55,
    "WY": 56,
}


def _state_to_fips(state: str) -> int:
    return _STATE_FIPS.get(state.upper(), 0)


def main():
    parser = argparse.ArgumentParser(
        description="Load historical enrollment data for schools from Urban Institute API"
    )
    parser.add_argument("--states",       nargs="+", metavar="ST", help="Limit to specific states")
    parser.add_argument("--years",        type=int, default=DEFAULT_YEARS,
                        help=f"Number of school years to fetch (default {DEFAULT_YEARS})")
    parser.add_argument("--limit",        type=int, help="Max schools to process (for testing)")
    parser.add_argument("--charter-only", action="store_true", help="Only fetch charter schools")
    parser.add_argument("--verbose",      action="store_true", help="Print API progress")
    args = parser.parse_args()

    db.init_db()

    # Load NCES IDs from our schools table
    schools_df = db.get_schools(
        states=args.states,
        charter_only=args.charter_only,
    )
    if schools_df.empty:
        print("No schools found in database. Run fetch_nces_schools.py first.")
        return

    if args.limit:
        schools_df = schools_df.head(args.limit)

    # Group by state so we can fetch state-level API pages efficiently
    states_in_db = schools_df["state"].dropna().unique().tolist()
    print(f"CD Command Center — Enrollment History Fetch")
    print(f"  Schools:  {len(schools_df):,} in {len(states_in_db)} states")
    print(f"  Years:    {MOST_RECENT_YEAR - args.years + 1} – {MOST_RECENT_YEAR}")
    print()

    total_stored = 0

    for year in range(MOST_RECENT_YEAR, MOST_RECENT_YEAR - args.years, -1):
        print(f"  Fetching school year {year}...")
        year_stored = 0

        for state in sorted(states_in_db):
            state_nces = set(
                schools_df[schools_df["state"] == state]["nces_id"].dropna().astype(str).str.zfill(12)
            )
            if not state_nces:
                continue

            df = fetch_enrollment_by_state(year, state, verbose=args.verbose)
            if df.empty:
                if args.verbose:
                    print(f"    {state}: no data returned for {year}")
                continue

            # Filter to only schools in our database
            df = df[df["nces_id"].isin(state_nces)]

            for _, row in df.iterrows():
                db.upsert_enrollment_history({
                    "nces_id":     row["nces_id"],
                    "school_year": year,
                    "enrollment":  int(row["enrollment"]) if row["enrollment"] > 0 else None,
                })
                year_stored += 1

            time.sleep(API_SLEEP)

        print(f"    Stored {year_stored:,} enrollment records for {year}")
        total_stored += year_stored

    print()
    print(f"Done. Total records stored: {total_stored:,}")


if __name__ == "__main__":
    main()
