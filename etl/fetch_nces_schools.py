"""
etl/fetch_nces_schools.py — Fetch NCES public school data via Urban Institute API.

Data source: Urban Institute Education Data Portal
  https://educationdata.urban.org/api/v1/schools/ccd/directory/{year}/

This script fetches ALL public schools (charters and traditional) from the NCES
Common Core of Data and loads them into the schools table. Each school is tagged
with is_charter=1 or 0 based on the API's charter field.

It fetches state-by-state (including territories) so you can watch progress
and so a single failure doesn't lose all work.

Usage:
    # Fetch all public schools for all states + territories (default):
    python etl/fetch_nces_schools.py

    # Fetch specific states only:
    python etl/fetch_nces_schools.py --states CA TX NY

    # Fetch only charter schools (backward-compatible):
    python etl/fetch_nces_schools.py --charter-only

    # Also fetch race/ethnicity demographics (pct_black, pct_hispanic, pct_white):
    python etl/fetch_nces_schools.py --demographics

    # Use a different data year:
    python etl/fetch_nces_schools.py --year 2022

Race codes in the enrollment endpoint:
    1 = White, 2 = Black/African American, 3 = Hispanic,
    4 = Asian, 5 = Pacific Islander, 6 = American Indian,
    7 = Two or more races, 99 = All races (total)
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
from models.charter_survival import CharterSurvivalModel

# ---------------------------------------------------------------------------
# State / Territory FIPS codes (all 50 states + DC + territories)
# ---------------------------------------------------------------------------

# Maps 2-letter abbreviation → state FIPS code (int, as used by the API)
STATE_FIPS = {
    "AL": 1,  "AK": 2,  "AZ": 4,  "AR": 5,  "CA": 6,
    "CO": 8,  "CT": 9,  "DE": 10, "DC": 11, "FL": 12,
    "GA": 13, "HI": 15, "ID": 16, "IL": 17, "IN": 18,
    "IA": 19, "KS": 20, "KY": 21, "LA": 22, "ME": 23,
    "MD": 24, "MA": 25, "MI": 26, "MN": 27, "MS": 28,
    "MO": 29, "MT": 30, "NE": 31, "NV": 32, "NH": 33,
    "NJ": 34, "NM": 35, "NY": 36, "NC": 37, "ND": 38,
    "OH": 39, "OK": 40, "OR": 41, "PA": 42, "RI": 44,
    "SC": 45, "SD": 46, "TN": 47, "TX": 48, "UT": 49,
    "VT": 50, "VA": 51, "WA": 53, "WV": 54, "WI": 55,
    "WY": 56,
    # Territories
    "PR": 72,  # Puerto Rico
    "GU": 66,  # Guam
    "VI": 78,  # US Virgin Islands
    "AS": 60,  # American Samoa
    "MP": 69,  # Northern Mariana Islands
}

# Reverse map: FIPS int → abbreviation (used for display)
FIPS_STATE = {v: k for k, v in STATE_FIPS.items()}

BASE_DIRECTORY_URL = "https://educationdata.urban.org/api/v1/schools/ccd/directory"
BASE_ENROLLMENT_URL = "https://educationdata.urban.org/api/v1/schools/ccd/enrollment"

# How many records to fetch per API page (max the API allows is ~10000)
PER_PAGE = 2000

# Seconds to wait between API pages to avoid hammering the server
PAGE_SLEEP = 0.3

# ---------------------------------------------------------------------------
# school_status int → string
# See NCES CCD data documentation
# ---------------------------------------------------------------------------
STATUS_MAP = {
    1: "Open",
    2: "Closed",
    3: "Open",     # New school
    4: "Open",     # Added (reopened)
    5: "Closed",   # Changed agency
    6: "Closed",   # Inactive
    7: "Pending",  # Future school
}


def decode_status(code) -> str:
    """Convert NCES school_status integer code to human-readable string."""
    if code is None:
        return "Open"
    try:
        return STATUS_MAP.get(int(code), "Open")
    except (TypeError, ValueError):
        return "Open"


def decode_grade(code) -> str | None:
    """
    Convert NCES grade integer code to a grade label string.

    NCES grade codes:
      -1 = Pre-K, 0 = Kindergarten, 1-12 = grades, 13 = Ungraded
    """
    if code is None:
        return None
    try:
        code = int(code)
    except (TypeError, ValueError):
        return None

    if code == -1:
        return "PK"
    elif code == 0:
        return "KG"
    elif 1 <= code <= 12:
        return str(code)
    elif code == 13:
        return "UG"
    else:
        return None


def fetch_state_demographics(year: int, fips: int, ncessch_set: set) -> dict:
    """
    Fetch race/ethnicity enrollment data for schools in a single state.

    Uses the enrollment endpoint with race disaggregation, then filters to only
    the school IDs we already fetched from the directory.

    Returns:
        dict keyed by ncessch → {'pct_black': float|None, 'pct_hispanic': float|None,
                                   'pct_white': float|None}
    """
    results = []
    page = 1

    url = f"{BASE_ENROLLMENT_URL}/{year}/grade-99/race-2/"

    while True:
        params = {
            "fips": fips,
            "per_page": PER_PAGE,
            "page": page,
        }
        try:
            resp = requests.get(url, params=params, timeout=30)
            resp.raise_for_status()
            data = resp.json()
        except requests.RequestException as e:
            print(f"    Enrollment API error on page {page}: {e}")
            break

        results.extend(data.get("results", []))

        if not data.get("next"):
            break
        page += 1
        time.sleep(PAGE_SLEEP)

    # Build a lookup: ncessch → {race_code: enrollment_count}
    raw = {}
    for row in results:
        ncessch = row.get("ncessch")
        if ncessch not in ncessch_set:
            continue
        if row.get("sex") != 99:
            continue

        race_code = row.get("race")
        enrollment_count = row.get("enrollment") or 0
        if enrollment_count < 0:
            enrollment_count = 0

        if ncessch not in raw:
            raw[ncessch] = {}
        raw[ncessch][race_code] = enrollment_count

    # Compute percentages from raw counts
    demographics = {}
    for ncessch, counts in raw.items():
        total = counts.get(99, 0)
        if total and total > 0:
            demographics[ncessch] = {
                "pct_black":    _safe_pct(counts.get(2, 0), total),
                "pct_hispanic": _safe_pct(counts.get(3, 0), total),
                "pct_white":    _safe_pct(counts.get(1, 0), total),
            }
        else:
            demographics[ncessch] = {
                "pct_black": None,
                "pct_hispanic": None,
                "pct_white": None,
            }

    return demographics


def _safe_pct(numerator, denominator) -> float | None:
    """Compute a percentage, returning None if the denominator is zero or None."""
    if not denominator or denominator <= 0:
        return None
    return round(numerator / denominator * 100, 1)


def fetch_state_schools(year: int, fips: int, charter_only: bool = False) -> list:
    """
    Fetch public schools for a single state from the Urban Institute API.

    Args:
        year: CCD data year (e.g. 2023)
        fips: State FIPS code as int (e.g. 6 for CA)
        charter_only: if True, only fetch charter schools (charter=1)

    Returns:
        List of raw API result dicts for that state
    """
    results = []
    page = 1

    while True:
        url = f"{BASE_DIRECTORY_URL}/{year}/"
        params = {
            "fips": fips,
            "per_page": PER_PAGE,
            "page": page,
        }
        if charter_only:
            params["charter"] = 1

        try:
            resp = requests.get(url, params=params, timeout=30)
            resp.raise_for_status()
            data = resp.json()
        except requests.RequestException as e:
            print(f"    API error on page {page}: {e}")
            break

        page_results = data.get("results", [])
        results.extend(page_results)

        if not data.get("next"):
            break

        page += 1
        time.sleep(PAGE_SLEEP)

    return results


def map_record(api_row: dict, year: int, demographics: dict = None) -> dict:
    """
    Map a raw Urban Institute API result dict → our schools column schema.

    Handles:
    - Field renaming
    - school_status int → string
    - Grade code int → string label
    - FRL count → percentage
    - county FIPS construction (state FIPS + county code)
    - is_charter flag from API charter field
    """
    enrollment = api_row.get("enrollment") or 0
    frl_count = api_row.get("free_or_reduced_price_lunch") or 0

    # NCES uses negative sentinel codes (-1, -2, -3) for missing/suppressed data.
    if enrollment < 0:
        enrollment = 0
    if frl_count < 0:
        frl_count = None

    pct_frl = round(frl_count / enrollment * 100, 1) if (enrollment > 0 and frl_count is not None) else None

    state_fips = api_row.get("fips")
    county_code = api_row.get("county_code")
    county_fips = None
    if state_fips and county_code:
        try:
            county_fips = f"{int(state_fips):02d}{str(county_code).zfill(3)}"
        except (TypeError, ValueError):
            pass

    # Determine charter status from API (charter field: 1=charter, 0=not charter)
    charter_val = api_row.get("charter")
    is_charter = 1 if charter_val == 1 else 0

    # City: fall back to empty string so display shows "—" not blank
    city = api_row.get("city_location") or ""

    return {
        "nces_id": str(api_row.get("ncessch", "") or "").strip() or None,
        "school_name": api_row.get("school_name"),
        "lea_name": api_row.get("lea_name"),
        "lea_id": str(api_row.get("leaid", "") or "").strip() or None,
        "state": api_row.get("state_location"),
        "city": city if city else None,
        "address": api_row.get("street_location"),
        "zip_code": str(api_row.get("zip_location", "") or "").strip() or None,
        "county": county_fips,
        "census_tract_id": None,         # assigned later via etl/assign_census_tracts.py
        "latitude": api_row.get("latitude"),
        "longitude": api_row.get("longitude"),
        "enrollment": int(enrollment) if enrollment > 0 else None,
        "grade_low": decode_grade(api_row.get("lowest_grade_offered")),
        "grade_high": decode_grade(api_row.get("highest_grade_offered")),
        "is_charter": is_charter,
        "pct_free_reduced_lunch": pct_frl,
        "pct_ell": None,
        "pct_sped": None,
        "pct_black":    (demographics or {}).get(api_row.get("ncessch"), {}).get("pct_black"),
        "pct_hispanic": (demographics or {}).get(api_row.get("ncessch"), {}).get("pct_hispanic"),
        "pct_white":    (demographics or {}).get(api_row.get("ncessch"), {}).get("pct_white"),
        "school_status": decode_status(api_row.get("school_status")),
        "year_opened": None,
        "year_closed": None,
        "survival_score": None,
        "survival_risk_tier": None,
        "data_year": year,
    }


def score_records(records: list) -> list:
    """
    Run the charter survival model on charter school records.
    Only scores records where is_charter=1.
    """
    model = CharterSurvivalModel()

    model_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "models", "charter_survival.pkl",
    )
    if os.path.exists(model_path):
        model.load(model_path)

    # Only score charter schools
    charter_records = [r for r in records if r.get("is_charter") == 1]
    if charter_records:
        df = pd.DataFrame(charter_records)
        scores = model.predict_batch(df)

        charter_idx = 0
        for record in records:
            if record.get("is_charter") == 1:
                record["survival_score"] = scores["survival_score"].iloc[charter_idx]
                record["survival_risk_tier"] = scores["survival_risk_tier"].iloc[charter_idx]
                charter_idx += 1

    return records


def load_to_db(records: list) -> tuple[int, int]:
    """
    Upsert records into the schools table.
    Returns (loaded_count, error_count).
    """
    loaded = 0
    errors = 0
    for record in records:
        if not record.get("school_name"):
            errors += 1
            continue
        try:
            db.upsert_school(record)
            loaded += 1
        except Exception as e:
            print(f"    DB error for {record.get('nces_id', '?')}: {e}")
            errors += 1
    return loaded, errors


def main():
    parser = argparse.ArgumentParser(
        description="Fetch NCES public school data via Urban Institute API"
    )
    parser.add_argument(
        "--year",
        type=int,
        default=2023,
        help="CCD data year to fetch (default: 2023)",
    )
    parser.add_argument(
        "--states",
        nargs="+",
        metavar="STATE",
        help="2-letter state abbreviations to fetch (default: all states + territories)",
    )
    parser.add_argument(
        "--charter-only",
        action="store_true",
        help="Only fetch charter schools (backward-compatible mode)",
    )
    parser.add_argument(
        "--demographics",
        action="store_true",
        help="Also fetch race/ethnicity enrollment data (pct_black, pct_hispanic, pct_white). "
             "Adds extra API calls per state.",
    )
    args = parser.parse_args()

    # Determine which states to fetch
    if args.states:
        unknown = [s for s in args.states if s.upper() not in STATE_FIPS]
        if unknown:
            print(f"Error: unknown state abbreviation(s): {', '.join(unknown)}")
            print(f"Valid options: {', '.join(sorted(STATE_FIPS.keys()))}")
            sys.exit(1)
        states_to_fetch = {s.upper(): STATE_FIPS[s.upper()] for s in args.states}
    else:
        states_to_fetch = STATE_FIPS

    school_type = "charter schools" if args.charter_only else "all public schools"
    print(f"CD Command Center — NCES School Fetch ({school_type})")
    print(f"  Data year: {args.year}")
    print(f"  States/territories: {len(states_to_fetch)} ({', '.join(states_to_fetch.keys())})")
    print(f"  Charter only: {'yes' if args.charter_only else 'no (all public schools)'}")
    print(f"  Demographics: {'yes' if args.demographics else 'no (use --demographics to include)'}")
    print()

    db.init_db()

    all_records = []
    total_api_results = 0

    for abbr, fips in states_to_fetch.items():
        print(f"  Fetching {abbr}...", end="", flush=True)
        raw = fetch_state_schools(args.year, fips, charter_only=args.charter_only)
        total_api_results += len(raw)

        # Count charters vs traditional
        charters = sum(1 for r in raw if r.get("charter") == 1)
        print(f" {len(raw):,} schools ({charters:,} charters)", end="")

        # Optionally fetch race/ethnicity enrollment data
        demographics = None
        if args.demographics and raw:
            print(f", fetching demographics...", end="", flush=True)
            school_ids = {str(row.get("ncessch", "")).strip() for row in raw if row.get("ncessch")}
            demographics = fetch_state_demographics(args.year, fips, school_ids)
            pct_with_demo = sum(1 for v in demographics.values() if v.get("pct_black") is not None)
            print(f" ({pct_with_demo:,} with race data)", end="")

        print()

        for api_row in raw:
            record = map_record(api_row, args.year, demographics=demographics)
            all_records.append(record)

    print()
    print(f"Fetched {total_api_results:,} total schools from API.")

    if not all_records:
        print("No records to load. Check your year/state arguments.")
        sys.exit(0)

    print("Running survival model scoring (charter schools only)...")
    all_records = score_records(all_records)

    print(f"Loading into database...")
    loaded, errors = load_to_db(all_records)

    print()
    print(f"Done.")
    print(f"  Loaded: {loaded:,} schools")
    if errors:
        print(f"  Errors: {errors:,}")

    charter_count = sum(1 for r in all_records if r.get("is_charter") == 1)
    trad_count = sum(1 for r in all_records if r.get("is_charter") == 0)
    print(f"  Charter schools: {charter_count:,}")
    print(f"  Traditional public: {trad_count:,}")

    summary = db.get_school_summary()
    print()
    print(f"Database now contains:")
    print(f"  Total schools: {summary.get('total_schools', 0):,}")
    print(f"  Open schools:  {summary.get('open_schools', 0):,}")


if __name__ == "__main__":
    main()
