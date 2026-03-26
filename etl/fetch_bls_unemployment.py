"""
etl/fetch_bls_unemployment.py — Load BLS unemployment rates by county and MSA.

Monthly unemployment data provides economic context that deal underwriters and
impact analysts use to:
    - Benchmark a target market's economic conditions against national / state averages
    - Show labor market distress as part of a NMTC "community need" narrative
    - Track economic recovery or decline in a project's service area over time
    - Support job creation impact claims with baseline unemployment context

Data source:
    BLS Local Area Unemployment Statistics (LAUS) are available via two routes,
    both used here:

    Route 1 — FRED API (recommended for MSAs and states):
        FRED hosts BLS LAUS series under consistent series IDs.
        Requires a free FRED API key (fred.stlouisfed.org).
        Series ID format:
            - County: LAUCN{5-digit-fips}0000000003A  (annual) or ...3  (monthly)
            - MSA: LAUMT{6-digit-msa}0000000003  (monthly unemployment rate)
            - State: {STATE}UR (e.g. CAUR for California)

    Route 2 — BLS public API (no key for small requests):
        https://api.bls.gov/publicAPI/v2/timeseries/data/
        Accepts up to 50 series per request without a key; 500 with a free BLS key.
        Register for a free BLS API key at: https://data.bls.gov/registrationEngine/

    This script supports both routes. FRED is simpler for ad-hoc pulls;
    BLS API is better for bulk county-level loads.

Usage:
    # Fetch state-level unemployment from FRED (quick overview):
    python etl/fetch_bls_unemployment.py --mode fred-states --api-key YOUR_FRED_KEY
    python etl/fetch_bls_unemployment.py --mode fred-states --api-key YOUR_FRED_KEY --states CA TX NY

    # Fetch MSA-level unemployment from FRED:
    python etl/fetch_bls_unemployment.py --mode fred-msa --api-key YOUR_FRED_KEY \\
        --msa-series LAUMT064720000000003 LAUMT367400000000003

    # Fetch county-level unemployment from BLS API (no key needed for small requests):
    python etl/fetch_bls_unemployment.py --mode bls-county --fips 06037 06059 17031
    python etl/fetch_bls_unemployment.py --mode bls-county --fips 06037 --bls-key YOUR_BLS_KEY

    # Or set env vars:
    export FRED_API_KEY=your_fred_key
    export BLS_API_KEY=your_bls_key
    python etl/fetch_bls_unemployment.py --mode fred-states

FRED API key:  https://fred.stlouisfed.org/docs/api/api_key.html
BLS API key:   https://data.bls.gov/registrationEngine/
Both are free.
"""

import argparse
import os
import sys
import time
from datetime import date

import requests

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import db

FRED_BASE_URL = "https://api.stlouisfed.org/fred/series/observations"
BLS_BASE_URL  = "https://api.bls.gov/publicAPI/v2/timeseries/data/"
REQUEST_DELAY = 0.5

# State abbreviation → FRED unemployment series ID (seasonally adjusted monthly rate)
STATE_FRED_SERIES = {
    "AL": "ALUR", "AK": "AKUR", "AZ": "AZUR", "AR": "ARUR", "CA": "CAUR",
    "CO": "COUR", "CT": "CTUR", "DE": "DEUR", "DC": "DCUR", "FL": "FLUR",
    "GA": "GAUR", "HI": "HIUR", "ID": "IDUR", "IL": "ILUR", "IN": "INUR",
    "IA": "IAUR", "KS": "KSUR", "KY": "KYUR", "LA": "LAUR", "ME": "MEUR",
    "MD": "MDUR", "MA": "MAUR", "MI": "MIUR", "MN": "MNUR", "MS": "MSUR",
    "MO": "MOUR", "MT": "MTUR", "NE": "NEUR", "NV": "NVUR", "NH": "NHUR",
    "NJ": "NJUR", "NM": "NMUR", "NY": "NYUR", "NC": "NCUR", "ND": "NDUR",
    "OH": "OHUR", "OK": "OKUR", "OR": "ORUR", "PA": "PAUR", "RI": "RIUR",
    "SC": "SCUR", "SD": "SDUR", "TN": "TNUR", "TX": "TXUR", "UT": "UTUR",
    "VT": "VTUR", "VA": "VAUR", "WA": "WAUR", "WV": "WVUR", "WI": "WIUR",
    "WY": "WYUR",
}

ALL_STATES = list(STATE_FRED_SERIES.keys())


# ---------------------------------------------------------------------------
# FRED API helpers
# ---------------------------------------------------------------------------

def fetch_fred_series(series_id: str, api_key: str, start_date: str, end_date: str) -> list[dict]:
    """Fetch observations for a single FRED series."""
    resp = requests.get(FRED_BASE_URL, params={
        "series_id": series_id,
        "api_key": api_key,
        "file_type": "json",
        "observation_start": start_date,
        "observation_end": end_date,
        "sort_order": "asc",
    }, timeout=30)

    if resp.status_code == 400:
        return []
    resp.raise_for_status()
    return resp.json().get("observations", [])


def load_fred_states(states: list[str], api_key: str, months: int) -> list[dict]:
    """Fetch monthly state unemployment rates from FRED."""
    end = date.today()
    start = date(end.year - (months // 12 + 1), end.month, 1)

    rows = []
    for state in states:
        series_id = STATE_FRED_SERIES.get(state)
        if not series_id:
            print(f"    No FRED series known for state: {state} — skipping.")
            continue

        print(f"  {state} ({series_id})...", end=" ", flush=True)
        observations = fetch_fred_series(series_id, api_key, start.isoformat(), end.isoformat())

        count = 0
        for obs in observations:
            if obs.get("value") == ".":
                continue
            try:
                rate = float(obs["value"])
            except (ValueError, TypeError):
                continue
            # FRED state series are monthly; period = YYYY-MM
            period = obs["date"][:7]
            rows.append({
                "area_fips": state,       # use state abbrev as key for state-level records
                "area_name": state,
                "area_type": "state",
                "state": state,
                "period": period,
                "unemployment_rate": rate,
                "labor_force": None,
                "employed": None,
                "unemployed": None,
            })
            count += 1

        print(f"{count} months")
        time.sleep(REQUEST_DELAY)

    return rows


def load_fred_msa(msa_series: list[str], api_key: str, months: int) -> list[dict]:
    """
    Fetch monthly MSA unemployment rates from FRED.
    msa_series: list of FRED series IDs like ['LAUMT064720000000003', ...]
    The series ID encodes the MSA FIPS: LAUMT + 6-digit MSA + 0000000003.
    """
    end = date.today()
    start = date(end.year - (months // 12 + 1), end.month, 1)

    rows = []
    for series_id in msa_series:
        print(f"  {series_id}...", end=" ", flush=True)
        observations = fetch_fred_series(series_id, api_key, start.isoformat(), end.isoformat())

        # Extract MSA FIPS from series ID: LAUMT{6-digit-fips}...
        msa_fips = series_id[5:11] if series_id.startswith("LAUMT") else series_id

        count = 0
        for obs in observations:
            if obs.get("value") == ".":
                continue
            try:
                rate = float(obs["value"])
            except (ValueError, TypeError):
                continue
            rows.append({
                "area_fips": msa_fips,
                "area_name": None,
                "area_type": "msa",
                "state": None,
                "period": obs["date"][:7],
                "unemployment_rate": rate,
                "labor_force": None,
                "employed": None,
                "unemployed": None,
            })
            count += 1

        print(f"{count} months")
        time.sleep(REQUEST_DELAY)

    return rows


# ---------------------------------------------------------------------------
# BLS API helpers (county-level)
# ---------------------------------------------------------------------------

def build_bls_series_id(county_fips: str) -> str:
    """
    Build the BLS LAUS series ID for a county's unemployment RATE.
    Format: LA + U + N + {5-digit-fips} + 0000000003
    Series measure code 3 = unemployment rate.
    """
    fips = str(county_fips).zfill(5)
    return f"LAUCN{fips}0000000003"


def fetch_bls_series(series_ids: list[str], bls_key: str, start_year: int, end_year: int) -> dict:
    """
    Fetch BLS series data for up to 50 series per call (500 with a key).
    Returns dict: series_id → list of (period, value) tuples.
    """
    payload = {
        "seriesid": series_ids,
        "startyear": str(start_year),
        "endyear": str(end_year),
        "catalog": False,
        "calculations": False,
        "annualaverage": False,
    }
    if bls_key:
        payload["registrationkey"] = bls_key

    resp = requests.post(BLS_BASE_URL, json=payload, timeout=60)
    resp.raise_for_status()
    data = resp.json()

    results = {}
    for series in data.get("Results", {}).get("series", []):
        sid = series["seriesID"]
        results[sid] = []
        for obs in series.get("data", []):
            # BLS period format: M01=Jan, M12=Dec, M13=annual
            period_code = obs.get("period", "")
            if not period_code.startswith("M") or period_code == "M13":
                continue
            month = period_code[1:]  # '01' through '12'
            year = obs.get("year", "")
            period = f"{year}-{month}"
            try:
                rate = float(obs["value"])
            except (ValueError, TypeError):
                continue
            results[sid].append((period, rate))

    return results


def load_bls_counties(county_fips_list: list[str], bls_key: str, years: int) -> list[dict]:
    """Fetch county unemployment rates from BLS API in batches of 50."""
    end_year = date.today().year
    start_year = end_year - years

    batch_size = 500 if bls_key else 50
    rows = []

    for i in range(0, len(county_fips_list), batch_size):
        batch = county_fips_list[i:i + batch_size]
        series_ids = [build_bls_series_id(f) for f in batch]
        fips_by_series = {build_bls_series_id(f): f for f in batch}

        print(f"  Fetching batch {i // batch_size + 1} ({len(batch)} counties)...", end=" ", flush=True)
        try:
            results = fetch_bls_series(series_ids, bls_key, start_year, end_year)
        except requests.RequestException as e:
            print(f"Error: {e}")
            continue

        count = 0
        for series_id, observations in results.items():
            fips = fips_by_series.get(series_id, series_id)
            state = fips[:2] if len(fips) >= 2 else None
            for period, rate in observations:
                rows.append({
                    "area_fips": fips,
                    "area_name": None,
                    "area_type": "county",
                    "state": state,
                    "period": period,
                    "unemployment_rate": rate,
                    "labor_force": None,
                    "employed": None,
                    "unemployed": None,
                })
                count += 1

        print(f"{count} observations")
        time.sleep(REQUEST_DELAY)

    return rows


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Load BLS unemployment rates by state, MSA, or county"
    )
    parser.add_argument(
        "--mode",
        choices=["fred-states", "fred-msa", "bls-county"],
        required=True,
        help=(
            "fred-states: state monthly rates via FRED (needs --api-key). "
            "fred-msa: MSA rates via FRED (needs --api-key and --msa-series). "
            "bls-county: county rates via BLS API (needs --fips)."
        ),
    )
    parser.add_argument(
        "--api-key",
        default=os.environ.get("FRED_API_KEY"),
        help="FRED API key (for fred-states and fred-msa modes). Or set FRED_API_KEY env var.",
    )
    parser.add_argument(
        "--bls-key",
        default=os.environ.get("BLS_API_KEY"),
        help="BLS API key (optional; increases batch size from 50→500). Or set BLS_API_KEY env var.",
    )
    parser.add_argument(
        "--states",
        nargs="+",
        metavar="STATE",
        help="For fred-states: state abbreviations (default: all).",
    )
    parser.add_argument(
        "--msa-series",
        nargs="+",
        metavar="SERIES_ID",
        dest="msa_series",
        help="For fred-msa: FRED series IDs. E.g. LAUMT064720000000003",
    )
    parser.add_argument(
        "--fips",
        nargs="+",
        metavar="FIPS",
        help="For bls-county: 5-digit county FIPS codes. E.g. --fips 06037 17031",
    )
    parser.add_argument(
        "--months",
        type=int,
        default=36,
        help="Months of history to fetch for FRED modes (default: 36).",
    )
    parser.add_argument(
        "--years",
        type=int,
        default=3,
        help="Years of history for bls-county mode (default: 3).",
    )
    args = parser.parse_args()

    print("CD Command Center — BLS Unemployment Load")
    print(f"  Mode: {args.mode}")
    print()

    db.init_db()
    run_id = db.log_load_start("bls_unemployment")
    total_loaded = 0

    try:
        if args.mode == "fred-states":
            if not args.api_key:
                print("Error: --api-key or FRED_API_KEY env var required for fred-states mode.")
                sys.exit(1)
            states = args.states if args.states else ALL_STATES
            rows = load_fred_states(states, args.api_key, args.months)
            if rows:
                total_loaded = db.upsert_rows("bls_unemployment", rows, unique_cols=["area_fips", "period"])

        elif args.mode == "fred-msa":
            if not args.api_key:
                print("Error: --api-key or FRED_API_KEY env var required for fred-msa mode.")
                sys.exit(1)
            if not args.msa_series:
                print("Error: --msa-series required for fred-msa mode.")
                sys.exit(1)
            rows = load_fred_msa(args.msa_series, args.api_key, args.months)
            if rows:
                total_loaded = db.upsert_rows("bls_unemployment", rows, unique_cols=["area_fips", "period"])

        elif args.mode == "bls-county":
            if not args.fips:
                print("Error: --fips required for bls-county mode.")
                sys.exit(1)
            rows = load_bls_counties(args.fips, args.bls_key or "", args.years)
            if rows:
                total_loaded = db.upsert_rows("bls_unemployment", rows, unique_cols=["area_fips", "period"])

    except Exception as e:
        db.log_load_finish(run_id, rows_loaded=total_loaded, error=str(e))
        raise

    db.log_load_finish(run_id, rows_loaded=total_loaded)
    print()
    print(f"Done. Total rows upserted: {total_loaded:,}")


if __name__ == "__main__":
    main()
