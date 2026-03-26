"""
etl/fetch_hmda.py — Load HMDA mortgage lending activity by census tract.

The Home Mortgage Disclosure Act (HMDA) requires most lenders to report every
mortgage application. The CFPB aggregates this into public data at the census
tract level. In CD deal origination, HMDA data is used to:
    - Identify credit deserts: tracts with high poverty but low mortgage origination
    - Show denial rates by geography (evidence of market failure warranting CDFI/NMTC)
    - Quantify the gap between housing demand and mortgage access in a target area
    - Support impact narratives with hard numbers on lending disparities

Data source:
    CFPB HMDA Data Browser API (no API key required):
        https://ffiec.cfpb.gov/api/data-browser-api/v2/

    This script fetches aggregate counts by census tract — NOT individual loan records.
    Individual records would be hundreds of millions of rows; tract aggregates are practical.

    The API returns counts of applications by action taken (originated, denied, withdrawn)
    grouped by census tract for a given state and year.

Usage:
    python etl/fetch_hmda.py --year 2023 --states CA TX NY
    python etl/fetch_hmda.py --year 2022 --states CA         # single state
    python etl/fetch_hmda.py --year 2023 --all               # all states (slow — ~50 API calls)

Notes:
    - HMDA data is available from 2018 onward via the CFPB API.
    - Each state fetch can take 10–30 seconds depending on API response time.
    - The API aggregates by census tract. Tracts with fewer than 10 applications
      may be suppressed by CFPB for privacy.
    - loan_purpose=1 is home purchase; 2 is home improvement; 31/32 are refinance.
"""

import argparse
import sys
import os
import time

import requests
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import db

CFPB_API_BASE = "https://ffiec.cfpb.gov/api/data-browser-api/v2/aggregations"
REQUEST_DELAY = 1.0  # CFPB asks for polite usage

ALL_STATES = [
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "DC", "FL",
    "GA", "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME",
    "MD", "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH",
    "NJ", "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI",
    "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY",
]

# HMDA action_taken codes
ACTION_ORIGINATED  = "1"
ACTION_APPROVED_NOT_ACCEPTED = "2"
ACTION_DENIED      = "3"
ACTION_WITHDRAWN   = "4"
ACTION_INCOMPLETE  = "5"
ACTION_PURCHASED   = "6"

# loan_purpose codes
PURPOSE_HOME_PURCHASE  = "1"
PURPOSE_HOME_IMPROVEMENT = "2"
PURPOSE_REFINANCE_CASH_OUT = "31"
PURPOSE_REFINANCE_NO_CASH  = "32"


def fetch_state_aggregates(state: str, year: int) -> list[dict]:
    """
    Fetch HMDA tract-level aggregates for one state and year.

    Makes one API call per action type to get counts by census tract,
    then joins them into a single row per tract.
    """
    # Fetch all applications (action_taken not filtered) grouped by census tract
    params = {
        "states": state,
        "years": str(year),
        "variables": "census_tract,action_taken,loan_purpose,loan_type",
        "pageSize": 10000,
    }

    resp = requests.get(CFPB_API_BASE, params=params, timeout=60)

    if resp.status_code == 404:
        return []

    resp.raise_for_status()
    data = resp.json()

    # Response structure: {"aggregations": [{"census_tract": "...", "action_taken": "1", "count": 5, ...}]}
    records = data.get("aggregations", [])

    if not records:
        return []

    df = pd.DataFrame(records)

    if "census_tract" not in df.columns:
        return []

    # Normalize census tract to 11-digit FIPS
    df["census_tract"] = df["census_tract"].astype(str).str.zfill(11)

    # Pivot: one row per tract, columns = action_taken counts
    df["count"] = pd.to_numeric(df.get("count", 0), errors="coerce").fillna(0)
    df["loan_amount_000s"] = pd.to_numeric(df.get("loan_amount_000s", None), errors="coerce")

    # Aggregate by tract + action_taken
    action_col = "action_taken" if "action_taken" in df.columns else None
    purpose_col = "loan_purpose" if "loan_purpose" in df.columns else None
    loan_type_col = "loan_type" if "loan_type" in df.columns else None

    tract_rows = {}

    for _, row in df.iterrows():
        tract = row["census_tract"]
        if tract not in tract_rows:
            tract_rows[tract] = {
                "census_tract_id": tract,
                "report_year": year,
                "state": state,
                "total_applications": 0,
                "total_originations": 0,
                "total_denials": 0,
                "total_withdrawn": 0,
                "home_purchase_originations": 0,
                "refinance_originations": 0,
                "home_improvement_originations": 0,
                "conventional_originations": 0,
                "fha_originations": 0,
                "va_originations": 0,
                "total_loan_amount": 0.0,
                "median_loan_amount": None,
            }

        count = int(row.get("count", 0) or 0)
        tract_rows[tract]["total_applications"] += count

        action = str(row.get(action_col, "")) if action_col else ""
        purpose = str(row.get(purpose_col, "")) if purpose_col else ""
        loan_type = str(row.get(loan_type_col, "")) if loan_type_col else ""

        if action == ACTION_ORIGINATED:
            tract_rows[tract]["total_originations"] += count
            if purpose == PURPOSE_HOME_PURCHASE:
                tract_rows[tract]["home_purchase_originations"] += count
            elif purpose == PURPOSE_HOME_IMPROVEMENT:
                tract_rows[tract]["home_improvement_originations"] += count
            elif purpose in (PURPOSE_REFINANCE_CASH_OUT, PURPOSE_REFINANCE_NO_CASH):
                tract_rows[tract]["refinance_originations"] += count
            # loan_type: 1=conventional, 2=FHA, 3=VA, 4=FSA/RHS
            if loan_type == "1":
                tract_rows[tract]["conventional_originations"] += count
            elif loan_type == "2":
                tract_rows[tract]["fha_originations"] += count
            elif loan_type == "3":
                tract_rows[tract]["va_originations"] += count

        elif action == ACTION_DENIED:
            tract_rows[tract]["total_denials"] += count
        elif action == ACTION_WITHDRAWN:
            tract_rows[tract]["total_withdrawn"] += count

        # Accumulate loan amounts (stored in thousands in HMDA)
        loan_amt = row.get("loan_amount_000s")
        if loan_amt and pd.notna(loan_amt):
            tract_rows[tract]["total_loan_amount"] += float(loan_amt) * 1000

    # Compute derived rates
    rows = []
    for tract, r in tract_rows.items():
        apps = r["total_applications"]
        origs = r["total_originations"]
        denials = r["total_denials"]
        r["denial_rate"] = round(denials / apps, 4) if apps > 0 else None
        r["origination_rate"] = round(origs / apps, 4) if apps > 0 else None
        r["total_loan_amount"] = round(r["total_loan_amount"], 2) if r["total_loan_amount"] else None
        rows.append(r)

    return rows


def main():
    parser = argparse.ArgumentParser(
        description="Load HMDA mortgage lending activity by census tract"
    )
    parser.add_argument(
        "--year",
        type=int,
        required=True,
        help="HMDA report year (2018 or later). E.g. --year 2023",
    )
    parser.add_argument(
        "--states",
        nargs="+",
        metavar="STATE",
        help="State abbreviations to load. E.g. --states CA TX NY",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        dest="all_states",
        help="Load all 50 states + DC (slow — about 50 API calls).",
    )
    args = parser.parse_args()

    if not args.states and not args.all_states:
        print("Error: specify --states or --all.")
        sys.exit(1)

    states = ALL_STATES if args.all_states else args.states

    print("CD Command Center — HMDA Load")
    print(f"  Year: {args.year}")
    print(f"  States: {', '.join(states)}")
    print()

    db.init_db()
    run_id = db.log_load_start("hmda")
    total_loaded = 0

    try:
        for state in states:
            print(f"  {state}...", end=" ", flush=True)
            try:
                rows = fetch_state_aggregates(state, args.year)
            except requests.RequestException as e:
                print(f"Error: {e}")
                continue

            if not rows:
                print("no data")
                continue

            n = db.upsert_rows("hmda_activity", rows, unique_cols=["census_tract_id", "report_year"])
            total_loaded += n
            print(f"{len(rows):,} tracts")
            time.sleep(REQUEST_DELAY)

    except Exception as e:
        db.log_load_finish(run_id, rows_loaded=total_loaded, error=str(e))
        raise

    db.log_load_finish(run_id, rows_loaded=total_loaded)
    print()
    print(f"Done. Total rows upserted: {total_loaded:,}")


if __name__ == "__main__":
    main()
