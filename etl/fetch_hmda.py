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
    CFPB HMDA Data Browser CSV download endpoint (no API key required):
        https://ffiec.cfpb.gov/v2/data-browser-api/view/csv

    This script downloads individual loan-level records per state as CSV,
    then aggregates them by census tract locally. The old aggregations API
    (/api/data-browser-api/v2/aggregations) no longer supports census_tract
    grouping as of API version v2.6+.

Usage:
    python etl/fetch_hmda.py --year 2023 --states CA TX NY
    python etl/fetch_hmda.py --year 2022 --states CA         # single state
    python etl/fetch_hmda.py --year 2023 --all               # all states (slow — ~50 downloads)

Notes:
    - HMDA data is available from 2018 onward via the CFPB API.
    - Each state CSV download can be 50-500 MB for large states.
    - Tracts with fewer than 10 applications may be suppressed by CFPB for privacy.
    - loan_purpose=1 is home purchase; 2 is home improvement; 31/32 are refinance.
    - loan_amount is in dollars in the CSV (not thousands).
"""

import argparse
import sys
import os
import io
import time

import requests
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import db

CFPB_CSV_URL = "https://ffiec.cfpb.gov/v2/data-browser-api/view/csv"
REQUEST_DELAY = 2.0  # Be polite — CSV downloads are heavier than API calls

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
    Download HMDA loan-level CSV for one state and year, then aggregate by census tract.

    The CFPB /view/csv endpoint returns individual loan application records.
    We group them by census_tract and sum counts and amounts to produce one
    row per tract — matching the schema of the hmda_activity table.

    Note: The old /api/data-browser-api/v2/aggregations endpoint no longer
    supports census_tract grouping (API v2.6+), so we download raw CSV and
    aggregate locally.
    """
    params = {
        "states": state,
        "years": str(year),
        # Include all action types so we can count applications, originations, denials, etc.
        "actions_taken": "1,2,3,4,5,6",
    }

    resp = requests.get(CFPB_CSV_URL, params=params, timeout=300, allow_redirects=True,
                        stream=True)

    if resp.status_code == 404:
        return []

    resp.raise_for_status()

    # Stream the CSV in 50k-row chunks to avoid OOM on large states (CA/TX/NY can be
    # 500k+ rows). We write to a temp file then read back chunked.
    import tempfile
    tmp = tempfile.NamedTemporaryFile(mode='wb', suffix='.csv', delete=False)
    tmp_path = tmp.name
    try:
        for chunk in resp.iter_content(chunk_size=1024 * 1024):  # 1 MB chunks
            if chunk:
                tmp.write(chunk)
        tmp.close()

        # Peek at header to validate columns exist
        header_df = pd.read_csv(tmp_path, nrows=0, dtype=str)
        if "census_tract" not in header_df.columns:
            return []

        tract_rows = {}

        for chunk_df in pd.read_csv(tmp_path, dtype=str, low_memory=False, chunksize=50_000):
            # Drop rows missing census_tract
            chunk_df = chunk_df[chunk_df["census_tract"].notna()]
            chunk_df = chunk_df[chunk_df["census_tract"].astype(str).str.strip() != ""]
            chunk_df = chunk_df[~chunk_df["census_tract"].astype(str).str.lower().isin(
                ["nan", "na", "none", "exempt"]
            )]
            if chunk_df.empty:
                continue

            chunk_df["census_tract"] = chunk_df["census_tract"].astype(str).str.zfill(11)
            chunk_df["action_taken"] = chunk_df["action_taken"].astype(str)
            chunk_df["loan_purpose"] = chunk_df["loan_purpose"].astype(str) \
                if "loan_purpose" in chunk_df.columns else ""
            chunk_df["loan_type"]    = chunk_df["loan_type"].astype(str) \
                if "loan_type"    in chunk_df.columns else ""
            chunk_df["loan_amount"]  = pd.to_numeric(
                chunk_df.get("loan_amount"), errors="coerce"
            )

            # Vectorized aggregation — much faster than iterrows for large CSVs
            g = chunk_df.groupby("census_tract")

            # Total applications per tract
            apps = g.size().rename("total_applications")

            # Counts by action type
            orig_mask = chunk_df["action_taken"] == ACTION_ORIGINATED
            deny_mask = chunk_df["action_taken"] == ACTION_DENIED
            with_mask = chunk_df["action_taken"] == ACTION_WITHDRAWN

            orig_df = chunk_df[orig_mask]
            deny_df = chunk_df[deny_mask]
            with_df = chunk_df[with_mask]

            originations     = orig_df.groupby("census_tract").size().rename("total_originations")
            denials          = deny_df.groupby("census_tract").size().rename("total_denials")
            withdrawals      = with_df.groupby("census_tract").size().rename("total_withdrawn")

            hp_orig  = orig_df[orig_df["loan_purpose"] == PURPOSE_HOME_PURCHASE].groupby("census_tract").size().rename("home_purchase_originations")
            hi_orig  = orig_df[orig_df["loan_purpose"] == PURPOSE_HOME_IMPROVEMENT].groupby("census_tract").size().rename("home_improvement_originations")
            ref_orig = orig_df[orig_df["loan_purpose"].isin([PURPOSE_REFINANCE_CASH_OUT, PURPOSE_REFINANCE_NO_CASH])].groupby("census_tract").size().rename("refinance_originations")

            conv_orig = orig_df[orig_df["loan_type"] == "1"].groupby("census_tract").size().rename("conventional_originations")
            fha_orig  = orig_df[orig_df["loan_type"] == "2"].groupby("census_tract").size().rename("fha_originations")
            va_orig   = orig_df[orig_df["loan_type"] == "3"].groupby("census_tract").size().rename("va_originations")

            loan_amt_sum = chunk_df.groupby("census_tract")["loan_amount"].sum().rename("total_loan_amount")

            # Merge all series into a single chunk-level DataFrame
            chunk_agg = pd.concat([
                apps, originations, denials, withdrawals,
                hp_orig, hi_orig, ref_orig,
                conv_orig, fha_orig, va_orig, loan_amt_sum,
            ], axis=1).fillna(0)
            chunk_agg.index.name = "census_tract"
            chunk_agg = chunk_agg.reset_index()

            # Accumulate into tract_rows dict
            for _, row in chunk_agg.iterrows():
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
                for col in ("total_applications", "total_originations", "total_denials",
                            "total_withdrawn", "home_purchase_originations",
                            "home_improvement_originations", "refinance_originations",
                            "conventional_originations", "fha_originations",
                            "va_originations", "total_loan_amount"):
                    if col in row:
                        tract_rows[tract][col] += int(row[col]) if col != "total_loan_amount" else float(row[col])

    finally:
        import os as _os
        try:
            _os.unlink(tmp_path)
        except OSError:
            pass

    # Compute derived rates
    rows = []
    for tract, r in tract_rows.items():
        apps    = r["total_applications"]
        origs   = r["total_originations"]
        denials = r["total_denials"]
        r["denial_rate"]       = round(denials / apps, 4) if apps > 0 else None
        r["origination_rate"]  = round(origs   / apps, 4) if apps > 0 else None
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
