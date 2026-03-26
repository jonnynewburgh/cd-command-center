"""
etl/fetch_fred_rates.py — Fetch market rate data from the St. Louis Fed FRED API.

Loads daily observations for the following series into the market_rates table:

    SOFR       Secured Overnight Financing Rate (replaces LIBOR for most CDFI deals)
    DGS5       5-Year Treasury Constant Maturity Rate
    DGS10      10-Year Treasury Constant Maturity Rate
    DGS30      30-Year Treasury Constant Maturity Rate
    DFF        Effective Federal Funds Rate (daily)

Why these series:
    SOFR is the floating-rate benchmark used in most new NMTC and CDFI loan structures.
    5/10/30yr Treasuries are the spread benchmarks for fixed-rate CD loans.
    Fed Funds sets the floor for short-term lending rates.

API key:
    FRED requires a free API key. Get one at:
        https://fred.stlouisfed.org/docs/api/api_key.html
    Pass it via --api-key or set the FRED_API_KEY environment variable.

Usage:
    python etl/fetch_fred_rates.py --api-key YOUR_KEY
    python etl/fetch_fred_rates.py --api-key YOUR_KEY --days 730   # 2 years of history
    python etl/fetch_fred_rates.py --api-key YOUR_KEY --series SOFR DGS10  # specific series only
    python etl/fetch_fred_rates.py --api-key YOUR_KEY --latest     # most recent value only

    # With env var:
    export FRED_API_KEY=your_key_here
    python etl/fetch_fred_rates.py
"""

import argparse
import os
import sys
import time
from datetime import date, timedelta

import requests

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import db

# ---------------------------------------------------------------------------
# Series to fetch
# ---------------------------------------------------------------------------

# Each entry: (series_id, human_readable_name)
DEFAULT_SERIES = [
    ("SOFR",   "Secured Overnight Financing Rate (SOFR)"),
    ("DGS5",   "5-Year Treasury Constant Maturity Rate"),
    ("DGS10",  "10-Year Treasury Constant Maturity Rate"),
    ("DGS30",  "30-Year Treasury Constant Maturity Rate"),
    ("DFF",    "Effective Federal Funds Rate"),
]

FRED_BASE_URL = "https://api.stlouisfed.org/fred/series/observations"

# Polite pause between API calls (FRED rate-limits aggressive callers)
REQUEST_DELAY = 0.5


def fetch_series(series_id: str, api_key: str, observation_start: str, observation_end: str) -> list[dict]:
    """
    Fetch all observations for one FRED series between two dates.
    Returns a list of dicts with keys: date, value (as float or None for missing).
    """
    params = {
        "series_id": series_id,
        "api_key": api_key,
        "file_type": "json",
        "observation_start": observation_start,
        "observation_end": observation_end,
        "sort_order": "asc",
    }

    resp = requests.get(FRED_BASE_URL, params=params, timeout=30)

    if resp.status_code == 400:
        # FRED returns 400 with a JSON error message for bad series IDs
        try:
            msg = resp.json().get("error_message", resp.text)
        except Exception:
            msg = resp.text
        raise ValueError(f"FRED API error for series '{series_id}': {msg}")

    resp.raise_for_status()
    data = resp.json()

    observations = []
    for obs in data.get("observations", []):
        raw_value = obs.get("value", ".")
        # FRED uses "." for missing values
        if raw_value == ".":
            value = None
        else:
            try:
                value = float(raw_value)
            except (ValueError, TypeError):
                value = None
        observations.append({"date": obs["date"], "value": value})

    return observations


def main():
    parser = argparse.ArgumentParser(
        description="Fetch FRED market rate data (SOFR, Treasuries, Fed Funds)"
    )
    parser.add_argument(
        "--api-key",
        default=os.environ.get("FRED_API_KEY"),
        help="FRED API key. Free at fred.stlouisfed.org. Or set FRED_API_KEY env var.",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=365,
        help="Number of calendar days of history to fetch (default: 365). Use 0 for all history.",
    )
    parser.add_argument(
        "--latest",
        action="store_true",
        help="Fetch only the most recent 7 days (quick refresh). Overrides --days.",
    )
    parser.add_argument(
        "--series",
        nargs="+",
        metavar="SERIES_ID",
        help="Specific FRED series IDs to fetch (default: all). E.g. --series SOFR DGS10",
    )
    args = parser.parse_args()

    if not args.api_key:
        print(
            "Error: FRED API key required.\n"
            "  Get a free key at: https://fred.stlouisfed.org/docs/api/api_key.html\n"
            "  Then pass it via --api-key or set the FRED_API_KEY environment variable."
        )
        sys.exit(1)

    # Resolve which series to fetch
    series_map = {sid: name for sid, name in DEFAULT_SERIES}
    if args.series:
        # User-specified series; use default name if known, otherwise use the series ID as name
        series_to_fetch = [(sid, series_map.get(sid, sid)) for sid in args.series]
    else:
        series_to_fetch = DEFAULT_SERIES

    # Date range
    end_date = date.today()
    if args.latest:
        start_date = end_date - timedelta(days=7)
    elif args.days == 0:
        start_date = date(2018, 1, 1)  # SOFR starts April 2018; earlier data won't exist
    else:
        start_date = end_date - timedelta(days=args.days)

    obs_start = start_date.isoformat()
    obs_end = end_date.isoformat()

    print("CD Command Center — FRED Rate Fetch")
    print(f"  Date range: {obs_start} → {obs_end}")
    print(f"  Series: {', '.join(sid for sid, _ in series_to_fetch)}")
    print()

    db.init_db()

    total_loaded = 0
    run_id = db.log_load_start("fred_rates")

    try:
        for series_id, series_name in series_to_fetch:
            print(f"  Fetching {series_id} ({series_name})...")

            try:
                observations = fetch_series(series_id, args.api_key, obs_start, obs_end)
            except ValueError as e:
                print(f"    Warning: {e} — skipping.")
                continue
            except requests.RequestException as e:
                print(f"    Error: {e} — skipping.")
                continue

            # Filter out missing values (FRED '.' entries) before inserting
            rows = [
                {
                    "series_id": series_id,
                    "series_name": series_name,
                    "rate_date": obs["date"],
                    "rate_value": obs["value"],
                }
                for obs in observations
                if obs["value"] is not None
            ]

            if not rows:
                print(f"    No data returned.")
                continue

            n = db.upsert_rows("market_rates", rows, unique_cols=["series_id", "rate_date"])
            total_loaded += n

            # Show most recent value as a sanity check
            latest = rows[-1]
            print(f"    Loaded {len(rows):,} observations. Latest: {latest['rate_date']} = {latest['rate_value']:.4f}%")

            time.sleep(REQUEST_DELAY)

    except Exception as e:
        db.log_load_finish(run_id, rows_loaded=total_loaded, error=str(e))
        raise

    db.log_load_finish(run_id, rows_loaded=total_loaded)
    print()
    print(f"Done. Total rows upserted: {total_loaded:,}")


if __name__ == "__main__":
    main()
