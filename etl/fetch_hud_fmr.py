"""
etl/fetch_hud_fmr.py — Load HUD Fair Market Rents (FMRs) into the hud_fmr table.

FMRs are the gross rents (rent + utilities) that HUD estimates represent the 40th
percentile of rents in a local housing market. They set the payment standard for
Section 8 vouchers and are used in NMTC and LIHTC deals to:
    - Test whether proposed rents are at or below FMR (a requirement for some programs)
    - Estimate market-rate rent comparables for rent-restricted units
    - Show the rent discount that a project's affordable units represent

Data source:
    HUD publishes FMRs annually via their public API (no account required):
        https://www.huduser.gov/hudapi/public/fmr

    FMRs are published at the county or HUD FMR area level for all bedroom sizes (0–4BR).

    Alternative — local Excel file:
    HUD also publishes Excel files at:
        https://www.huduser.gov/portal/datasets/fmr.html
    Download "FY{YEAR}_4050_FMRs_Final.xlsx" (or similar) and pass via --file.

Usage:
    # Fetch from HUD API (recommended — no account required):
    python etl/fetch_hud_fmr.py
    python etl/fetch_hud_fmr.py --year 2025
    python etl/fetch_hud_fmr.py --states CA TX NY

    # Load from a locally downloaded Excel file:
    python etl/fetch_hud_fmr.py --file data/raw/FY2025_4050_FMRs_Final.xlsx
    python etl/fetch_hud_fmr.py --file data/raw/FY2025_4050_FMRs_Final.xlsx --columns-only

Why 2-bedroom is the standard:
    The 2BR FMR is the conventional benchmark for housing program analyses because it
    matches the "payment standard" used for Section 8 vouchers and is the baseline for
    HUD's cost-of-housing comparisons across markets.
"""

import argparse
import os
import sys
import time
from datetime import date

import requests
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import db

HUD_API_BASE = "https://www.huduser.gov/hudapi/public/fmr"
REQUEST_DELAY = 0.3

ALL_STATES = [
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "DC", "FL",
    "GA", "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME",
    "MD", "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH",
    "NJ", "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI",
    "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY",
]


# ---------------------------------------------------------------------------
# HUD API fetch
# ---------------------------------------------------------------------------

def fetch_fmr_for_state(state: str, year: int, api_key: str = None) -> list[dict]:
    """
    Fetch FMRs for all counties/areas in a state via the HUD API.
    Returns a list of row dicts ready for upsert into hud_fmr.

    HUD API: GET /fmr/data/{state}?year={year}
    Requires a Bearer token from https://www.huduser.gov/hudapi/public/home
    Response: list of area objects with Efficiency, One-Bedroom, ..., Four-Bedroom fields.
    """
    headers = {"Authorization": f"Bearer {api_key}"} if api_key else {}
    resp = requests.get(
        f"{HUD_API_BASE}/data/{state}",
        params={"year": str(year)},
        headers=headers,
        timeout=30,
    )

    if resp.status_code == 404:
        return []
    resp.raise_for_status()

    payload = resp.json()
    # HUD response structure: {"data": {"metroareas": [...], "counties": [...]}}
    # or a flat list — handle both
    if isinstance(payload, list):
        areas = payload
    else:
        data = payload.get("data", payload)
        metro_areas = data.get("metroareas", [])
        counties    = data.get("counties", [])
        areas = metro_areas + counties

    rows = []
    for area in areas:
        fips      = area.get("fips_code") or area.get("fipsCode") or area.get("areaId", "")
        area_name = area.get("area_name") or area.get("areaName") or area.get("name", "")
        county    = area.get("county_name") or area.get("countyName") or ""

        def get_fmr(key_variants):
            for k in key_variants:
                val = area.get(k)
                if val is not None:
                    try:
                        return float(val)
                    except (TypeError, ValueError):
                        pass
            return None

        rows.append({
            "fiscal_year": year,
            "state": state,
            "fips": str(fips),
            "area_name": area_name,
            "county_name": county,
            "fmr_0br": get_fmr(["Efficiency", "efficiency", "fmr0", "br0"]),
            "fmr_1br": get_fmr(["One-Bedroom", "one_bedroom", "fmr1", "br1"]),
            "fmr_2br": get_fmr(["Two-Bedroom", "two_bedroom", "fmr2", "br2"]),
            "fmr_3br": get_fmr(["Three-Bedroom", "three_bedroom", "fmr3", "br3"]),
            "fmr_4br": get_fmr(["Four-Bedroom", "four_bedroom", "fmr4", "br4"]),
        })

    return rows


# ---------------------------------------------------------------------------
# Excel file load (alternative to API)
# ---------------------------------------------------------------------------

def load_from_excel(filepath: str, year: int, columns_only: bool = False) -> list[dict]:
    """
    Parse a locally downloaded HUD FMR Excel file.
    Handles the standard HUD FY{YEAR}_4050_FMRs_Final.xlsx format.
    """
    print(f"  Reading: {filepath}")
    df = pd.read_excel(filepath, dtype=str)
    print(f"  Rows: {len(df):,}")

    if columns_only:
        print("  Columns:")
        for col in df.columns:
            print(f"    {col}")
        return []

    cols_lower = {c.lower().strip(): c for c in df.columns}

    def find_col(*candidates):
        for c in candidates:
            if c in cols_lower:
                return cols_lower[c]
        return None

    fips_col    = find_col("fips", "fips_code", "area code")
    area_col    = find_col("area name", "areaname", "area_name", "metro area name", "county name")
    state_col   = find_col("state", "state_alpha", "stateabb")
    county_col  = find_col("county name", "county_name")

    br0_col = find_col("efficiency", "fmr_0", "zero br", "studio")
    br1_col = find_col("one-bedroom", "one bedroom", "fmr_1", "1br")
    br2_col = find_col("two-bedroom", "two bedroom", "fmr_2", "2br")
    br3_col = find_col("three-bedroom", "three bedroom", "fmr_3", "3br")
    br4_col = find_col("four-bedroom", "four bedroom", "fmr_4", "4br")

    if not fips_col:
        raise ValueError(
            f"Could not find FIPS column. Available: {list(df.columns)}. "
            "Pass --columns-only to inspect the file."
        )

    def to_float(row, col):
        if col is None:
            return None
        try:
            return float(str(row[col]).replace(",", "").replace("$", "").strip())
        except (ValueError, TypeError):
            return None

    rows = []
    for _, row in df.iterrows():
        rows.append({
            "fiscal_year": year,
            "state": str(row[state_col]).strip()  if state_col  else "",
            "fips":  str(row[fips_col]).strip().zfill(10),
            "area_name":   str(row[area_col]).strip()   if area_col   else "",
            "county_name": str(row[county_col]).strip() if county_col else "",
            "fmr_0br": to_float(row, br0_col),
            "fmr_1br": to_float(row, br1_col),
            "fmr_2br": to_float(row, br2_col),
            "fmr_3br": to_float(row, br3_col),
            "fmr_4br": to_float(row, br4_col),
        })

    return rows


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    current_year = date.today().year

    parser = argparse.ArgumentParser(
        description="Load HUD Fair Market Rents (FMRs) into the hud_fmr table"
    )
    parser.add_argument(
        "--year",
        type=int,
        default=current_year,
        help=f"HUD fiscal year to fetch (default: {current_year})",
    )
    parser.add_argument(
        "--states",
        nargs="+",
        metavar="STATE",
        help="State abbreviations to load (default: all). E.g. --states CA TX NY",
    )
    parser.add_argument(
        "--file",
        default=None,
        help="Path to a locally downloaded HUD FMR Excel file. "
             "If not provided, fetches from the HUD API.",
    )
    parser.add_argument(
        "--columns-only",
        action="store_true",
        help="Print column names from the Excel file and exit (use with --file).",
    )
    parser.add_argument(
        "--api-key",
        default=None,
        help="HUD USER API Bearer token. Register free at "
             "https://www.huduser.gov/hudapi/public/home. "
             "Can also be set via HUD_API_KEY env var.",
    )
    args = parser.parse_args()

    api_key = args.api_key or os.environ.get("HUD_API_KEY")

    print("CD Command Center — HUD FMR Load")
    print(f"  Fiscal year: {args.year}")

    db.init_db()
    run_id = db.log_load_start("hud_fmr")
    total_loaded = 0

    try:
        if args.file:
            # --- Excel file mode ---
            if not os.path.exists(args.file):
                print(f"Error: file not found: {args.file}")
                sys.exit(1)

            rows = load_from_excel(args.file, args.year, columns_only=args.columns_only)
            if args.columns_only:
                return

            if args.states:
                rows = [r for r in rows if r.get("state") in args.states]

            if not rows:
                print("  No rows to load after filtering.")
            else:
                total_loaded = db.upsert_rows("hud_fmr", rows, unique_cols=["fiscal_year", "fips"])
                print(f"  Loaded {total_loaded:,} FMR area records.")

        else:
            # --- API mode ---
            if not api_key:
                print(
                    "Error: HUD API requires a free token. Register at:\n"
                    "  https://www.huduser.gov/hudapi/public/home\n"
                    "Then pass --api-key YOUR_TOKEN or set HUD_API_KEY env var.\n"
                    "\nAlternatively, download the Excel file and use --file:\n"
                    "  https://www.huduser.gov/portal/datasets/fmr.html"
                )
                sys.exit(1)

            states = args.states if args.states else ALL_STATES
            print(f"  States: {', '.join(states)}")
            print()

            for state in states:
                print(f"  {state}...", end=" ", flush=True)
                try:
                    rows = fetch_fmr_for_state(state, args.year, api_key=api_key)
                except requests.RequestException as e:
                    print(f"Error: {e}")
                    continue

                if not rows:
                    print("no data")
                    continue

                n = db.upsert_rows("hud_fmr", rows, unique_cols=["fiscal_year", "fips"])
                total_loaded += n
                print(f"{len(rows):,} areas")
                time.sleep(REQUEST_DELAY)

    except Exception as e:
        db.log_load_finish(run_id, rows_loaded=total_loaded, error=str(e))
        raise

    db.log_load_finish(run_id, rows_loaded=total_loaded)
    print()
    print(f"Done. Total rows upserted: {total_loaded:,}")


if __name__ == "__main__":
    main()
