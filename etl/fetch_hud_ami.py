"""
etl/fetch_hud_ami.py — Load HUD Area Median Income (AMI) limits into the hud_ami table.

AMI limits are the foundation of affordable housing underwriting. Every rent-restricted
unit in an LIHTC or NMTC deal is priced at a percentage of AMI (30%, 50%, 60%, 80%,
120%). This script loads HUD's official income limit data so deal underwriters can
quickly check whether a proposed project meets program thresholds.

Data source:
    HUD publishes income limits annually. Two options:

    Option A — HUD USER API (requires free API token):
        Register at https://www.huduser.gov/hudapi/public/home to get a Bearer token.
        Set it via --api-key or the HUD_API_KEY env var.
        Endpoint: https://www.huduser.gov/hudapi/public/il

    Option B — local Excel file (recommended, no account needed):
        Download from https://www.huduser.gov/portal/datasets/il.html
        Look for "FY2025 Income Limits" → download the Excel file.
        Pass via --file: python etl/fetch_hud_ami.py --file data/raw/Section8-FY25.xlsx

    The API returns limits by HUD area (county or metro) for all family sizes (1–8 persons).
    This script stores 4-person family limits as the primary benchmark columns, plus
    a JSON blob of all family sizes for less common lookups.

Usage:
    # Load from a locally downloaded HUD Excel file (recommended):
    # Download from https://www.huduser.gov/portal/datasets/il.html
    python etl/fetch_hud_ami.py --file data/raw/Section8-FY25.xlsx
    python etl/fetch_hud_ami.py --file data/raw/Section8-FY25.xlsx --columns-only

    # Fetch from HUD API (requires free token from huduser.gov/hudapi/public/home):
    python etl/fetch_hud_ami.py --api-key YOUR_HUD_TOKEN
    python etl/fetch_hud_ami.py --api-key YOUR_HUD_TOKEN --year 2024
    python etl/fetch_hud_ami.py --api-key YOUR_HUD_TOKEN --states CA TX NY
    # Or set HUD_API_KEY env var and omit --api-key

Notes on AMI thresholds:
    30% AMI  = Extremely Low Income (ELI) — deepest subsidy housing
    50% AMI  = Very Low Income (VLI) — Section 8 standard
    80% AMI  = Low Income — most common LIHTC/NMTC affordable housing threshold
    120% AMI = Middle income (not in HUD data; computed here as median_income * 1.2)
               Used in workforce housing and some NMTC deals
"""

import argparse
import json
import os
import sys
import time
from datetime import date

import requests
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import db

HUD_API_BASE = "https://www.huduser.gov/hudapi/public/il"
REQUEST_DELAY = 0.3

# US state abbreviations (+ DC and territories HUD covers)
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

def fetch_ami_for_state(state: str, year: int, api_key: str = None) -> list[dict]:
    """
    Fetch AMI limits for all HUD areas in a state via the HUD API.
    Returns a list of row dicts ready for upsert into hud_ami.

    The HUD API endpoint: GET /il/data?stateId={STATE}&year={YEAR}
    Requires a Bearer token from https://www.huduser.gov/hudapi/public/home
    Response: list of area objects, each with il30, il50, il80 for family sizes 1-8.
    """
    params = {"stateId": state, "year": str(year)}
    headers = {"Authorization": f"Bearer {api_key}"} if api_key else {}
    resp = requests.get(f"{HUD_API_BASE}/data", params=params, headers=headers, timeout=30)

    if resp.status_code == 404:
        return []  # State or year not available
    resp.raise_for_status()

    data = resp.json()
    areas = data if isinstance(data, list) else data.get("data", [])

    rows = []
    for area in areas:
        # HUD returns limits for all family sizes. We pull family size 4 (index 3, 0-based)
        # as the standard benchmark, and store all sizes as JSON.
        il30 = area.get("il30", {})
        il50 = area.get("il50", {})
        il80 = area.get("il80", {})

        # Family size 4 benchmark (keys are "p1" through "p8" in the HUD response)
        limit_30 = _get_family4(il30)
        limit_50 = _get_family4(il50)
        limit_80 = _get_family4(il80)

        # Median income: HUD publishes it directly in some endpoints; otherwise back-calculate
        # from 80% limit (80% limit = 80% of median, so median = limit_80 / 0.8)
        median_raw = area.get("median_income") or area.get("medianIncome")
        if median_raw:
            median_income = float(median_raw)
        elif limit_80:
            median_income = round(limit_80 / 0.80)
        else:
            median_income = None

        limit_120 = round(median_income * 1.20) if median_income else None

        # Build the full family-size JSON for completeness
        limits_by_size = {}
        for i in range(1, 9):
            key = f"p{i}"
            limits_by_size[str(i)] = {
                "30": il30.get(key),
                "50": il50.get(key),
                "80": il80.get(key),
            }

        fips = area.get("fips_code") or area.get("fipsCode") or area.get("areaId", "")
        area_name = area.get("area_name") or area.get("areaName") or area.get("name", "")
        county_name = area.get("county_name") or area.get("countyName") or ""

        rows.append({
            "fiscal_year": year,
            "state": state,
            "fips": str(fips),
            "area_name": area_name,
            "county_name": county_name,
            "median_income": median_income,
            "limit_30_pct": limit_30,
            "limit_50_pct": limit_50,
            "limit_80_pct": limit_80,
            "limit_120_pct": limit_120,
            "limits_json": json.dumps(limits_by_size),
        })

    return rows


def _get_family4(limits_dict: dict):
    """Pull the family-size-4 value from a HUD limits dict (key 'p4')."""
    val = limits_dict.get("p4")
    if val is None:
        return None
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


# ---------------------------------------------------------------------------
# Excel file load (alternative to API)
# ---------------------------------------------------------------------------

def load_from_excel(filepath: str, year: int, columns_only: bool = False) -> list[dict]:
    """
    Parse a locally downloaded HUD income limits Excel file.
    Handles the standard HUD Section8-FY{YY}.xlsx format.
    """
    print(f"  Reading: {filepath}")
    df = pd.read_excel(filepath, dtype=str)
    print(f"  Rows: {len(df):,}")

    if columns_only:
        print("  Columns:")
        for col in df.columns:
            print(f"    {col}")
        return []

    # HUD Excel column names vary slightly by year. Try to detect key columns.
    cols_lower = {c.lower().strip(): c for c in df.columns}

    def find_col(*candidates):
        for c in candidates:
            if c in cols_lower:
                return cols_lower[c]
        return None

    fips_col    = find_col("fips", "fips_code", "state_alpha", "metro code")
    area_col    = find_col("area name", "areaname", "area_name", "metro area name")
    state_col   = find_col("state_alpha", "state", "stateabb")
    county_col  = find_col("county name", "countyname", "county_name")

    # Income limits: HUD uses l30_p4, l50_p4, l80_p4 (limit at 30/50/80%, 4-person family)
    l30_col = find_col("l30_p4", "lim30_p4", "30% p4", "vli_p4")
    l50_col = find_col("l50_p4", "lim50_p4", "50% p4")
    l80_col = find_col("l80_p4", "lim80_p4", "80% p4")
    med_col = find_col("median", "median income", "median_income", "ami")

    if not fips_col:
        raise ValueError(
            f"Could not find FIPS column. Available: {list(df.columns)}. "
            "Pass --columns-only to inspect the file."
        )

    rows = []
    for _, row in df.iterrows():
        fips      = str(row[fips_col]).strip().zfill(10) if fips_col else ""
        area_name = str(row[area_col]).strip()           if area_col  else ""
        state     = str(row[state_col]).strip()          if state_col else ""
        county    = str(row[county_col]).strip()         if county_col else ""

        def to_float(col):
            if col is None:
                return None
            val = row[col]
            try:
                return float(str(val).replace(",", "").strip())
            except (ValueError, TypeError):
                return None

        limit_30 = to_float(l30_col)
        limit_50 = to_float(l50_col)
        limit_80 = to_float(l80_col)
        median   = to_float(med_col)

        if median is None and limit_80 is not None:
            median = round(limit_80 / 0.80)

        limit_120 = round(median * 1.20) if median else None

        rows.append({
            "fiscal_year": year,
            "state": state,
            "fips": fips,
            "area_name": area_name,
            "county_name": county,
            "median_income": median,
            "limit_30_pct": limit_30,
            "limit_50_pct": limit_50,
            "limit_80_pct": limit_80,
            "limit_120_pct": limit_120,
            "limits_json": None,  # Full family-size JSON not available from Excel
        })

    return rows


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    current_year = date.today().year

    parser = argparse.ArgumentParser(
        description="Load HUD Area Median Income (AMI) limits into hud_ami table"
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
        help="Path to a locally downloaded HUD income limits Excel file. "
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

    # Resolve API key from arg or env var
    api_key = args.api_key or os.environ.get("HUD_API_KEY")

    print("CD Command Center — HUD AMI Load")
    print(f"  Fiscal year: {args.year}")

    db.init_db()
    run_id = db.log_load_start("hud_ami")
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
                total_loaded = db.upsert_rows("hud_ami", rows, unique_cols=["fiscal_year", "fips"])
                print(f"  Loaded {total_loaded:,} area AMI records.")

        else:
            # --- API mode ---
            if not api_key:
                print(
                    "Error: HUD API requires a free token. Register at:\n"
                    "  https://www.huduser.gov/hudapi/public/home\n"
                    "Then pass --api-key YOUR_TOKEN or set HUD_API_KEY env var.\n"
                    "\nAlternatively, download the Excel file and use --file:\n"
                    "  https://www.huduser.gov/portal/datasets/il.html"
                )
                sys.exit(1)

            states = args.states if args.states else ALL_STATES
            print(f"  States: {', '.join(states)}")
            print()

            for state in states:
                print(f"  {state}...", end=" ", flush=True)
                try:
                    rows = fetch_ami_for_state(state, args.year, api_key=api_key)
                except requests.RequestException as e:
                    print(f"Error: {e}")
                    continue

                if not rows:
                    print("no data")
                    continue

                n = db.upsert_rows("hud_ami", rows, unique_cols=["fiscal_year", "fips"])
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
