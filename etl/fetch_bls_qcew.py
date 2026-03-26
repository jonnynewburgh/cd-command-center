"""
etl/fetch_bls_qcew.py — Load BLS QCEW employment data by county and industry.

The Quarterly Census of Employment and Wages (QCEW) covers virtually all US jobs
and wages, by county and NAICS industry code. In CD deal origination and impact
analysis, QCEW is used to:
    - Show the economic base of a target market (which industries employ most workers)
    - Quantify job impact: a new facility adds X jobs to a county with Y total jobs
    - Identify high-unemployment industries in distressed markets
    - Support NMTC "community impact" narratives with employment data

Data source:
    BLS QCEW API (no key required for basic use):
        https://data.bls.gov/cew/apps/api_sample_code/v1/home.htm

    API endpoint: https://data.bls.gov/cew/data/api/{year}/{quarter}/area/{area_code}.json
    where area_code is the 5-digit county FIPS or 'US000' for national totals.

    Alternative — downloadable CSVs:
        https://www.bls.gov/cew/downloadable-data.htm
        Annual single-file CSVs are available; pass via --file for bulk loads.

Usage:
    # Fetch specific counties by FIPS from BLS API:
    python etl/fetch_bls_qcew.py --fips 06037 17031 36061 --year 2023 --quarter 4
    python etl/fetch_bls_qcew.py --fips 06037 --year 2023 --annual   # annual averages

    # Load a downloaded BLS QCEW CSV (bulk load, faster for many counties):
    python etl/fetch_bls_qcew.py --file data/raw/2023.q1-q4.by_area/2023.q4 06000.csv --year 2023
    python etl/fetch_bls_qcew.py --file data/raw/2023_annual_singlefile.csv --year 2023 --annual
    python etl/fetch_bls_qcew.py --file data/raw/2023_annual.csv --year 2023 --columns-only

    # Filter to just total-all-industries (ownership_code=0, industry_code=10):
    python etl/fetch_bls_qcew.py --fips 06037 --year 2023 --quarter 4 --totals-only

BLS QCEW API key: not required for individual area lookups; BLS rate-limits aggressive use.
"""

import argparse
import os
import sys
import time

import requests
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import db

BLS_QCEW_API_BASE = "https://data.bls.gov/cew/data/api"
REQUEST_DELAY = 0.5

# State abbreviation → 2-digit FIPS string (used to filter CSV rows by state)
STATE_ABBREV_TO_FIPS = {
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
}

# Ownership codes
OWNERSHIP_TOTAL   = "0"   # Total, all ownerships
OWNERSHIP_PRIVATE = "5"   # Private sector
OWNERSHIP_FEDERAL = "1"   # Federal government

# Industry codes we default to when --totals-only is set
TOTAL_INDUSTRY_CODE = "10"  # Total, all industries


# ---------------------------------------------------------------------------
# BLS API fetch
# ---------------------------------------------------------------------------

def fetch_qcew_area(area_fips: str, year: int, quarter: int | str) -> list[dict]:
    """
    Fetch QCEW data for one county/area from the BLS API.

    quarter: 1-4 for quarterly, or 'a' for annual averages.
    Returns a list of row dicts ready for upsert into bls_qcew.
    """
    q = str(quarter)
    url = f"{BLS_QCEW_API_BASE}/{year}/q{q}/area/{area_fips}.json"

    resp = requests.get(url, timeout=30)
    if resp.status_code == 404:
        return []
    resp.raise_for_status()

    data = resp.json()
    # BLS QCEW API returns: {"quarterly_data": [...]} or {"annual_data": [...]}
    records = data.get("quarterly_data") or data.get("annual_data") or []

    quarter_num = 0 if q == "a" else int(q)
    state_fips = area_fips[:2] if len(area_fips) >= 2 else None

    rows = []
    for rec in records:
        rows.append({
            "area_fips":      area_fips,
            "area_name":      rec.get("area_title"),
            "state":          state_fips,
            "year":           year,
            "quarter":        quarter_num,
            "industry_code":  str(rec.get("industry_code", "")),
            "industry_title": rec.get("industry_title"),
            "ownership_code": str(rec.get("own_code", "")),
            "establishments": _to_int(rec.get("qtrly_estabs") or rec.get("annual_avg_estabs")),
            "employment":     _to_int(rec.get("month3_emplvl") or rec.get("annual_avg_emplvl")),
            "total_wages":    _to_float(rec.get("total_qtrly_wages") or rec.get("total_annual_wages")),
            "avg_weekly_wage": _to_float(rec.get("avg_wkly_wage")),
        })

    return rows


def _to_int(val):
    try:
        return int(str(val).replace(",", "").strip())
    except (TypeError, ValueError):
        return None


def _to_float(val):
    try:
        return float(str(val).replace(",", "").strip())
    except (TypeError, ValueError):
        return None


# ---------------------------------------------------------------------------
# CSV file load (alternative to API)
# ---------------------------------------------------------------------------

# QCEW CSV column mapping (BLS uses consistent names in their downloadable CSVs)
QCEW_CSV_COLS = {
    "area_fips":      ["area_fips", "FIPS"],
    "area_name":      ["area_title", "Area Title"],
    "year":           ["year", "Year"],
    "quarter":        ["qtr", "Quarter", "Qtr"],
    "industry_code":  ["industry_code", "Industry Code"],
    "industry_title": ["industry_title", "Industry Title"],
    "ownership_code": ["own_code", "Ownership Code"],
    "establishments": ["qtrly_estabs", "annual_avg_estabs", "Establishments"],
    "employment":     ["month3_emplvl", "annual_avg_emplvl", "Employment"],
    "total_wages":    ["total_qtrly_wages", "total_annual_wages", "Total Wages"],
    "avg_weekly_wage": ["avg_wkly_wage", "Avg Weekly Wage"],
}


def find_col(df, candidates):
    cols_lower = {c.lower().strip(): c for c in df.columns}
    for c in candidates:
        if c in df.columns:
            return c
        if c.lower().strip() in cols_lower:
            return cols_lower[c.lower().strip()]
    return None


def load_from_csv(filepath: str, year: int, is_annual: bool, states: list[str],
                  totals_only: bool, columns_only: bool, chunksize: int = 100_000) -> int:
    """Load a BLS QCEW downloadable CSV in chunks."""
    print(f"  Reading: {filepath}")
    total = 0
    chunk_num = 0
    col_map = None

    for chunk in pd.read_csv(filepath, dtype=str, encoding="latin-1",
                              low_memory=False, chunksize=chunksize):
        chunk_num += 1

        if chunk_num == 1:
            if columns_only:
                print("  Columns:")
                for c in chunk.columns:
                    print(f"    {c}")
                return 0
            col_map = {k: find_col(chunk, v) for k, v in QCEW_CSV_COLS.items()}

        if totals_only:
            own_col = col_map.get("ownership_code")
            ind_col = col_map.get("industry_code")
            if own_col:
                chunk = chunk[chunk[own_col].isin([OWNERSHIP_TOTAL, OWNERSHIP_PRIVATE])]
            if ind_col:
                chunk = chunk[chunk[ind_col] == TOTAL_INDUSTRY_CODE]

        # State filter via area_fips prefix (first 2 digits = state FIPS)
        if states:
            fips_col = col_map.get("area_fips")
            if fips_col:
                state_fips_set = {STATE_ABBREV_TO_FIPS[s] for s in states if s in STATE_ABBREV_TO_FIPS}
                chunk = chunk[chunk[fips_col].str[:2].isin(state_fips_set)]

        if chunk.empty:
            continue

        def get(row, key):
            c = col_map.get(key)
            return str(row[c]).strip() if c and pd.notna(row.get(c)) else None

        quarter_col = col_map.get("quarter")
        rows = []
        for _, row in chunk.iterrows():
            q_raw = get(row, "quarter")
            if q_raw == "A" or q_raw is None:
                quarter_num = 0
            else:
                try:
                    quarter_num = int(q_raw)
                except ValueError:
                    quarter_num = 0

            area_fips = get(row, "area_fips") or ""
            rows.append({
                "area_fips":      area_fips,
                "area_name":      get(row, "area_name"),
                "state":          area_fips[:2] if area_fips else None,
                "year":           year,
                "quarter":        quarter_num,
                "industry_code":  get(row, "industry_code"),
                "industry_title": get(row, "industry_title"),
                "ownership_code": get(row, "ownership_code"),
                "establishments": _to_int(get(row, "establishments")),
                "employment":     _to_int(get(row, "employment")),
                "total_wages":    _to_float(get(row, "total_wages")),
                "avg_weekly_wage": _to_float(get(row, "avg_weekly_wage")),
            })

        rows = [r for r in rows if r["area_fips"] and r["industry_code"]]
        if rows:
            n = db.upsert_rows(
                "bls_qcew", rows,
                unique_cols=["area_fips", "year", "quarter", "industry_code", "ownership_code"]
            )
            total += n

        if chunk_num % 20 == 0:
            print(f"    Processed chunk {chunk_num} ({total:,} rows so far)...")

    return total


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Load BLS QCEW employment and wage data by county and industry"
    )
    parser.add_argument(
        "--fips",
        nargs="+",
        metavar="FIPS",
        help="5-digit county FIPS codes to fetch from the BLS API. E.g. --fips 06037 17031",
    )
    parser.add_argument(
        "--year",
        type=int,
        required=True,
        help="Year to fetch (e.g. 2023)",
    )
    parser.add_argument(
        "--quarter",
        type=int,
        choices=[1, 2, 3, 4],
        default=None,
        help="Quarter to fetch (1-4). Use with --fips. Omit when using --annual.",
    )
    parser.add_argument(
        "--annual",
        action="store_true",
        help="Fetch annual average data instead of a single quarter.",
    )
    parser.add_argument(
        "--file",
        default=None,
        help="Path to a downloaded BLS QCEW CSV file (faster for many counties).",
    )
    parser.add_argument(
        "--states",
        nargs="+",
        metavar="STATE",
        help="Filter CSV load to specific states by FIPS prefix (use with --file).",
    )
    parser.add_argument(
        "--totals-only",
        action="store_true",
        dest="totals_only",
        help="Only load total-all-industries rows (ownership 0 or 5, industry 10). "
             "Reduces row count significantly.",
    )
    parser.add_argument(
        "--columns-only",
        action="store_true",
        help="Print column names from the CSV and exit (use with --file).",
    )
    args = parser.parse_args()

    if not args.fips and not args.file:
        print("Error: provide --fips (API mode) or --file (CSV mode).")
        sys.exit(1)

    if args.fips and not args.annual and not args.quarter:
        print("Error: provide --quarter (1-4) or --annual when using --fips.")
        sys.exit(1)

    print("CD Command Center — BLS QCEW Load")
    print(f"  Year: {args.year}")
    if args.fips:
        q_label = "annual" if args.annual else f"Q{args.quarter}"
        print(f"  Period: {q_label}")
        print(f"  Counties: {', '.join(args.fips)}")
    print()

    db.init_db()
    run_id = db.log_load_start("bls_qcew")
    total_loaded = 0

    try:
        if args.file:
            if not os.path.exists(args.file):
                print(f"Error: file not found: {args.file}")
                sys.exit(1)
            total_loaded = load_from_csv(
                filepath=args.file,
                year=args.year,
                is_annual=args.annual,
                states=args.states or [],
                totals_only=args.totals_only,
                columns_only=args.columns_only,
            )
            if args.columns_only:
                return

        else:
            quarter = "a" if args.annual else args.quarter
            for fips in args.fips:
                print(f"  County {fips}...", end=" ", flush=True)
                try:
                    rows = fetch_qcew_area(fips, args.year, quarter)
                except requests.RequestException as e:
                    print(f"Error: {e}")
                    continue

                if not rows:
                    print("no data")
                    continue

                if args.totals_only:
                    rows = [
                        r for r in rows
                        if r.get("ownership_code") in (OWNERSHIP_TOTAL, OWNERSHIP_PRIVATE)
                        and r.get("industry_code") == TOTAL_INDUSTRY_CODE
                    ]

                n = db.upsert_rows(
                    "bls_qcew", rows,
                    unique_cols=["area_fips", "year", "quarter", "industry_code", "ownership_code"]
                )
                total_loaded += n
                print(f"{len(rows):,} rows")
                time.sleep(REQUEST_DELAY)

    except Exception as e:
        db.log_load_finish(run_id, rows_loaded=total_loaded, error=str(e))
        raise

    db.log_load_finish(run_id, rows_loaded=total_loaded)
    print()
    print(f"Done. Total rows upserted: {total_loaded:,}")


if __name__ == "__main__":
    main()
