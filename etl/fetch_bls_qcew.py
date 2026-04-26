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

# The old data.bls.gov/cew/data/api endpoint is no longer accessible.
# We now use the BLS public timeseries API, which supports QCEW employment series.
# Note: the timeseries API only provides employment counts (ENU series).
# Wages and establishment counts are not available via this endpoint.
# For full metrics, download the annual CSV from bls.gov/cew/downloadable-data.htm
# and use --file mode.
BLS_TIMESERIES_API = "https://api.bls.gov/publicAPI/v2/timeseries/data/"
# Without a key: 25 queries/day limit. With a free key: 500 queries/day.
# Register free at: https://data.bls.gov/registrationEngine/
# Pass via --bls-key or BLS_API_KEY env var.
BLS_API_BATCH_SIZE = 50   # max series per request (same with or without key)
REQUEST_DELAY = 0.3

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

def _qcew_series_id(area_fips: str, ownership: str, industry: str) -> str:
    """
    Build a BLS QCEW employment series ID for the timeseries API.

    Format: ENU + fips(5) + size(1=5, all sizes) + ownership(1) + industry(3 padded)
    Ownership: "0"=total all, "5"=private sector
    Industry:  "10"=total all industries, "510"=private sector total

    Known working series:
      ENU{fips}50010 -> total employment, all industries, all ownerships
      ENU{fips}50510 -> total employment, all industries, private sector
    """
    ind_padded = industry.zfill(3)
    return f"ENU{area_fips}5{ownership}{ind_padded}"


def fetch_qcew_areas(fips_list: list[str], year: int, bls_key: str = None) -> dict[str, list[dict]]:
    """
    Fetch annual QCEW employment totals for a batch of counties via the BLS
    public timeseries API.

    Returns {area_fips: [row_dict, ...]} — two rows per county (total + private).

    Note: the BLS timeseries API only provides employment counts for QCEW.
    Wages and establishment counts are only available via CSV download.
    """
    # Build series IDs: total (own=0, ind=010) and private (own=0, ind=510)
    series_map = {}   # series_id -> (fips, ownership_code, industry_code)
    for fips in fips_list:
        for own, ind in [("0", "10"), ("0", "510")]:
            sid = _qcew_series_id(fips, own, ind)
            series_map[sid] = (fips, own, ind)

    results: dict[str, list[dict]] = {f: [] for f in fips_list}

    # BLS allows up to 50 series per request without a key
    series_ids = list(series_map.keys())
    for i in range(0, len(series_ids), BLS_API_BATCH_SIZE):
        batch = series_ids[i:i + BLS_API_BATCH_SIZE]
        payload = {
            "seriesid": batch,
            "startyear": str(year),
            "endyear": str(year),
        }
        if bls_key:
            payload["registrationkey"] = bls_key
        resp = requests.post(
            BLS_TIMESERIES_API,
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=30,
        )
        resp.raise_for_status()
        d = resp.json()

        for series in d.get("Results", {}).get("series", []):
            sid = series["seriesID"]
            fips, own, ind = series_map.get(sid, (None, None, None))
            if not fips:
                continue
            # Annual data has period="A01"
            annual = [pt for pt in series.get("data", []) if pt.get("period") == "A01"]
            if not annual:
                continue
            emp_val = _to_int(annual[0]["value"])
            row = {
                "area_fips":       fips,
                "area_name":       None,   # not returned by timeseries API
                "state":           fips[:2],
                "year":            year,
                "quarter":         0,      # 0 = annual
                "industry_code":   ind,
                "industry_title":  "Total, all industries" if ind == "10" else "Private sector",
                "ownership_code":  own,
                "establishments":  None,   # not available via timeseries API
                "employment":      emp_val,
                "total_wages":     None,   # not available via timeseries API
                "avg_weekly_wage": None,   # not available via timeseries API
            }
            results[fips].append(row)

        time.sleep(REQUEST_DELAY)

    return results


def fetch_qcew_area(area_fips: str, year: int, quarter: int | str) -> list[dict]:
    """
    Fetch QCEW employment for a single county. Wrapper around fetch_qcew_areas.
    quarter is accepted for API compatibility but annual data is always returned
    (the BLS timeseries API only publishes annual QCEW aggregates).
    """
    results = fetch_qcew_areas([area_fips], year)
    return results.get(area_fips, [])


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
        "--all-counties",
        action="store_true",
        dest="all_counties",
        help="Fetch all counties found in the census_tracts table (national load via API).",
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
    parser.add_argument(
        "--bls-key",
        default=os.environ.get("BLS_API_KEY", ""),
        dest="bls_key",
        metavar="KEY",
        help="BLS API registration key (free at data.bls.gov/registrationEngine/). "
             "Required for national loads (25 req/day without key, 500 with). "
             "Also reads from BLS_API_KEY env var.",
    )
    args = parser.parse_args()

    # --all-counties: pull every county FIPS from the census_tracts table
    if args.all_counties:
        db.init_db()
        con = db.get_connection()
        cur = con.cursor()
        cur.execute(
            "SELECT DISTINCT SUBSTR(census_tract_id, 1, 5) FROM census_tracts "
            "WHERE census_tract_id IS NOT NULL AND LENGTH(census_tract_id) >= 5 "
            "ORDER BY 1"
        )
        county_fips = [r[0] for r in cur.fetchall() if r[0]]
        con.close()
        if not county_fips:
            print("Error: no census tracts in DB. Run load_census_tracts.py first.")
            sys.exit(1)
        args.fips = county_fips
        print(f"  --all-counties: {len(args.fips)} counties found in census_tracts table")

    if not args.fips and not args.file:
        print("Error: provide --fips / --all-counties (API mode) or --file (CSV mode).")
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
            print(f"  Fetching {len(args.fips)} counties via BLS timeseries API...")
            print("  (Note: API provides employment only; wages/establishments require CSV)")
            try:
                area_results = fetch_qcew_areas(args.fips, args.year,
                                                bls_key=args.bls_key or None)
            except requests.RequestException as e:
                raise RuntimeError(f"BLS API request failed: {e}") from e

            counties_done = 0
            for fips, rows in area_results.items():
                if not rows:
                    counties_done += 1
                    continue

                if args.totals_only:
                    rows = [
                        r for r in rows
                        if r.get("ownership_code") in (OWNERSHIP_TOTAL, OWNERSHIP_PRIVATE)
                        and r.get("industry_code") in (TOTAL_INDUSTRY_CODE, "510")
                    ]

                n = db.upsert_rows(
                    "bls_qcew", rows,
                    unique_cols=["area_fips", "year", "quarter", "industry_code", "ownership_code"]
                )
                total_loaded += n
                counties_done += 1
                if len(area_results) > 50:
                    # For large loads, print a summary every 100 counties instead of every county
                    if counties_done % 100 == 0 or counties_done == len(area_results):
                        print(f"  {counties_done}/{len(area_results)} counties done, {total_loaded:,} rows loaded")
                else:
                    print(f"  County {fips}: {n} rows")

    except Exception as e:
        db.log_load_finish(run_id, rows_loaded=total_loaded, error=str(e))
        raise

    db.log_load_finish(run_id, rows_loaded=total_loaded)
    print()
    print(f"Done. Total rows upserted: {total_loaded:,}")


if __name__ == "__main__":
    main()
