"""
etl/fetch_lea_accountability.py — Fetch LEA (district) academic accountability
data and load into the lea_accountability table.

DATA SOURCES:
  1. Urban Institute Education Data Portal API (no key required)
     https://educationdata.urban.org/api/v1/
     - EDFacts grad-rates: 4-year ACGR graduation rates by district
       Endpoint: /school-districts/edfacts/grad-rates/{year}/
       NOTE: Only 2019 is currently functional; 2020-2022 return HTTP 500
             on the Urban Institute's server (confirmed as of 2026-03).
     - EDFacts assessments (math/reading proficiency): CURRENTLY UNAVAILABLE
       The /school-districts/edfacts/assessments/ endpoint returns HTTP 500
       for all years, grades, and states. This is a server-side bug at
       Urban Institute. Proficiency fields are left null until this is fixed.

  2. Scope is limited to the LEA IDs already in our schools table. We pull
     by FIPS (state) code to minimize API calls (~50 requests per data type
     instead of ~17K individual lookups).

WHAT WE STORE vs. WHAT WE DON'T:
  - proficiency_math, proficiency_reading: NOT POPULATED — EDFacts assessments
    endpoint returns HTTP 500 (server-side bug at Urban Institute, 2026-03).
    Will populate once the endpoint is restored.
  - graduation_rate: STORED — 4-year adjusted cohort graduation rate (ACGR)
    midpoint from EDFacts. Only SY 2018-19 data is available via this API;
    newer years (2020-2022) also return HTTP 500.
  - accountability_score, accountability_rating: NOT POPULATED here — these are
    state-specific A-F or numerical ratings with no national standard. Each state
    publishes its own report card; there is no national API for them. Leave null.

RUNNING TIME:
  ~2–5 minutes. Queries one FIPS code at a time (50 state iterations)
  with a short sleep between requests.

Usage:
    python etl/fetch_lea_accountability.py               # all states (grad rates only)
    python etl/fetch_lea_accountability.py --grad-year 2019  # year (2019 is only working year)
    python etl/fetch_lea_accountability.py --states CA TX    # specific states
    python etl/fetch_lea_accountability.py --dry-run         # print rows, no DB write
"""

import argparse
import sys
import os
import time

import requests

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import db

# ---------------------------------------------------------------------------
# Urban Institute Education Data Portal API
# Docs: https://educationdata.urban.org/documentation/
# ---------------------------------------------------------------------------
BASE_URL = "https://educationdata.urban.org/api/v1"

# Latest year available for each endpoint (update as NCES publishes new data)
DEFAULT_ASSESSMENT_YEAR = 2019   # EDFacts SY 2018-19 (2020+ return HTTP 500)
DEFAULT_GRAD_YEAR       = 2019   # EDFacts ACGR SY 2018-19 (2020+ return HTTP 500)

API_PAGE_SIZE  = 10000   # records per request (Urban Institute max)
API_SLEEP      = 0.5     # seconds between requests

# Map 2-letter state abbreviation → 2-digit FIPS code
_FIPS = {
    "AL":"01","AK":"02","AZ":"04","AR":"05","CA":"06","CO":"08","CT":"09",
    "DE":"10","DC":"11","FL":"12","GA":"13","HI":"15","ID":"16","IL":"17",
    "IN":"18","IA":"19","KS":"20","KY":"21","LA":"22","ME":"23","MD":"24",
    "MA":"25","MI":"26","MN":"27","MS":"28","MO":"29","MT":"30","NE":"31",
    "NV":"32","NH":"33","NJ":"34","NM":"35","NY":"36","NC":"37","ND":"38",
    "OH":"39","OK":"40","OR":"41","PA":"42","RI":"44","SC":"45","SD":"46",
    "TN":"47","TX":"48","UT":"49","VT":"50","VA":"51","WA":"53","WV":"54",
    "WI":"55","WY":"56","AS":"60","GU":"66","MP":"69","PR":"72","VI":"78",
}
_FIPS_REV = {v: k for k, v in _FIPS.items()}


# ---------------------------------------------------------------------------
# API helpers
# ---------------------------------------------------------------------------

def _get_all_pages(url: str, params: dict) -> list[dict]:
    """
    Fetch all pages from the Urban Institute API for a given endpoint.
    Handles the 'next' cursor pattern or offset-based pagination.
    """
    params = {**params, "limit": API_PAGE_SIZE}
    results = []
    offset = 0

    while True:
        params["offset"] = offset
        try:
            resp = requests.get(url, params=params, timeout=30)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            print(f"    Warning: request failed — {e}")
            break

        records = data.get("results", [])
        results.extend(records)

        count = data.get("count", 0)
        if offset + len(records) >= count or not records:
            break
        offset += len(records)

    return results


def _fetch_grad_rates(fips: str, year: int) -> list[dict]:
    """
    Fetch EDFacts 4-year ACGR graduation rates for districts in one state.
    Returns list of dicts including leaid, grad_rate_midpt.

    NOTE: Only year=2019 is currently functional via the Urban Institute API.
    Years 2020-2022 return HTTP 500 (server-side bug, confirmed 2026-03).
    """
    url = f"{BASE_URL}/school-districts/edfacts/grad-rates/{year}/"
    params = {
        "fips":  fips,
        "race":  99,    # all students
        "sex":   9,     # all sexes
    }
    rows = _get_all_pages(url, params)
    time.sleep(API_SLEEP)
    return rows


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def fetch_lea_accountability(
    states=None,
    grad_year=DEFAULT_GRAD_YEAR,
    dry_run=False,
):
    conn = db.get_connection()
    cur = conn.cursor()

    # Get all real LEA IDs from our schools table (exclude synthetic placeholders)
    cur.execute(
        "SELECT DISTINCT lea_id, lea_name, state FROM schools "
        "WHERE lea_id IS NOT NULL AND lea_id NOT LIKE 'LEA%' "
        "ORDER BY state, lea_id"
    )
    all_leas = cur.fetchall()
    conn.close()

    # Index by leaid for fast lookup
    lea_info = {row["lea_id"]: {"lea_name": row["lea_name"], "state": row["state"]}
                for row in all_leas}

    # Determine which FIPS codes to query
    if states:
        fips_list = [_FIPS[s.upper()] for s in states if s.upper() in _FIPS]
    else:
        # Derive FIPS from lea_id prefix (first 2 digits = FIPS state code)
        fips_list = sorted(set(lid[:2] for lid in lea_info if lid[:2].isdigit()))

    print(f"  LEA IDs in database:    {len(lea_info):,}")
    print(f"  States to query:        {len(fips_list)}")
    print(f"  Graduation rate year:   {grad_year}")
    print(f"  Proficiency data:       unavailable (Urban Institute API returning 500)")
    print()

    # Accumulate data keyed by leaid
    # Structure: {leaid: {grad_rate, lea_name, state}}
    records: dict[str, dict] = {}

    # --- Graduation rates ---
    print("Fetching graduation rates (EDFacts ACGR)...")
    grad_total = 0
    for fips in fips_list:
        state = _FIPS_REV.get(fips, fips)
        rows = _fetch_grad_rates(fips, grad_year)
        matched = 0
        for row in rows:
            leaid = str(row.get("leaid", "")).zfill(7)
            if leaid not in lea_info:
                continue
            rate = row.get("grad_rate_midpt")
            if rate is None or float(rate) < 0:   # negative = NCES suppression code
                continue
            if leaid not in records:
                records[leaid] = {}
            records[leaid]["graduation_rate"] = float(rate)
            matched += 1
        grad_total += matched
        if matched:
            print(f"  {state}: {matched} districts")

    print(f"  Total grad rate records matched: {grad_total:,}")
    print()

    # --- Build final rows and upsert ---
    print(f"Building {len(records):,} LEA accountability records...")

    upserted = 0
    for leaid, data in records.items():
        info = lea_info.get(leaid, {})
        row = {
            "lea_id":             leaid,
            "lea_name":           info.get("lea_name"),
            "state":              info.get("state"),
            "graduation_rate":    data.get("graduation_rate"),
            "data_year":          grad_year,
        }

        # Only insert rows with at least one real data point
        if row["graduation_rate"] is None:
            continue

        if dry_run:
            grad_s = f"{row['graduation_rate']:.1f}%"
            print(f"  {leaid}  {(row['lea_name'] or '')[:40]:<40} {row['state']}  "
                  f"grad={grad_s}")
            upserted += 1
            continue

        _upsert_lea(row)
        upserted += 1

    return upserted


def _upsert_lea(row: dict):
    conn = db.get_connection()
    cur = conn.cursor()
    cols = [k for k, v in row.items() if v is not None]
    vals = [row[k] for k in cols]
    placeholders = ",".join("?" * len(vals))
    update = ",".join(
        f"{c}=excluded.{c}" for c in cols if c != "lea_id"
    )
    cur.execute(
        db.adapt_sql(
            f"INSERT INTO lea_accountability ({','.join(cols)}) VALUES ({placeholders}) "
            f"ON CONFLICT(lea_id, data_year) DO UPDATE SET {update}, created_at=CURRENT_TIMESTAMP"
        ),
        vals,
    )
    conn.commit()
    conn.close()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Fetch LEA academic accountability data from NCES via Urban Institute API"
    )
    parser.add_argument(
        "--grad-year", type=int, default=DEFAULT_GRAD_YEAR,
        help=f"Graduation rate year to fetch (default: {DEFAULT_GRAD_YEAR})",
    )
    parser.add_argument(
        "--states", nargs="+", metavar="ST",
        help="Limit to specific states (e.g. CA TX NY)",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Print rows without writing to database",
    )
    args = parser.parse_args()

    db.init_db()

    print("CD Command Center — LEA Accountability Data")
    print(f"  Source:  Urban Institute Education Data Portal (NCES EDFacts + CCD)")
    if args.dry_run:
        print("  Mode:    DRY RUN (no DB writes)")
    print()

    n = fetch_lea_accountability(
        states=args.states,
        grad_year=args.grad_year,
        dry_run=args.dry_run,
    )

    if args.dry_run:
        print(f"\nDRY RUN — {n:,} rows would be written")
        return

    conn = db.get_connection()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM lea_accountability")
    total = cur.fetchone()[0]
    cur.execute(
        "SELECT COUNT(*) FROM lea_accountability WHERE graduation_rate IS NOT NULL"
    )
    with_grad = cur.fetchone()[0]
    conn.close()

    print(f"Done. lea_accountability rows: {total:,}")
    print(f"  With graduation rate:   {with_grad:,}")
    print(f"  Note: proficiency data unavailable (Urban Institute API returning 500)")


if __name__ == "__main__":
    main()
