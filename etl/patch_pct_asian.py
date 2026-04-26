"""
etl/patch_pct_asian.py — Backfill pct_asian for schools already in the database.

The pct_asian column was added after the initial demographics load. This script
fetches only Asian enrollment (race code 4) from the NCES Education Data API
and updates the schools table in-place — without re-downloading all school data.

Race codes (NCES):
    1 = White, 2 = Black, 3 = Hispanic, 4 = Asian,
    5 = Native Hawaiian/Pacific Islander, 6 = American Indian/Alaska Native,
    7 = Two or more races, 99 = All (total)

Usage:
    python etl/patch_pct_asian.py               # all schools
    python etl/patch_pct_asian.py --states CA TX # specific states
    python etl/patch_pct_asian.py --year 2023    # specific NCES year (default: most recent)
"""

import argparse
import os
import sys
import time

import requests

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import db

ENROLLMENT_URL = "https://educationdata.urban.org/api/v1/schools/ccd/enrollment"
MOST_RECENT_YEAR = 2023   # update annually
PAGE_SLEEP = 0.25
PER_PAGE = 5000

# NCES race code for Asian
RACE_ASIAN = 4
RACE_TOTAL = 99

_STATE_FIPS = {
    "AL": 1,  "AK": 2,  "AZ": 4,  "AR": 5,  "CA": 6,  "CO": 8,  "CT": 9,
    "DE": 10, "DC": 11, "FL": 12, "GA": 13, "HI": 15, "ID": 16, "IL": 17,
    "IN": 18, "IA": 19, "KS": 20, "KY": 21, "LA": 22, "ME": 23, "MD": 24,
    "MA": 25, "MI": 26, "MN": 27, "MS": 28, "MO": 29, "MT": 30, "NE": 31,
    "NV": 32, "NH": 33, "NJ": 34, "NM": 35, "NY": 36, "NC": 37, "ND": 38,
    "OH": 39, "OK": 40, "OR": 41, "PA": 42, "RI": 44, "SC": 45, "SD": 46,
    "TN": 47, "TX": 48, "UT": 49, "VT": 50, "VA": 51, "WA": 53, "WV": 54,
    "WI": 55, "WY": 56,
}


def fetch_asian_pct_for_state(year: int, fips: int, ncessch_set: set) -> dict:
    """
    Fetch race=4 (Asian) and race=99 (total) enrollment for all schools in a state.
    Returns dict: ncessch -> pct_asian (float or None).
    """
    url = f"{ENROLLMENT_URL}/{year}/grade-99/race-2/"
    results = []
    page = 1

    while True:
        try:
            resp = requests.get(url, params={"fips": fips, "per_page": PER_PAGE, "page": page},
                                timeout=30)
            if resp.status_code == 404:
                break
            resp.raise_for_status()
            data = resp.json()
        except requests.RequestException as e:
            print(f"    API error page {page}: {e}")
            break

        results.extend(data.get("results", []))
        if not data.get("next"):
            break
        page += 1
        time.sleep(PAGE_SLEEP)

    # Build per-school counts for Asian and Total
    raw = {}
    for row in results:
        ncessch = row.get("ncessch")
        if ncessch not in ncessch_set:
            continue
        if row.get("sex") != 99:   # all-gender rows only
            continue
        race = row.get("race")
        count = max(row.get("enrollment") or 0, 0)
        if ncessch not in raw:
            raw[ncessch] = {}
        raw[ncessch][race] = count

    out = {}
    for ncessch, counts in raw.items():
        total = counts.get(RACE_TOTAL, 0)
        asian = counts.get(RACE_ASIAN, 0)
        if total and total > 0:
            out[ncessch] = round(asian / total * 100, 1)
        else:
            out[ncessch] = None
    return out


def main():
    parser = argparse.ArgumentParser(description="Backfill pct_asian for schools table")
    parser.add_argument("--states", nargs="+", metavar="ST")
    parser.add_argument("--year", type=int, default=MOST_RECENT_YEAR)
    args = parser.parse_args()

    print("CD Command Center — pct_asian backfill")
    print(f"  NCES year: {args.year}")

    db.init_db()
    conn = db.get_connection()

    # Load nces_id + state for all schools (or filtered states)
    cur = conn.cursor()
    if args.states:
        placeholders = ",".join("?" * len(args.states))
        cur.execute(
            db.adapt_sql(
                f"SELECT nces_id, state FROM schools WHERE state IN ({placeholders})"
            ),
            args.states,
        )
        rows = cur.fetchall()
    else:
        cur.execute("SELECT nces_id, state FROM schools")
        rows = cur.fetchall()

    # Group by state
    by_state = {}
    for r in rows:
        nces_id, state = r[0], r[1]
        by_state.setdefault(state, set()).add(str(nces_id).zfill(12))

    states = sorted(by_state.keys())
    print(f"  Schools: {sum(len(v) for v in by_state.values()):,} across {len(states)} states")
    print()

    total_updated = 0
    for state in states:
        fips = _STATE_FIPS.get(state)
        if not fips:
            print(f"  {state}... skipped (unknown FIPS)")
            continue

        ncessch_set = by_state[state]
        print(f"  {state}...", end=" ", flush=True)
        pct_map = fetch_asian_pct_for_state(args.year, fips, ncessch_set)

        if not pct_map:
            print("no data")
            continue

        # Update schools table in batches
        updated = 0
        for nces_id, pct in pct_map.items():
            cur.execute(
                db.adapt_sql("UPDATE schools SET pct_asian = ? WHERE nces_id = ?"),
                (pct, nces_id),
            )
            updated += 1

        conn.commit()
        total_updated += updated
        print(f"{updated:,} schools")

    conn.close()
    print()
    print(f"Done. Total schools updated: {total_updated:,}")


if __name__ == "__main__":
    main()
