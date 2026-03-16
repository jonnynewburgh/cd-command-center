"""
etl/assign_census_tracts.py — Batch-assign census tracts to schools using lat/lon.

Schools loaded from NCES have latitude and longitude but no census_tract_id.
This script uses the Census Bureau's coordinate-to-geography API to look up
the census tract for each school and update the database.

The Census Bureau API is free but rate-limited. This script adds a 0.5s delay
between calls and prints progress every 100 schools.

Usage:
    python etl/assign_census_tracts.py               # process all schools missing tracts
    python etl/assign_census_tracts.py --states CA TX # only schools in these states
    python etl/assign_census_tracts.py --limit 500    # process at most 500 schools
"""

import argparse
import sys
import os
import time
import sqlite3

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import db
from utils.geo import reverse_geocode_tract

# Seconds between API calls (Census Bureau rate limit)
API_SLEEP = 0.5


def get_schools_without_tracts(states=None, limit=None) -> list[dict]:
    """
    Get schools that have lat/lon but no census_tract_id.
    Returns list of dicts with nces_id, latitude, longitude.
    """
    conn = db.get_connection()
    cur = conn.cursor()

    conditions = [
        "latitude IS NOT NULL",
        "longitude IS NOT NULL",
        "(census_tract_id IS NULL OR census_tract_id = '')",
    ]
    params = []

    if states:
        placeholders = ",".join("?" * len(states))
        conditions.append(f"state IN ({placeholders})")
        params.extend(states)

    where = "WHERE " + " AND ".join(conditions)
    limit_clause = f"LIMIT {limit}" if limit else ""

    # Try schools table, fall back to charter_schools
    for table in ["schools", "charter_schools"]:
        try:
            cur.execute(
                f"SELECT nces_id, latitude, longitude, school_name, state "
                f"FROM {table} {where} ORDER BY state, school_name {limit_clause}",
                params,
            )
            rows = [dict(r) for r in cur.fetchall()]
            conn.close()
            return rows
        except Exception:
            continue

    conn.close()
    return []


def main():
    parser = argparse.ArgumentParser(
        description="Assign census tracts to schools using lat/lon geocoding"
    )
    parser.add_argument(
        "--states",
        nargs="+",
        metavar="STATE",
        help="Only process schools in these states",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Maximum number of schools to process",
    )
    args = parser.parse_args()

    print("CD Command Center — Census Tract Assignment")

    schools = get_schools_without_tracts(states=args.states, limit=args.limit)

    if not schools:
        print("No schools need census tract assignment. All done!")
        return

    print(f"  Schools to process: {len(schools):,}")
    if args.states:
        print(f"  States: {', '.join(args.states)}")
    print()

    assigned = 0
    failed = 0

    for i, school in enumerate(schools):
        lat = school["latitude"]
        lon = school["longitude"]
        nces_id = school["nces_id"]

        tract_id = reverse_geocode_tract(lat, lon)

        if tract_id:
            db.update_school_census_tract(nces_id, tract_id)
            assigned += 1
        else:
            failed += 1

        # Progress every 100 schools
        if (i + 1) % 100 == 0:
            print(f"  Processed {i + 1:,} / {len(schools):,} — assigned: {assigned:,}, failed: {failed:,}")

        time.sleep(API_SLEEP)

    print()
    print(f"Done.")
    print(f"  Assigned: {assigned:,}")
    print(f"  Failed:   {failed:,}")
    print(f"  Total:    {len(schools):,}")


if __name__ == "__main__":
    main()
