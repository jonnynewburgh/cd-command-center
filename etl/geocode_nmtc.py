"""
etl/geocode_nmtc.py — Geocode NMTC projects that are missing lat/lon.

NMTC projects are loaded from the CDFI Fund Excel file, which does not include
coordinates. This script geocodes them using the Census Bureau's free geocoding
API (city + state + zip), then writes lat/lon and census_tract_id back to the
database.

The Census API is free but rate-limited. This script sleeps 0.5s between calls.
With ~8,000 projects it takes roughly 90 minutes to run fully. Use --limit to
test on a small batch first.

Usage:
    python etl/geocode_nmtc.py                    # geocode all missing
    python etl/geocode_nmtc.py --limit 50         # test on 50 rows
    python etl/geocode_nmtc.py --states CA TX     # only projects in these states
    python etl/geocode_nmtc.py --overwrite        # redo even ones already geocoded
"""

import argparse
import sys
import os
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import db
from utils.geo import geocode_address

# Seconds to sleep between Census API calls to avoid rate-limiting
API_SLEEP = 0.5


def get_projects_to_geocode(states=None, limit=None, overwrite=False) -> list[dict]:
    """
    Fetch NMTC projects that need geocoding.
    By default returns only rows where latitude IS NULL.
    With overwrite=True, returns all rows.
    """
    conn = db.get_connection()
    cur = conn.cursor()

    conditions = []
    params = []

    if not overwrite:
        conditions.append("(latitude IS NULL OR longitude IS NULL)")

    if states:
        placeholders = ",".join("?" * len(states))
        conditions.append(f"state IN ({placeholders})")
        params.extend(states)

    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""
    limit_clause = f"LIMIT {limit}" if limit else ""

    cur.execute(
        f"SELECT id, cdfi_project_id, city, state, zip_code, address "
        f"FROM nmtc_projects {where} ORDER BY state, id {limit_clause}",
        params,
    )
    rows = cur.fetchall()
    conn.close()

    return [
        {
            "id": r[0],
            "cdfi_project_id": r[1],
            "city": r[2],
            "state": r[3],
            "zip_code": r[4],
            "address": r[5],
        }
        for r in rows
    ]


def build_address_string(project: dict) -> str:
    """
    Build the best address string we can from available fields.
    Falls back from full address → city+state+zip → city+state.
    """
    parts = []
    if project.get("address"):
        parts.append(project["address"])
    if project.get("city"):
        parts.append(project["city"])
    if project.get("state"):
        parts.append(project["state"])
    if project.get("zip_code"):
        parts.append(project["zip_code"])
    return ", ".join(parts)


def update_project_coords(cdfi_project_id: str, lat: float, lon: float, census_tract_id: str):
    """Write geocoded coordinates back to the database."""
    record = {
        "cdfi_project_id": cdfi_project_id,
        "latitude": lat,
        "longitude": lon,
    }
    if census_tract_id:
        record["census_tract_id"] = census_tract_id
    db.upsert_nmtc_project(record)


def main():
    parser = argparse.ArgumentParser(description="Geocode NMTC projects (add lat/lon)")
    parser.add_argument("--limit", type=int, help="Max number of projects to process")
    parser.add_argument("--states", nargs="+", metavar="ST", help="Filter to specific state codes (e.g. CA TX)")
    parser.add_argument("--overwrite", action="store_true", help="Re-geocode projects that already have coordinates")
    args = parser.parse_args()

    projects = get_projects_to_geocode(
        states=args.states,
        limit=args.limit,
        overwrite=args.overwrite,
    )

    total = len(projects)
    if total == 0:
        print("No projects need geocoding. Use --overwrite to redo existing coordinates.")
        return

    print(f"Geocoding {total:,} NMTC projects...")
    if args.limit:
        print(f"  (limited to {args.limit})")
    if args.states:
        print(f"  States: {args.states}")
    print()

    success = 0
    failed = 0

    for i, project in enumerate(projects, 1):
        addr = build_address_string(project)
        result = geocode_address(addr)

        if result.get("lat") and result.get("lon"):
            update_project_coords(
                project["cdfi_project_id"],
                result["lat"],
                result["lon"],
                result.get("census_tract_id", ""),
            )
            success += 1
            if i % 100 == 0 or i == total:
                print(f"  [{i}/{total}] {success} geocoded, {failed} failed...")
        else:
            failed += 1
            if failed <= 10 or i % 500 == 0:
                print(f"  [{i}/{total}] FAILED: {addr}")

        time.sleep(API_SLEEP)

    print()
    print(f"Done. {success:,} geocoded, {failed:,} failed out of {total:,} total.")
    print()

    # Summary
    conn = db.get_connection()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM nmtc_projects WHERE latitude IS NOT NULL")
    with_coords = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM nmtc_projects")
    total_in_db = cur.fetchone()[0]
    conn.close()
    print(f"Database: {with_coords:,} of {total_in_db:,} NMTC projects now have coordinates.")


if __name__ == "__main__":
    main()
