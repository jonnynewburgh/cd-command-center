"""
etl/geocode_nmtc.py — Geocode NMTC projects that are missing lat/lon.

NMTC projects are loaded from the CDFI Fund Excel file, which does not include
coordinates. This script looks up lat/lon using the ZIP code via the free
zippopotam.us API (no API key needed), then writes coordinates back to the
database.

ZIP-level geocoding places each project at the center of its ZIP code, which
is good enough for map display. Projects in the same ZIP will overlap on the
map but can be distinguished in the data table.

Because many projects share a ZIP code, we cache results so each ZIP is only
looked up once. This makes the full run much faster (~5 minutes vs 90 minutes).

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

import requests

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import db

# Seconds to sleep between API calls (only fires on cache misses)
API_SLEEP = 0.2


def geocode_zip(zip_code: str, cache: dict) -> dict:
    """
    Look up lat/lon for a ZIP code using zippopotam.us.
    Results are cached in the provided dict so each ZIP is only fetched once.
    Returns dict with 'lat' and 'lon', or empty dict on failure.
    """
    zip5 = str(zip_code).strip().split("-")[0].zfill(5)  # normalize to 5-digit ZIP

    if zip5 in cache:
        return cache[zip5]

    try:
        resp = requests.get(f"https://api.zippopotam.us/us/{zip5}", timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            places = data.get("places", [])
            if places:
                result = {
                    "lat": float(places[0]["latitude"]),
                    "lon": float(places[0]["longitude"]),
                }
                cache[zip5] = result
                time.sleep(API_SLEEP)
                return result
    except Exception:
        pass

    cache[zip5] = {}  # cache the failure so we don't retry the same bad ZIP
    return {}


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
        db.adapt_sql(
            f"SELECT id, cdfi_project_id, city, state, zip_code "
            f"FROM nmtc_projects {where} ORDER BY zip_code, id {limit_clause}"
        ),
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
        }
        for r in rows
    ]


def update_project_coords(cdfi_project_id: str, lat: float, lon: float):
    """Write geocoded coordinates back to the database."""
    db.upsert_nmtc_project({
        "cdfi_project_id": cdfi_project_id,
        "latitude": lat,
        "longitude": lon,
    })


def main():
    parser = argparse.ArgumentParser(description="Geocode NMTC projects (add lat/lon from ZIP code)")
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

    print(f"Geocoding {total:,} NMTC projects by ZIP code...")
    if args.limit:
        print(f"  (limited to {args.limit})")
    if args.states:
        print(f"  States: {args.states}")
    print()

    zip_cache = {}  # ZIP → {lat, lon} — avoids re-fetching the same ZIP
    success = 0
    failed = 0

    for i, project in enumerate(projects, 1):
        zip_code = project.get("zip_code")
        if not zip_code:
            failed += 1
            continue

        result = geocode_zip(zip_code, zip_cache)

        if result.get("lat") and result.get("lon"):
            update_project_coords(project["cdfi_project_id"], result["lat"], result["lon"])
            success += 1
        else:
            failed += 1
            if failed <= 5:
                print(f"  FAILED ZIP: {zip_code} ({project.get('city')}, {project.get('state')})")

        if i % 500 == 0 or i == total:
            unique_zips = len(zip_cache)
            print(f"  [{i:,}/{total:,}] {success:,} geocoded, {failed} failed ({unique_zips} unique ZIPs looked up)")

    print()
    print(f"Done. {success:,} geocoded, {failed} failed out of {total:,} total.")
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
