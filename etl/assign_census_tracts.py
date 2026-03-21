"""
etl/assign_census_tracts.py — Batch-assign census tracts to schools and FQHCs using lat/lon.

Schools and FQHCs have latitude and longitude but may be missing census_tract_id.
This script uses the Census Bureau's coordinate-to-geography API to look up
the census tract for each facility and update the database.

Uses concurrent requests (10 workers by default) to run ~10x faster than
sequential processing. The Census Bureau API is free with no official rate limit,
but we keep concurrency reasonable to be polite.

Estimated run times (10 workers, ~1s per API call):
  10,000 schools  → ~17 min
  100,000 schools → ~2.8 hours
  18,000 FQHCs    → ~30 min

Usage:
    python etl/assign_census_tracts.py               # schools + FQHCs, all states
    python etl/assign_census_tracts.py --schools     # schools only
    python etl/assign_census_tracts.py --fqhc        # FQHCs only
    python etl/assign_census_tracts.py --states CA TX # limit by state
    python etl/assign_census_tracts.py --limit 500   # process at most 500 per type
    python etl/assign_census_tracts.py --workers 20  # more concurrent workers
"""

import argparse
import sys
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import db
from utils.geo import reverse_geocode_tract

# Default number of concurrent API workers
DEFAULT_WORKERS = 10


def get_facilities_without_tracts(table: str, id_col: str, states=None, limit=None) -> list[dict]:
    """
    Get facilities that have lat/lon but no census_tract_id.
    Returns list of dicts with id_col, latitude, longitude.
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

    try:
        cur.execute(
            f"SELECT {id_col}, latitude, longitude, state FROM {table} "
            f"{where} ORDER BY state {limit_clause}",
            params,
        )
        rows = [dict(zip([id_col, "latitude", "longitude", "state"], r)) for r in cur.fetchall()]
    except Exception as e:
        print(f"  Warning: could not query {table}: {e}")
        rows = []

    conn.close()
    return rows


def process_facility(facility: dict, id_col: str, update_fn) -> tuple[str, str | None]:
    """
    Look up census tract for one facility and update the DB.
    Returns (facility_id, tract_id_or_None).
    """
    fac_id = facility[id_col]
    lat = facility["latitude"]
    lon = facility["longitude"]

    tract_id = reverse_geocode_tract(lat, lon)
    if tract_id:
        update_fn(fac_id, tract_id)
    return fac_id, tract_id


def run_batch(label: str, facilities: list[dict], id_col: str, update_fn, workers: int):
    """
    Process a batch of facilities concurrently.
    Prints progress every 500 records.
    """
    if not facilities:
        print(f"  No {label} need census tract assignment.")
        return

    print(f"\n{label}:")
    print(f"  To process: {len(facilities):,}")
    print(f"  Workers:    {workers}")

    assigned = 0
    failed = 0
    total = len(facilities)
    start = time.time()

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(process_facility, fac, id_col, update_fn): fac
            for fac in facilities
        }

        for i, future in enumerate(as_completed(futures), 1):
            try:
                _, tract_id = future.result()
                if tract_id:
                    assigned += 1
                else:
                    failed += 1
            except Exception:
                failed += 1

            # Progress every 500 records
            if i % 500 == 0 or i == total:
                elapsed = time.time() - start
                rate = i / elapsed if elapsed > 0 else 0
                remaining = (total - i) / rate if rate > 0 else 0
                print(
                    f"  {i:,}/{total:,} — assigned: {assigned:,}, "
                    f"failed: {failed:,}, "
                    f"rate: {rate:.1f}/s, "
                    f"ETA: {remaining/60:.1f}m"
                )

    elapsed = time.time() - start
    print(f"\n  Done in {elapsed/60:.1f} minutes.")
    print(f"  Assigned: {assigned:,} | Failed: {failed:,}")


def main():
    parser = argparse.ArgumentParser(
        description="Assign census tracts to schools and FQHCs using lat/lon geocoding"
    )
    parser.add_argument("--schools", action="store_true", help="Process schools only")
    parser.add_argument("--fqhc", action="store_true", help="Process FQHCs only")
    parser.add_argument("--states", nargs="+", metavar="STATE", help="Only process these states")
    parser.add_argument("--limit", type=int, default=None, help="Max facilities per type")
    parser.add_argument(
        "--workers",
        type=int,
        default=DEFAULT_WORKERS,
        help=f"Concurrent API workers (default: {DEFAULT_WORKERS})",
    )
    args = parser.parse_args()

    # If neither flag given, do both
    do_schools = args.schools or (not args.schools and not args.fqhc)
    do_fqhc = args.fqhc or (not args.schools and not args.fqhc)

    print("CD Command Center — Census Tract Assignment")
    if args.states:
        print(f"  States: {', '.join(args.states)}")
    if args.limit:
        print(f"  Limit:  {args.limit:,} per type")

    if do_schools:
        schools = get_facilities_without_tracts("schools", "nces_id", args.states, args.limit)
        run_batch("Charter + Traditional Schools", schools, "nces_id", db.update_school_census_tract, args.workers)

    if do_fqhc:
        fqhcs = get_facilities_without_tracts("fqhc", "bhcmis_id", args.states, args.limit)
        run_batch("FQHCs / Health Centers", fqhcs, "bhcmis_id", db.update_fqhc_census_tract, args.workers)


if __name__ == "__main__":
    main()
