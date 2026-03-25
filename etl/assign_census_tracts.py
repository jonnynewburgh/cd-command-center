"""
etl/assign_census_tracts.py — Batch-assign census tracts to facilities using lat/lon.

Facilities loaded from external sources have latitude and longitude but no
census_tract_id. This script uses the FCC Area API (primary) or Census Bureau
coordinates API (fallback) to look up the census tract for each facility and
write it back to the database.

Supports schools, FQHC sites, and ECE centers. All three use the same FCC →
Census coordinate lookup; only the source table and ID column differ.

Why the FCC API?
    The Census Bureau coordinate API is accurate but rate-limits aggressively
    under load. The FCC Area API returns the same census tract from the same
    TIGER/Line data and tolerates higher throughput without throttling.

Speed (10 workers):
    - Schools:  ~95k records → ~90-120 min
    - FQHC:     ~19k records → ~20-30 min
    - ECE:      ~5k records  → ~5-8 min

Resume safety:
    Only records where census_tract_id IS NULL are processed. Re-running after
    an interruption picks up where it left off.

Usage:
    python etl/assign_census_tracts.py                    # schools (default)
    python etl/assign_census_tracts.py --table schools
    python etl/assign_census_tracts.py --table fqhc
    python etl/assign_census_tracts.py --table ece
    python etl/assign_census_tracts.py --table schools --states CA TX
    python etl/assign_census_tracts.py --table fqhc --limit 100    # test run
    python etl/assign_census_tracts.py --table fqhc --workers 20
"""

import argparse
import sys
import os
import time
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import db

BATCH_SIZE = 500
DEFAULT_WORKERS = 10
REQUEST_TIMEOUT = 10

# ---------------------------------------------------------------------------
# Table configuration
# ---------------------------------------------------------------------------

TABLE_CONFIG = {
    "schools": {
        "table":         "schools",
        "id_col":        "nces_id",
        "name_col":      "school_name",
        "batch_update":  db.batch_update_school_census_tracts,
        "pipeline_name": "assign_census_tracts_schools",
    },
    "fqhc": {
        "table":         "fqhc",
        "id_col":        "bhcmis_id",
        "name_col":      "site_name",
        "batch_update":  None,   # set below after defining the helper
        "pipeline_name": "assign_census_tracts_fqhc",
    },
    "ece": {
        "table":         "ece_centers",
        "id_col":        "license_id",
        "name_col":      "provider_name",
        "batch_update":  None,   # set below
        "pipeline_name": "assign_census_tracts_ece",
    },
}


def _make_batch_update(table: str, id_col: str):
    """
    Return a batch-update function that sets census_tract_id (only) for the
    given table. Used for FQHC and ECE where we already have lat/lon and just
    need to fill in the tract.
    """
    def batch_update(records: list[dict]):
        if not records:
            return
        conn = db.get_connection()
        cur = conn.cursor()
        cur.executemany(
            db._adapt_sql(
                f"UPDATE {table} SET census_tract_id = ? WHERE {id_col} = ?"
            ),
            [(r["census_tract_id"], r[id_col]) for r in records],
        )
        conn.commit()
        conn.close()
    return batch_update


TABLE_CONFIG["fqhc"]["batch_update"] = _make_batch_update("fqhc", "bhcmis_id")
TABLE_CONFIG["ece"]["batch_update"]  = _make_batch_update("ece_centers", "license_id")


# ---------------------------------------------------------------------------
# API lookups — FCC primary, Census fallback
# ---------------------------------------------------------------------------

def lookup_tract_fcc(lat: float, lon: float) -> str | None:
    """
    Look up census tract using the FCC Area API.
    Returns an 11-digit census tract FIPS string, or None on failure.

    The FCC API returns a 15-digit census block FIPS; first 11 digits = tract.
    """
    try:
        resp = requests.get(
            "https://geo.fcc.gov/api/census/block/find",
            params={"latitude": lat, "longitude": lon, "format": "json", "showall": "false"},
            timeout=REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        block_fips = resp.json().get("Block", {}).get("FIPS", "")
        if block_fips and len(block_fips) >= 11:
            return block_fips[:11]
    except Exception:
        pass
    return None


def lookup_tract_census(lat: float, lon: float) -> str | None:
    """
    Look up census tract using the Census Bureau coordinates API.
    Used as fallback when FCC returns nothing.
    """
    try:
        resp = requests.get(
            "https://geocoding.geo.census.gov/geocoder/geographies/coordinates",
            params={
                "x": lon, "y": lat,
                "benchmark": "Public_AR_Current",
                "vintage": "Current_Current",
                "layers": "Census Tracts",
                "format": "json",
            },
            timeout=REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        tracts = resp.json().get("result", {}).get("geographies", {}).get("Census Tracts", [])
        if tracts:
            t = tracts[0]
            state, county, tract = t.get("STATE", ""), t.get("COUNTY", ""), t.get("TRACT", "")
            if state and county and tract:
                return f"{state}{county}{tract}"
    except Exception:
        pass
    return None


def lookup_tract(record: dict, id_col: str) -> dict:
    """
    Look up the census tract for one record.
    Returns the record dict with 'result_tract_id' added (None if both APIs fail).
    """
    lat, lon = record.get("latitude"), record.get("longitude")
    if lat is None or lon is None:
        return {**record, "result_tract_id": None}

    tract_id = lookup_tract_fcc(lat, lon)
    if not tract_id:
        tract_id = lookup_tract_census(lat, lon)

    return {**record, "result_tract_id": tract_id}


# ---------------------------------------------------------------------------
# Database fetch
# ---------------------------------------------------------------------------

def get_records_without_tracts(config: dict, states: list[str] | None, limit: int | None) -> list[dict]:
    """Fetch records that have lat/lon but no census_tract_id."""
    table  = config["table"]
    id_col = config["id_col"]

    conditions = [
        "latitude IS NOT NULL",
        "longitude IS NOT NULL",
        "(census_tract_id IS NULL OR census_tract_id = '')",
    ]
    params = []

    if states:
        placeholders = ",".join(["?"] * len(states))
        conditions.append(f"state IN ({placeholders})")
        params.extend(states)

    where = "WHERE " + " AND ".join(conditions)
    limit_clause = f"LIMIT {limit}" if limit else ""

    conn = db.get_connection()
    cur = conn.cursor()
    cur.execute(
        db._adapt_sql(
            f"SELECT {id_col}, latitude, longitude, state FROM {table} {where} "
            f"ORDER BY state {limit_clause}"
        ),
        params,
    )
    rows = [
        {id_col: r[0], "latitude": r[1], "longitude": r[2], "state": r[3]}
        for r in cur.fetchall()
    ]
    conn.close()
    return rows


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Assign census tracts to facilities using lat/lon"
    )
    parser.add_argument(
        "--table", default="schools", choices=list(TABLE_CONFIG.keys()),
        help="Which table to process (default: schools)",
    )
    parser.add_argument("--states", nargs="+", metavar="STATE",
                        help="Only process records in these states")
    parser.add_argument("--limit", type=int, default=None,
                        help="Max records to process (good for testing)")
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS,
                        help=f"Parallel API workers (default: {DEFAULT_WORKERS})")
    args = parser.parse_args()

    config = TABLE_CONFIG[args.table]
    id_col = config["id_col"]

    print(f"CD Command Center — Census Tract Assignment ({args.table})")

    records = get_records_without_tracts(config, args.states, args.limit)

    if not records:
        print("  No records need census tract assignment. All done!")
        return

    total = len(records)
    print(f"  Records to process: {total:,}")
    if args.states:
        print(f"  States: {', '.join(args.states)}")
    print(f"  Workers: {args.workers}")
    print()

    run_id = db.log_load_start(config["pipeline_name"])

    assigned = 0
    failed = 0
    pending = []
    start_time = time.time()

    try:
        with ThreadPoolExecutor(max_workers=args.workers) as executor:
            futures = {executor.submit(lookup_tract, r, id_col): r for r in records}

            for i, future in enumerate(as_completed(futures), start=1):
                result = future.result()
                tract_id = result.get("result_tract_id")

                if tract_id:
                    pending.append({id_col: result[id_col], "census_tract_id": tract_id})
                    assigned += 1
                else:
                    failed += 1

                if len(pending) >= BATCH_SIZE:
                    config["batch_update"](pending)
                    pending = []

                if i % 1000 == 0 or i == total:
                    elapsed = time.time() - start_time
                    rate = i / elapsed if elapsed > 0 else 0
                    eta_min = ((total - i) / rate / 60) if rate > 0 else 0
                    print(
                        f"  {i:,}/{total:,}  assigned: {assigned:,}  failed: {failed:,}"
                        f"  rate: {rate:.0f}/s  ETA: {eta_min:.1f}m"
                    )

        if pending:
            config["batch_update"](pending)

    except Exception as e:
        db.log_load_finish(run_id, rows_loaded=assigned, error=str(e))
        print(f"\nError: {e}")
        sys.exit(1)

    elapsed = time.time() - start_time
    print()
    print(f"Done in {elapsed / 60:.1f} minutes.")
    print(f"  Assigned: {assigned:,}")
    print(f"  Failed:   {failed:,}")
    print(f"  Total:    {total:,}")

    db.log_load_finish(run_id, rows_loaded=assigned)


if __name__ == "__main__":
    main()
