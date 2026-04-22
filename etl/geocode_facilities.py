"""
etl/geocode_facilities.py — Geocode FQHC and ECE facilities from their addresses.

FQHC sites and ECE centers are loaded with street addresses but often without
latitude/longitude or census_tract_id. This script geocodes them using the
Census Bureau's address-to-coordinates API and writes the results back.

Unlike assign_census_tracts.py (which takes existing lat/lon and looks up the
tract), this script starts from an address string and gets lat, lon, AND
census_tract_id in a single API call.

The geocoder returns all three values at once, so there's no need for a
separate tract-assignment step after geocoding.

Speed:
    The Census geocoder handles ~5-10 requests/sec with 10 workers.
    - FQHC: ~18,800 sites → ~30-45 minutes
    - ECE:  ~4,500 sites  → ~8-12 minutes
    Results are flushed to the database every BATCH_SIZE records.

Usage:
    python etl/geocode_facilities.py --table fqhc
    python etl/geocode_facilities.py --table ece
    python etl/geocode_facilities.py --table fqhc --states CA TX
    python etl/geocode_facilities.py --table fqhc --limit 100    # test run
    python etl/geocode_facilities.py --table fqhc --workers 20
    python etl/geocode_facilities.py --table fqhc --force        # re-geocode even if already set
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
DEFAULT_WORKERS = 5   # Census address API is rate-sensitive; keep this conservative
REQUEST_TIMEOUT = 10

# ---------------------------------------------------------------------------
# Table configuration — describes how to read and write each table
# ---------------------------------------------------------------------------

TABLE_CONFIG = {
    "fqhc": {
        "table":        "fqhc",
        "id_col":       "bhcmis_id",
        "name_col":     "site_name",
        "address_cols": ["site_address", "city", "state", "zip_code"],
        "batch_update": db.batch_update_fqhc_geo,
        "pipeline_name": "geocode_fqhc",
    },
    "ece": {
        "table":        "ece_centers",
        "id_col":       "license_id",
        "name_col":     "provider_name",
        "address_cols": ["address", "city", "state", "zip_code"],
        "batch_update": db.batch_update_ece_geo,
        "pipeline_name": "geocode_ece",
    },
}


# ---------------------------------------------------------------------------
# Geocoding
# ---------------------------------------------------------------------------

def geocode_address(address: str, retries: int = 3) -> dict | None:
    """
    Geocode a full address string using the Census Bureau API.
    Returns a dict with lat, lon, census_tract_id, or None on failure.

    Retries up to `retries` times with a short sleep between attempts to
    handle transient rate-limit responses (the Census API returns empty results
    rather than 429s when overwhelmed, so we retry on empty-match responses too).
    """
    import time as _time

    for attempt in range(retries):
        try:
            resp = requests.get(
                "https://geocoding.geo.census.gov/geocoder/geographies/onelineaddress",
                params={
                    "address": address,
                    "benchmark": "Public_AR_Current",
                    "vintage": "Current_Current",
                    "layers": "Census Tracts",
                    "format": "json",
                },
                timeout=REQUEST_TIMEOUT,
            )
            resp.raise_for_status()
            data = resp.json()

            matches = data.get("result", {}).get("addressMatches", [])
            if not matches:
                # Empty result might be rate-limiting — retry with backoff
                if attempt < retries - 1:
                    _time.sleep(0.5 * (attempt + 1))
                    continue
                return None

            match = matches[0]
            coords = match.get("coordinates", {})
            tracts = match.get("geographies", {}).get("Census Tracts", [])

            lat = coords.get("y")
            lon = coords.get("x")
            if lat is None or lon is None:
                return None

            tract_id = None
            if tracts:
                t = tracts[0]
                state = t.get("STATE", "")
                county = t.get("COUNTY", "")
                tract = t.get("TRACT", "")
                if state and county and tract:
                    tract_id = f"{state}{county}{tract}"

            return {"lat": float(lat), "lon": float(lon), "census_tract_id": tract_id}

        except Exception:
            if attempt < retries - 1:
                _time.sleep(0.5 * (attempt + 1))
            continue

    return None


def geocode_record(record: dict, id_col: str, address_cols: list[str]) -> dict:
    """
    Geocode a single record. Builds the address string from the record's
    address columns, calls the Census geocoder, and returns the record
    with result_lat, result_lon, result_tract_id added.
    """
    parts = [str(record.get(col) or "").strip() for col in address_cols]
    parts = [p for p in parts if p]
    address = ", ".join(parts)

    result = geocode_address(address) if address else None

    return {
        **record,
        "result_lat": result["lat"] if result else None,
        "result_lon": result["lon"] if result else None,
        "result_tract_id": result["census_tract_id"] if result else None,
    }


# ---------------------------------------------------------------------------
# Fetching records that need geocoding
# ---------------------------------------------------------------------------

def get_ungeocoded(config: dict, states: list[str] | None, limit: int | None, force: bool) -> list[dict]:
    """
    Return records from the target table that are missing lat/lon (or all records if --force).
    """
    table = config["table"]
    id_col = config["id_col"]
    name_col = config["name_col"]
    addr_cols = config["address_cols"]

    select_cols = [id_col, name_col] + [c for c in addr_cols if c != name_col]
    select_clause = ", ".join(select_cols)

    conditions = []
    params = []

    if not force:
        conditions.append("(latitude IS NULL OR longitude IS NULL)")

    if states:
        placeholders = ",".join(["?"] * len(states))
        conditions.append(f"state IN ({placeholders})")
        params.extend(states)

    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""
    limit_clause = f"LIMIT {limit}" if limit else ""

    conn = db.get_connection()
    cur = conn.cursor()
    cur.execute(
        db.adapt_sql(f"SELECT {select_clause} FROM {table} {where} ORDER BY state {limit_clause}"),
        params,
    )
    rows = [dict(zip(select_cols, r)) for r in cur.fetchall()]
    conn.close()
    return rows


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Geocode FQHC or ECE facilities from their street addresses"
    )
    parser.add_argument(
        "--table", required=True, choices=list(TABLE_CONFIG.keys()),
        help="Which table to geocode",
    )
    parser.add_argument("--states", nargs="+", metavar="STATE",
                        help="Only geocode records in these states")
    parser.add_argument("--limit", type=int, default=None,
                        help="Max records to process (good for testing)")
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS,
                        help=f"Parallel workers (default: {DEFAULT_WORKERS})")
    parser.add_argument("--force", action="store_true",
                        help="Re-geocode records that already have coordinates")
    args = parser.parse_args()

    config = TABLE_CONFIG[args.table]
    print(f"CD Command Center — Facility Geocoder ({args.table.upper()})")

    records = get_ungeocoded(config, args.states, args.limit, args.force)

    if not records:
        print("  No records need geocoding. All done!")
        return

    total = len(records)
    print(f"  Records to geocode: {total:,}")
    if args.states:
        print(f"  States: {', '.join(args.states)}")
    print(f"  Workers: {args.workers}")
    print()

    run_id = db.log_load_start(config["pipeline_name"])

    geocoded = 0
    failed = 0
    pending = []
    start_time = time.time()

    try:
        with ThreadPoolExecutor(max_workers=args.workers) as executor:
            futures = {
                executor.submit(geocode_record, r, config["id_col"], config["address_cols"]): r
                for r in records
            }

            for i, future in enumerate(as_completed(futures), start=1):
                result = future.result()

                if result["result_lat"] is not None:
                    pending.append({
                        config["id_col"]:   result[config["id_col"]],
                        "latitude":         result["result_lat"],
                        "longitude":        result["result_lon"],
                        "census_tract_id":  result["result_tract_id"],
                    })
                    geocoded += 1
                else:
                    failed += 1

                if len(pending) >= BATCH_SIZE:
                    config["batch_update"](pending)
                    pending = []

                if i % 500 == 0 or i == total:
                    elapsed = time.time() - start_time
                    rate = i / elapsed if elapsed > 0 else 0
                    eta_min = ((total - i) / rate / 60) if rate > 0 else 0
                    print(
                        f"  {i:,}/{total:,}  geocoded: {geocoded:,}  failed: {failed:,}"
                        f"  rate: {rate:.0f}/s  ETA: {eta_min:.1f}m"
                    )

        if pending:
            config["batch_update"](pending)

    except Exception as e:
        db.log_load_finish(run_id, rows_loaded=geocoded, error=str(e))
        print(f"\nError: {e}")
        sys.exit(1)

    elapsed = time.time() - start_time
    print()
    print(f"Done in {elapsed / 60:.1f} minutes.")
    print(f"  Geocoded: {geocoded:,}")
    print(f"  Failed:   {failed:,}  (address not found or outside US)")
    print(f"  Total:    {total:,}")

    db.log_load_finish(run_id, rows_loaded=geocoded)


if __name__ == "__main__":
    main()
