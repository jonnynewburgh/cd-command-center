"""
etl/load_opportunity_zones.py — Mark census tracts as Treasury-designated Opportunity Zones.

Opportunity Zones were designated in 2018 under the Tax Cuts and Jobs Act.
They are census tracts nominated by governors and approved by Treasury.
OZ status enables Qualified Opportunity Fund investments with deferred
capital gains taxes — a complementary financing tool to NMTC.

Data source:
  Treasury/IRS publishes the OZ tract list as a spreadsheet.
  Download from: https://www.irs.gov/pub/irs-utl/Designated_QOZ_8996.xlsx
  Or from CDFI Fund: https://www.cdfifund.gov/opportunity-zones

The file has one row per designated OZ census tract. The key column is
the 11-digit FIPS census tract ID (same format used in our census_tracts table).

Column naming varies across editions of the file. This script tries the most
common column names: 'census_tract', 'Census Tract Number', 'Tract Number',
'GEOID', 'FIPS'. Pass --column if your file uses a different column name.

Usage:
    python etl/load_opportunity_zones.py --file data/raw/opportunity_zones.csv
    python etl/load_opportunity_zones.py --file data/raw/Designated_QOZ_8996.xlsx
    python etl/load_opportunity_zones.py --file data/raw/opportunity_zones.csv --column GEOID
    python etl/load_opportunity_zones.py --file data/raw/opportunity_zones.csv --columns-only
"""

import argparse
import sys
import os

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import db

# Common column names used for the census tract FIPS across different OZ file editions
CANDIDATE_COLUMNS = [
    "census_tract",
    "Census Tract Number",
    "CensusTract",
    "Tract Number",
    "GEOID",
    "GEOID10",
    "FIPS",
    "fips",
    "tract_id",
]


def find_tract_column(df: pd.DataFrame, override: str = None) -> str:
    """
    Find the column in df that holds census tract FIPS codes.
    If override is given, use that. Otherwise try CANDIDATE_COLUMNS.
    Raises ValueError if nothing suitable is found.
    """
    if override:
        if override in df.columns:
            return override
        raise ValueError(
            f"Column '{override}' not found in file. "
            f"Available columns: {list(df.columns)}"
        )

    for col in CANDIDATE_COLUMNS:
        if col in df.columns:
            return col

    raise ValueError(
        f"Could not find a census tract column. Tried: {CANDIDATE_COLUMNS}. "
        f"Available columns in file: {list(df.columns)}. "
        f"Pass --column <name> to specify the right one."
    )


def normalize_tract_id(value) -> str | None:
    """
    Normalize a raw census tract value to a zero-padded 11-digit FIPS string.
    Handles:
    - Integer (e.g. 1003010100 → '01003010100')
    - String with or without leading zeros
    - Float (e.g. 1003010100.0 → '01003010100')
    Returns None for missing/invalid values.
    """
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    try:
        # Convert float to int first (removes .0 suffix)
        raw = str(int(float(str(value).strip())))
        # Zero-pad to 11 digits
        return raw.zfill(11)
    except (ValueError, TypeError):
        return None


def main():
    parser = argparse.ArgumentParser(
        description="Load Opportunity Zone census tract designations"
    )
    parser.add_argument(
        "--file",
        required=True,
        help="Path to the OZ tract file (CSV or Excel). Download from IRS or CDFI Fund.",
    )
    parser.add_argument(
        "--column",
        default=None,
        help="Column name that contains the 11-digit census tract FIPS. "
             "Auto-detected if not specified.",
    )
    parser.add_argument(
        "--columns-only",
        action="store_true",
        help="Print column names from the file and exit (useful for identifying the tract column).",
    )
    args = parser.parse_args()

    if not os.path.exists(args.file):
        print(f"Error: file not found: {args.file}")
        sys.exit(1)

    # Load the file
    print(f"CD Command Center — Opportunity Zone Load")
    print(f"  File: {args.file}")

    if args.file.endswith((".xlsx", ".xls")):
        df = pd.read_excel(args.file, dtype=str)
    else:
        df = pd.read_csv(args.file, dtype=str)

    print(f"  Rows: {len(df):,}")

    if args.columns_only:
        print("  Columns in file:")
        for col in df.columns:
            print(f"    {col}")
        return

    # Find the tract column
    try:
        tract_col = find_tract_column(df, args.column)
    except ValueError as e:
        print(f"Error: {e}")
        sys.exit(1)

    print(f"  Using column: '{tract_col}'")
    print()

    db.init_db()

    # Normalize all tract IDs
    tract_ids = set()
    for raw_val in df[tract_col]:
        tid = normalize_tract_id(raw_val)
        if tid and len(tid) == 11:
            tract_ids.add(tid)

    print(f"  Valid 11-digit tract IDs found: {len(tract_ids):,}")

    if not tract_ids:
        print("  No valid tract IDs found. Check the file and column name.")
        sys.exit(1)

    # Check how many of these tracts are in our database
    conn = db.get_connection()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM census_tracts")
    total_in_db = cur.fetchone()[0]
    conn.close()

    print(f"  Census tracts in database: {total_in_db:,}")
    print()
    print("  Updating census_tracts.is_opportunity_zone...")

    # First, reset all OZ flags to 0 (so re-running is idempotent)
    conn = db.get_connection()
    cur = conn.cursor()
    cur.execute("UPDATE census_tracts SET is_opportunity_zone = 0")

    # Then mark designated tracts as 1
    updated = 0
    not_in_db = 0
    for tid in tract_ids:
        cur.execute(
            db.adapt_sql(
                "UPDATE census_tracts SET is_opportunity_zone = 1 WHERE census_tract_id = ?"
            ),
            (tid,),
        )
        if cur.rowcount > 0:
            updated += 1
        else:
            not_in_db += 1

    conn.commit()
    conn.close()

    print(f"  Marked as Opportunity Zone: {updated:,}")
    if not_in_db > 0:
        print(
            f"  Tract IDs in OZ file but not in census_tracts table: {not_in_db:,} "
            f"(load census tract data first with load_census_tracts.py)"
        )
    print()
    print("Done. Use the 'Opportunity Zones only' filter in the app to see OZ-designated facilities.")


if __name__ == "__main__":
    main()
