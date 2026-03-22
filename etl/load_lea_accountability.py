"""
etl/load_lea_accountability.py — Load LEA (district) accountability data into SQLite.

Expected CSV columns:
  - lea_id, lea_name, state, accountability_score, accountability_rating,
    proficiency_reading, proficiency_math, graduation_rate, data_year

Usage:
    python etl/load_lea_accountability.py --file data/raw/lea_accountability.csv
"""

import argparse
import sys
import os
import pandas as pd
import numpy as np

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import db


EXPECTED_COLUMNS = [
    "lea_id", "lea_name", "state", "accountability_score", "accountability_rating",
    "proficiency_reading", "proficiency_math", "graduation_rate", "data_year",
]

NUMERIC_COLS = [
    "accountability_score", "proficiency_reading", "proficiency_math",
    "graduation_rate", "data_year",
]


def main():
    # DEPRECATED: This script required a manually downloaded CSV file.
    # Use fetch_edfacts.py instead, which auto-downloads federal EDFacts data for all 50 states.
    # For state-specific accountability data, use fetch_state_accountability.py.
    # Example: python etl/fetch_edfacts.py --year 2023
    print("DEPRECATED: load_lea_accountability.py is superseded by fetch_edfacts.py")
    print("  fetch_edfacts.py auto-downloads federal LEA accountability data (all 50 states)")
    print("  Run: python etl/fetch_edfacts.py --year 2023")
    print("  For state-specific data: python etl/fetch_state_accountability.py --state TX --year 2023")
    sys.exit(0)

    parser = argparse.ArgumentParser(description="Load LEA accountability CSV into SQLite")
    parser.add_argument("--file", required=True, help="Path to LEA accountability CSV")
    parser.add_argument("--year", type=int, help="Override data_year for all rows")
    args = parser.parse_args()

    if not os.path.exists(args.file):
        print(f"Error: file not found: {args.file}")
        sys.exit(1)

    db.init_db()

    print(f"Reading {args.file}...")
    df = pd.read_csv(args.file, dtype=str, low_memory=False)
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    print(f"  Found {len(df)} rows")

    # Add missing columns
    for col in EXPECTED_COLUMNS:
        if col not in df.columns:
            print(f"  Warning: column '{col}' not found, will be NULL")
            df[col] = None

    df = df[EXPECTED_COLUMNS]

    for col in NUMERIC_COLS:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    if args.year:
        df["data_year"] = args.year

    print(f"Loading {len(df)} LEA records into database...")
    loaded = 0
    errors = 0

    for _, row in df.iterrows():
        record = {}
        for col in EXPECTED_COLUMNS:
            val = row.get(col)
            record[col] = None if (val is None or (isinstance(val, float) and np.isnan(val))) else val

        if not record.get("lea_id"):
            errors += 1
            continue

        try:
            db.upsert_lea_accountability(record)
            loaded += 1
        except Exception as e:
            print(f"  Error: {e}")
            errors += 1

    print(f"  Done: {loaded} loaded, {errors} errors")
    print("LEA accountability load complete.")


if __name__ == "__main__":
    main()
