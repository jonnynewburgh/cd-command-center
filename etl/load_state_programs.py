"""
etl/load_state_programs.py — Load state incentive programs into the database.

State programs (historic tax credits, state NMTCs, LIHTC, etc.) can stack
with federal NMTC, making deals more financially viable. This script loads
a curated CSV of programs into the state_programs table.

The default seed file (data/raw/state_programs_seed.csv) is committed to Git
and covers ~15 states with the most active CD finance markets.

To update the data, edit state_programs_seed.csv and re-run this script.
The script clears and reloads the table on each run (idempotent).

Usage:
    python etl/load_state_programs.py                              # load default seed file
    python etl/load_state_programs.py --file data/raw/state_programs_seed.csv
    python etl/load_state_programs.py --file my_updated_programs.csv --replace
"""

import argparse
import sys
import os

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import db

# Default seed file location (committed to Git)
DEFAULT_SEED_FILE = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data", "raw", "state_programs_seed.csv",
)

# Expected columns in the CSV (maps to state_programs table schema)
EXPECTED_COLUMNS = [
    "state",
    "program_name",
    "program_type",
    "eligible_uses",
    "max_credit_pct",
    "max_amount",
    "administering_agency",
    "website",
    "notes",
    "last_verified",
]


def main():
    parser = argparse.ArgumentParser(
        description="Load state incentive programs from CSV into the database"
    )
    parser.add_argument(
        "--file",
        default=DEFAULT_SEED_FILE,
        help=f"Path to the programs CSV (default: {DEFAULT_SEED_FILE})",
    )
    parser.add_argument(
        "--replace",
        action="store_true",
        default=True,
        help="Clear the state_programs table before loading (default: True for idempotent runs)",
    )
    args = parser.parse_args()

    if not os.path.exists(args.file):
        print(f"Error: file not found: {args.file}")
        print(f"  Expected: {DEFAULT_SEED_FILE}")
        print(f"  The seed file should be committed to Git. Check the data/raw/ directory.")
        sys.exit(1)

    print(f"CD Command Center — State Programs Load")
    print(f"  File: {args.file}")

    df = pd.read_csv(args.file, dtype=str)
    df = df.fillna("")
    print(f"  Rows: {len(df):,}")

    # Validate required columns
    missing = [col for col in ["state", "program_name"] if col not in df.columns]
    if missing:
        print(f"Error: required columns missing: {missing}")
        print(f"  File has: {list(df.columns)}")
        sys.exit(1)

    db.init_db()

    # Clear existing data before reload (ensures deleted programs are removed)
    conn = db.get_connection()
    cur = conn.cursor()
    cur.execute("DELETE FROM state_programs")
    conn.commit()
    conn.close()
    print(f"  Cleared existing state_programs data.")

    loaded = 0
    skipped = 0

    for _, row in df.iterrows():
        state = str(row.get("state", "")).strip()
        program_name = str(row.get("program_name", "")).strip()

        if not state or not program_name:
            skipped += 1
            continue

        # Parse numeric fields
        max_credit_pct = None
        raw_pct = str(row.get("max_credit_pct", "")).strip()
        if raw_pct and raw_pct not in ("", "nan"):
            try:
                max_credit_pct = float(raw_pct.replace("%", ""))
            except (ValueError, TypeError):
                pass

        max_amount = None
        raw_amount = str(row.get("max_amount", "")).strip()
        if raw_amount and raw_amount not in ("", "nan"):
            try:
                max_amount = float(raw_amount.replace("$", "").replace(",", "").replace("M", "000000").replace("K", "000"))
            except (ValueError, TypeError):
                pass

        record = {
            "state":               state,
            "program_name":        program_name,
            "program_type":        str(row.get("program_type", "")).strip() or None,
            "eligible_uses":       str(row.get("eligible_uses", "")).strip() or None,
            "max_credit_pct":      max_credit_pct,
            "max_amount":          max_amount,
            "administering_agency": str(row.get("administering_agency", "")).strip() or None,
            "website":             str(row.get("website", "")).strip() or None,
            "notes":               str(row.get("notes", "")).strip() or None,
            "last_verified":       str(row.get("last_verified", "")).strip() or None,
        }
        # Remove None values
        record = {k: v for k, v in record.items() if v is not None}

        try:
            db.upsert_state_program(record)
            loaded += 1
        except Exception as e:
            skipped += 1
            if skipped <= 3:
                print(f"  Error inserting {state}/{program_name}: {e}")

    print(f"State programs load complete.")
    print(f"  Loaded: {loaded:,}")
    if skipped:
        print(f"  Skipped: {skipped:,}")
    print()
    print("Programs are now available in the Tools → State Programs tab.")


if __name__ == "__main__":
    main()
