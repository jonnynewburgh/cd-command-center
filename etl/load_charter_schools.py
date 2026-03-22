"""
etl/load_charter_schools.py — Load charter school data into SQLite.

This script reads charter school data from a CSV file and loads it into the
charter_schools table. It also runs the survival model to compute survival
scores for each school.

Expected CSV columns (NCES Common Core of Data format or similar):
  - nces_id, school_name, lea_name, lea_id, state, city, address, zip_code,
    county, census_tract_id, latitude, longitude, enrollment, grade_low,
    grade_high, pct_free_reduced_lunch, pct_ell, pct_sped, pct_black,
    pct_hispanic, pct_white, school_status, year_opened, year_closed, data_year

Usage:
    python etl/load_charter_schools.py --file data/raw/charter_schools.csv
    python etl/load_charter_schools.py --file data/raw/charter_schools.csv --year 2023

The script will also accept a minimal CSV and fill in missing columns with NULL.
"""

import argparse
import sys
import os
import pandas as pd
import numpy as np

# Add parent directory to path so we can import db and models
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import db
from models.charter_survival import CharterSurvivalModel


# These are the columns we expect in the CSV. Any not present will be set to None.
EXPECTED_COLUMNS = [
    "nces_id", "school_name", "lea_name", "lea_id", "state", "city",
    "address", "zip_code", "county", "census_tract_id", "latitude", "longitude",
    "enrollment", "grade_low", "grade_high", "pct_free_reduced_lunch", "pct_ell",
    "pct_sped", "pct_black", "pct_hispanic", "pct_white", "school_status",
    "year_opened", "year_closed", "data_year",
]


def load_csv(filepath: str) -> pd.DataFrame:
    """Read the CSV file and normalize column names."""
    print(f"Reading {filepath}...")
    df = pd.read_csv(filepath, dtype=str, low_memory=False)

    # Normalize column names: lowercase, strip whitespace, replace spaces with underscores
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

    print(f"  Found {len(df)} rows, columns: {list(df.columns)}")
    return df


def align_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Add any missing expected columns as NaN, drop unexpected extras."""
    for col in EXPECTED_COLUMNS:
        if col not in df.columns:
            print(f"  Warning: column '{col}' not found in CSV, will be NULL")
            df[col] = None

    return df[EXPECTED_COLUMNS]


def clean_numerics(df: pd.DataFrame) -> pd.DataFrame:
    """Convert numeric columns from string to float/int, replacing blanks with None."""
    numeric_cols = [
        "latitude", "longitude", "enrollment", "pct_free_reduced_lunch", "pct_ell",
        "pct_sped", "pct_black", "pct_hispanic", "pct_white", "year_opened",
        "year_closed", "data_year",
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


def score_schools(df: pd.DataFrame) -> pd.DataFrame:
    """
    Run the survival model on each school to compute survival_score
    and survival_risk_tier. Schools without enough data to score get
    score=None and tier='Unknown'.
    """
    model = CharterSurvivalModel()

    # Load the model if a saved version exists, otherwise use the default heuristic
    model_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "models", "charter_survival.pkl"
    )
    if os.path.exists(model_path):
        model.load(model_path)
        print("  Loaded saved survival model.")
    else:
        print("  No saved model found, using heuristic scoring.")

    scores = model.predict_batch(df)
    df["survival_score"] = scores["survival_score"]
    df["survival_risk_tier"] = scores["survival_risk_tier"]

    return df


def load_to_db(df: pd.DataFrame):
    """Upsert each row into the charter_schools table via db.py."""
    print(f"Loading {len(df)} schools into database...")
    loaded = 0
    errors = 0

    for _, row in df.iterrows():
        record = {}
        for col in EXPECTED_COLUMNS + ["survival_score", "survival_risk_tier"]:
            val = row.get(col)
            # Convert NaN to None so SQLite stores NULL
            if val is not None and not (isinstance(val, float) and np.isnan(val)):
                record[col] = val
            else:
                record[col] = None

        # school_name is required
        if not record.get("school_name"):
            print(f"  Skipping row with missing school_name: {record.get('nces_id', 'unknown')}")
            errors += 1
            continue

        try:
            db.upsert_charter_school(record)
            loaded += 1
        except Exception as e:
            print(f"  Error loading {record.get('school_name', 'unknown')}: {e}")
            errors += 1

    print(f"  Done: {loaded} loaded, {errors} errors")


def main():
    # DEPRECATED: This script required a manually downloaded CSV file.
    # Use fetch_nces_schools.py instead, which auto-downloads from the Urban Institute API.
    # Example: python etl/fetch_nces_schools.py --states CA TX NY
    print("DEPRECATED: load_charter_schools.py is superseded by fetch_nces_schools.py")
    print("  fetch_nces_schools.py auto-downloads all public schools (charter + traditional)")
    print("  Run: python etl/fetch_nces_schools.py --states <STATE> [--charter-only]")
    sys.exit(0)

    parser = argparse.ArgumentParser(description="Load charter school CSV into SQLite")
    parser.add_argument("--file", required=True, help="Path to charter schools CSV file")
    parser.add_argument("--year", type=int, help="Override data_year for all rows")
    args = parser.parse_args()

    if not os.path.exists(args.file):
        print(f"Error: file not found: {args.file}")
        sys.exit(1)

    # Make sure the database and tables exist
    db.init_db()

    df = load_csv(args.file)
    df = align_columns(df)
    df = clean_numerics(df)

    if args.year:
        df["data_year"] = args.year

    df = score_schools(df)
    load_to_db(df)

    print("Charter school load complete.")


if __name__ == "__main__":
    main()
