"""
etl/fetch_fqhc.py — Load HRSA health center site data into the fqhc table.

Data source: HRSA Health Center Program
  The HRSA publishes a downloadable list of all Health Center Program-funded
  sites and look-alike sites. The file is updated periodically.

  Download URL (no account required):
    https://data.hrsa.gov/DataDownload/DD_Files/Health_Center_Service_Delivery_and_LookAlikeStates_Sites.zip

  The ZIP contains one CSV file with one row per health center site.

Usage:
    # Auto-download from HRSA and load all sites:
    python etl/fetch_fqhc.py

    # Use a file you already downloaded:
    python etl/fetch_fqhc.py --file data/raw/hrsa_health_centers.csv

    # Only load specific states:
    python etl/fetch_fqhc.py --states CA TX NY

    # Only load active sites (default behavior):
    python etl/fetch_fqhc.py --active-only

    # Load all sites including inactive:
    python etl/fetch_fqhc.py --all-sites

Column mapping:
    The HRSA CSV uses verbose headers. This script maps them to our schema.
    If HRSA changes their column names in a future release, update COLUMN_MAP below.
"""

import argparse
import sys
import os
import io
import zipfile
import requests
import pandas as pd
import numpy as np

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import db

# ---------------------------------------------------------------------------
# HRSA download URL
# ---------------------------------------------------------------------------

HRSA_DOWNLOAD_URL = (
    "https://data.hrsa.gov/DataDownload/DD_Files/"
    "Health_Center_Service_Delivery_and_LookAlike_Sites.csv"
)

# How long to wait for the download (the file is ~5 MB)
DOWNLOAD_TIMEOUT = 60

# ---------------------------------------------------------------------------
# Column mapping: HRSA CSV header (normalized) → our fqhc table column
#
# HRSA column names as of the 2024 data release. They normalize to lowercase
# with extra whitespace stripped before matching.
# ---------------------------------------------------------------------------

COLUMN_MAP = {
    # Site identification — only ONE of these will exist in any given HRSA file
    # Current HRSA file (2024/2025): site-level unique key
    "bphc assigned number":                   "bhcmis_id",
    # Older HRSA file formats
    "bhcmisid":                               "bhcmis_id",
    "bhcmis id":                              "bhcmis_id",
    "site id":                                "bhcmis_id",
    "health center site id":                  "bhcmis_id",
    # NOTE: "bhcmis organization identification number" is an ORG-level ID (not site-level).
    # Do NOT map it to bhcmis_id — it would collapse all sites of one org into one row.

    # Organization / site names
    "health center name":                     "health_center_name",
    "health center":                          "health_center_name",
    "organization name":                      "health_center_name",
    "site name":                              "site_name",

    # Address
    "site address":                           "site_address",
    "address":                                "site_address",
    "street address":                         "site_address",
    "site city":                              "city",
    "city":                                   "city",
    "site state abbreviation":                "state",
    "state abbreviation":                     "state",
    "state":                                  "state",
    "site postal code":                       "zip_code",
    "zip code":                               "zip_code",
    "postal code":                            "zip_code",
    "site county":                            "county",
    "county":                                 "county",

    # Geography — lat/lon (only ONE of these will exist per file vintage)
    "geocoded latitude":                      "latitude",
    "latitude":                               "latitude",
    # Current HRSA file (2024/2025)
    "geocoding artifact address primary y coordinate": "latitude",
    "geocoded longitude":                     "longitude",
    "longitude":                              "longitude",
    # Current HRSA file (2024/2025)
    "geocoding artifact address primary x coordinate": "longitude",
    # Census tract (not in current HRSA file; assigned separately via assign_census_tracts.py)
    "census tract":                           "census_tract_id",
    "census tract number":                    "census_tract_id",
    # County (prefer the verbose full name in current file)
    "complete county name":                   "county",
    "site county":                            "county",
    "county":                                 "county",

    # Classification (only ONE site-type column will exist per vintage)
    "site type description":                  "site_type",
    "site type":                              "site_type",
    # Current HRSA file (2024/2025)
    "health center service delivery site location setting description": "site_type",
    "site status description":               "site_status_raw",  # used to derive is_active
    "site status":                            "site_status_raw",
    # Health center type (only ONE will exist per vintage)
    "health center type":                     "health_center_type",
    "health center program grantee type":     "health_center_type",
    # Current HRSA file (2024/2025)
    "grantee organization type description":  "health_center_type",

    # Patient data (UDS)
    "total patients":                         "total_patients",
    "patients":                               "total_patients",
    "patients at or below 200% federal poverty level":  "patients_below_200pct_poverty",
    "patients below 200% fpl":                "patients_below_200pct_poverty",
}

# ---------------------------------------------------------------------------
# Status values that mean "active"
# ---------------------------------------------------------------------------

ACTIVE_STATUSES = {"active", "open", "operating", "funded", "grantee"}


def download_hrsa_zip() -> pd.DataFrame:
    """
    Download the HRSA health center CSV file directly.
    (HRSA changed from a ZIP to a direct CSV download in 2025.)
    """
    print(f"  Downloading HRSA data from:\n    {HRSA_DOWNLOAD_URL}")
    resp = requests.get(HRSA_DOWNLOAD_URL, timeout=DOWNLOAD_TIMEOUT, stream=True)
    resp.raise_for_status()

    chunks = []
    total = 0
    for chunk in resp.iter_content(chunk_size=65536):
        chunks.append(chunk)
        total += len(chunk)
        print(f"\r  Downloaded {total / 1024:.0f} KB...", end="", flush=True)
    print()

    raw_bytes = b"".join(chunks)
    try:
        df = pd.read_csv(io.BytesIO(raw_bytes), dtype=str, low_memory=False)
    except UnicodeDecodeError:
        df = pd.read_csv(io.BytesIO(raw_bytes), dtype=str, encoding="latin-1", low_memory=False)
    return df


def extract_csv_from_zip(df: pd.DataFrame) -> pd.DataFrame:
    """
    No-op passthrough — previously extracted CSV from a ZIP.
    HRSA now serves a direct CSV so download_hrsa_zip() returns a DataFrame.
    Kept to avoid changing the call site.
    """
    return df


def load_csv_file(path: str) -> pd.DataFrame:
    """Load a CSV file the user already downloaded."""
    print(f"  Reading {path}...")
    # Try UTF-8 first, fall back to latin-1
    try:
        return pd.read_csv(path, dtype=str, low_memory=False)
    except UnicodeDecodeError:
        return pd.read_csv(path, dtype=str, encoding="latin-1", low_memory=False)


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Lowercase and strip column names for consistent matching."""
    df.columns = [str(c).strip().lower() for c in df.columns]
    return df


def map_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Rename columns from HRSA names to our schema names.
    Columns not in COLUMN_MAP are dropped.

    When multiple source columns map to the same target (different-vintage
    names for the same field), keep only the first occurrence.
    """
    rename = {col: COLUMN_MAP[col] for col in df.columns if col in COLUMN_MAP}
    df = df.rename(columns=rename)

    # Keep only schema columns; when duplicates exist, keep the first occurrence.
    schema_cols = set(COLUMN_MAP.values())
    seen = set()
    final_cols = []
    for col in df.columns:
        if col in seen:
            continue
        seen.add(col)
        if col in schema_cols:
            final_cols.append(col)
    return df[final_cols]


def derive_is_active(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert the raw site status text column to an integer is_active flag.
    Drops the raw status column afterward.
    """
    if "site_status_raw" in df.columns:
        df["is_active"] = df["site_status_raw"].apply(
            lambda v: 1 if str(v).strip().lower() in ACTIVE_STATUSES else 0
        )
        df = df.drop(columns=["site_status_raw"])
    else:
        # If no status column found, assume all records are active
        df["is_active"] = 1
    return df


def clean_record(record: dict) -> dict:
    """
    Clean up a single record dict before upserting:
    - Convert numeric fields to int/float (or None if blank/NaN)
    - Strip string whitespace
    - Ensure bhcmis_id is present (skip rows without it)
    """
    numeric_cols = {"latitude", "longitude", "total_patients", "patients_below_200pct_poverty"}

    cleaned = {}
    for key, val in record.items():
        # Treat pandas NaN and empty strings as None
        if isinstance(val, float) and np.isnan(val):
            val = None
        elif isinstance(val, str):
            val = val.strip() or None

        # Cast numeric fields
        if key in numeric_cols and val is not None:
            try:
                if key in ("latitude", "longitude"):
                    val = float(val)
                else:
                    val = int(float(val))
            except (TypeError, ValueError):
                val = None

        cleaned[key] = val

    return cleaned


def main():
    parser = argparse.ArgumentParser(
        description="Load HRSA health center site data into the fqhc table"
    )
    parser.add_argument(
        "--file",
        metavar="PATH",
        help=(
            "Path to a CSV file already downloaded from HRSA. "
            "If omitted, the script downloads the latest file automatically."
        ),
    )
    parser.add_argument(
        "--states",
        nargs="+",
        metavar="STATE",
        help="Only load sites in these states (2-letter abbreviations)",
    )
    parser.add_argument(
        "--active-only",
        action="store_true",
        default=True,
        help="Only load active/open sites (default: on)",
    )
    parser.add_argument(
        "--all-sites",
        action="store_true",
        help="Load all sites including inactive/closed (overrides --active-only)",
    )
    args = parser.parse_args()

    active_only = not args.all_sites

    print("CD Command Center — FQHC / Health Center Data Fetch")
    print(f"  Active sites only: {'yes' if active_only else 'no (all sites)'}")
    if args.states:
        print(f"  States: {', '.join(args.states)}")
    print()

    # Load raw data
    if args.file:
        raw_df = load_csv_file(args.file)
    else:
        zip_bytes = download_hrsa_zip()
        raw_df = extract_csv_from_zip(zip_bytes)

    print(f"  Raw rows: {len(raw_df):,}")
    print(f"  Raw columns ({len(raw_df.columns)}): {list(raw_df.columns[:8])}{'...' if len(raw_df.columns) > 8 else ''}")
    print()

    # Normalize and map columns
    raw_df = normalize_columns(raw_df)
    df = map_columns(raw_df)

    # Show what we got
    matched_cols = list(df.columns)
    print(f"  Columns mapped to our schema: {matched_cols}")
    print()

    if df.empty:
        print("ERROR: No columns matched our schema. The HRSA CSV format may have changed.")
        print("  Check COLUMN_MAP at the top of this script and update it to match")
        print(f"  the actual column names: {list(raw_df.columns)}")
        sys.exit(1)

    # Derive is_active flag from status column
    df = derive_is_active(df)

    # Filter to active sites if requested
    if active_only and "is_active" in df.columns:
        before = len(df)
        df = df[df["is_active"] == 1]
        print(f"  Filtered to active sites: {len(df):,} of {before:,}")

    # Filter by state
    if args.states and "state" in df.columns:
        states_upper = [s.upper() for s in args.states]
        before = len(df)
        df = df[df["state"].str.upper().isin(states_upper)]
        print(f"  Filtered to {', '.join(states_upper)}: {len(df):,} of {before:,}")

    if df.empty:
        print("No records to load after filtering.")
        sys.exit(0)

    # Ensure bhcmis_id exists — it's required for the unique constraint
    if "bhcmis_id" not in df.columns:
        print("WARNING: No bhcmis_id column found. Generating synthetic IDs from row index.")
        print("  Rows will always be inserted (not upserted) since IDs are not stable.")
        df["bhcmis_id"] = [f"AUTO_{i+1:07d}" for i in range(len(df))]

    # Set data_year to current year of the dataset (we don't have a year column)
    if "data_year" not in df.columns:
        import datetime
        df["data_year"] = datetime.date.today().year

    # Load into database
    db.init_db()

    loaded = 0
    errors = 0
    for _, row in df.iterrows():
        record = clean_record(row.to_dict())

        # Skip rows without a meaningful name
        if not record.get("health_center_name") and not record.get("site_name"):
            errors += 1
            continue

        try:
            db.upsert_fqhc(record)
            loaded += 1
        except Exception as e:
            site_id = record.get("bhcmis_id", "?")
            print(f"    DB error for site {site_id}: {e}")
            errors += 1

        if loaded % 1000 == 0 and loaded > 0:
            print(f"  Loaded {loaded:,}...")

    print()
    print("Done.")
    print(f"  Loaded: {loaded:,} sites")
    if errors:
        print(f"  Skipped/errors: {errors:,}")

    summary = db.get_fqhc_summary()
    print()
    print("Database now contains:")
    print(f"  Total FQHC sites:     {summary.get('total_sites', 0):,}")
    print(f"  Active sites:         {summary.get('active_sites', 0):,}")
    print(f"  Health center orgs:   {summary.get('unique_health_centers', 0):,}")
    print(f"  States served:        {summary.get('states_served', 0):,}")
    total_patients = summary.get("total_patients") or 0
    if total_patients:
        print(f"  Total patients:       {total_patients:,}")


if __name__ == "__main__":
    main()
