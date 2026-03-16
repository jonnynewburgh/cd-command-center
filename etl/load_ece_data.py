"""
etl/load_ece_data.py — Load early care and education (ECE) facility data.

Data source: State child care licensing databases.

Unlike schools (NCES) or health centers (HRSA), there is no single national
ECE database. Each state maintains its own licensing registry. Most states
publish their licensed provider lists as downloadable CSV or Excel files.

States with confirmed open-data portal downloads (just download and run):

    CA: https://data.chhs.ca.gov/dataset/ccl-facilities
        → CSV, ~100k rows (CDSS Community Care Licensing)
        → Also: https://data.ca.gov/dataset/community-care-licensing-facilities

    CO: https://data.colorado.gov/d/a9rr-k8mu
        → CSV, updated monthly. *** INCLUDES COLORADO SHINES QRIS STAR RATING ***
        → The star_rating column in this tool maps to the "Quality Rating Level" column

    DE: https://data.delaware.gov  (search "licensed child care providers")
        → Multiple datasets including by age group and by county/capacity

    NY: https://data.ny.gov/Human-Services/Child-Care-Regulated-Programs/cb42-qumz
        → Regulated Programs CSV. Note: NYC programs are NOT included (separate DOHMH system)

    PA: https://data.pa.gov/Services-Near-You/Child-Care-Providers-including-Early-Learning-Prog/ajn5-kaxt
        → Open certified child care facilities + early learning programs, updated monthly

    TX: https://data.texas.gov  (search "HHSC CCL Daycare and Residential Operations")
        → XLSX from Texas Health and Human Services Commission

    WA: https://data.wa.gov/education/DCYF-Licensed-Childcare-Center-and-School-Age-Prog/was8-3ni8
        → Licensed childcare centers and school-age programs (DCYF)

States where bulk download requires FOIA or web scraping:
    FL: https://childcarefacilities.myflfamilies.com/
    IL: https://sunshine.dcfs.illinois.gov/content/licensing/providersearch.aspx
    GA: https://www.decal.ga.gov/BCS/Search.aspx
    OH: https://childcaresearch.ohio.gov/  (search only, no bulk download)

    For other states: search "[STATE] child care licensing data download" or
    "[STATE] licensed child care providers list", or check catalog.data.gov
    with the tag "child-care".

About QRIS (Quality Rating and Improvement System):
    26+ states run a QRIS program that rates child care quality on a star scale.
    Star ratings are stored in the star_rating column of ece_centers.
    Colorado is the only state that includes QRIS stars in the bulk download file.
    Most other states publish ratings through separate portals, e.g.:
        NC: Star Rated License (included in state licensing download)
        TN: Star Quality Program (https://www.tn.gov/humanservices/for-families/child-care-payment-assistance-program/star-quality-child-care-program.html)
        KY: STARS for KIDS NOW
        OH: Step Up To Quality (https://jfs.ohio.gov/childcare/sutq/)
    National QRIS aggregate info: https://qualitycompendium.org/view-state-profiles

Column mapping:
    State CSV column names vary widely. This script maps common patterns to
    our schema. Columns are normalized (lowercased, stripped) before matching.
    If your state's column names don't match, add them to COLUMN_MAP below.

Usage:
    # Load a state CSV file:
    python etl/load_ece_data.py --file data/raw/ca_licensed_facilities.csv --state CA

    # Load with explicit source label (shown in the data_source column):
    python etl/load_ece_data.py --file data/raw/ny_child_care.csv --state NY --source "NY OCFS"

    # Preview column names without loading:
    python etl/load_ece_data.py --file data/raw/tx_childcare.xlsx --columns-only

    # Load but only active facilities:
    python etl/load_ece_data.py --file data/raw/ca_licensed_facilities.csv --state CA --active-only
"""

import argparse
import sys
import os
import pandas as pd
import numpy as np

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import db

# ---------------------------------------------------------------------------
# Column mapping: state CSV header (normalized) → our ece_centers column
#
# Most state licensing files use some variation of these column names.
# Each key here maps one possible source column name to our target column.
# ---------------------------------------------------------------------------

COLUMN_MAP = {
    # Facility / license identifiers
    "license number":           "license_id",
    "license id":               "license_id",
    "license no":               "license_id",
    "credential number":        "license_id",
    "facility id":              "license_id",
    "facility number":          "license_id",
    "provider id":              "license_id",
    "operation number":         "license_id",      # TX HHSC uses this
    "operation id":             "license_id",

    # Provider / facility names
    "facility name":            "provider_name",
    "provider name":            "provider_name",
    "operation name":           "provider_name",   # TX
    "program name":             "provider_name",
    "agency name":              "provider_name",
    "business name":            "provider_name",
    "licensee name":            "provider_name",

    # Operator / owner (often separate from facility name)
    "operator name":            "operator_name",
    "owner name":               "operator_name",
    "owner/operator":           "operator_name",
    "contact name":             "operator_name",

    # Facility type
    "facility type":            "facility_type",
    "program type":             "facility_type",
    "operation type":           "facility_type",   # TX
    "care type":                "facility_type",
    "license type description": "facility_type",
    "type of care":             "facility_type",
    "provider type":            "facility_type",

    # License / credential type
    "license type":             "license_type",
    "credential type":          "license_type",
    "license category":         "license_type",

    # Status
    "license status":           "license_status",
    "status":                   "license_status",
    "operation status":         "license_status",  # TX
    "facility status":          "license_status",
    "current status":           "license_status",

    # Capacity
    "licensed capacity":        "capacity",
    "capacity":                 "capacity",
    "total capacity":           "capacity",
    "max capacity":             "capacity",
    "licensed slots":           "capacity",
    "number of slots":          "capacity",

    # Ages served
    "ages served":              "ages_served",
    "age group":                "ages_served",
    "age groups served":        "ages_served",
    "type of service":          "ages_served",
    "care level":               "ages_served",

    # Subsidy acceptance
    "accepts subsidies":        "accepts_subsidies",
    "subsidy program":          "accepts_subsidies",
    "ccdf":                     "accepts_subsidies",
    "title xx":                 "accepts_subsidies",
    "accepts ccdf":             "accepts_subsidies",
    "subsidized":               "accepts_subsidies",

    # Quality rating
    "star rating":              "star_rating",
    "quality rating":           "star_rating",
    "qris rating":              "star_rating",
    "quality level":            "star_rating",
    "stars":                    "star_rating",

    # Address
    "address":                  "address",
    "street address":           "address",
    "physical address":         "address",
    "mailing address":          "address",
    "address 1":                "address",
    "facility address":         "address",
    "operation address":        "address",  # TX

    # City
    "city":                     "city",
    "facility city":            "city",
    "operation city":           "city",

    # State
    "state":                    "state",
    "facility state":           "state",
    "state abbreviation":       "state",

    # ZIP
    "zip":                      "zip_code",
    "zip code":                 "zip_code",
    "postal code":              "zip_code",
    "facility zip":             "zip_code",

    # County
    "county":                   "county",
    "county name":              "county",
    "facility county":          "county",

    # Geography
    "latitude":                 "latitude",
    "lat":                      "latitude",
    "longitude":                "longitude",
    "lon":                      "longitude",
    "long":                     "longitude",
    "lng":                      "longitude",
    "x":                        "longitude",  # some GIS exports use X/Y
    "y":                        "latitude",
    "census tract":             "census_tract_id",
    "census tract number":      "census_tract_id",
}

# Status values that count as "active"
ACTIVE_STATUSES = {
    "active", "open", "licensed", "issued", "current", "in operation",
    "operating", "approved", "valid", "full license",
}

# Values in subsidy columns that mean "yes"
SUBSIDY_YES_VALUES = {"yes", "y", "true", "1", "x", "accepts", "participates"}


def load_file(path: str) -> pd.DataFrame:
    """Load CSV or Excel file, trying common encodings."""
    if path.endswith((".xlsx", ".xls")):
        return pd.read_excel(path, dtype=str)

    # CSV: try UTF-8 first, fall back to latin-1
    try:
        return pd.read_csv(path, dtype=str, low_memory=False)
    except UnicodeDecodeError:
        return pd.read_csv(path, dtype=str, encoding="latin-1", low_memory=False)


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [str(c).strip().lower() for c in df.columns]
    return df


def map_columns(df: pd.DataFrame) -> tuple[pd.DataFrame, list]:
    """
    Rename matched columns and return (mapped_df, unmatched_cols).
    Columns not in COLUMN_MAP are dropped from the result but reported.
    """
    rename = {col: COLUMN_MAP[col] for col in df.columns if col in COLUMN_MAP}
    unmatched = [col for col in df.columns if col not in COLUMN_MAP]

    df = df.rename(columns=rename)
    keep = list(set(COLUMN_MAP.values()))
    present = [c for c in keep if c in df.columns]
    return df[present], unmatched


def derive_active_flag(df: pd.DataFrame, active_only: bool) -> pd.DataFrame:
    """
    Normalize license_status to a consistent set of values.
    When active_only=True, drop rows whose status isn't in ACTIVE_STATUSES.
    """
    if "license_status" not in df.columns:
        return df

    df["license_status"] = df["license_status"].fillna("Unknown").str.strip()

    if active_only:
        before = len(df)
        active_mask = df["license_status"].str.lower().isin(ACTIVE_STATUSES)
        df = df[active_mask].copy()
        print(f"  Active facilities: {len(df):,} of {before:,} total")

    return df


def derive_subsidy_flag(df: pd.DataFrame) -> pd.DataFrame:
    """Convert the accepts_subsidies column to 0/1 integer."""
    if "accepts_subsidies" not in df.columns:
        return df

    df["accepts_subsidies"] = df["accepts_subsidies"].apply(
        lambda v: 1 if str(v).strip().lower() in SUBSIDY_YES_VALUES else 0
    )
    return df


def clean_record(record: dict) -> dict:
    """Coerce types and strip whitespace for a single record dict."""
    int_cols = {"capacity"}
    float_cols = {"latitude", "longitude", "star_rating"}

    cleaned = {}
    for key, val in record.items():
        if isinstance(val, float) and np.isnan(val):
            val = None
        elif isinstance(val, str):
            val = val.strip() or None

        if key in float_cols and val is not None:
            try:
                val = float(val)
            except (TypeError, ValueError):
                val = None
        elif key in int_cols and val is not None:
            try:
                val = int(float(val))
            except (TypeError, ValueError):
                val = None

        cleaned[key] = val
    return cleaned


def main():
    parser = argparse.ArgumentParser(
        description="Load ECE (early care and education) facility data from a state CSV/Excel file"
    )
    parser.add_argument(
        "--file",
        required=True,
        metavar="PATH",
        help="Path to the state-provided CSV or Excel file",
    )
    parser.add_argument(
        "--state",
        metavar="XX",
        help=(
            "2-letter state abbreviation (e.g. CA). Used to fill in the state column "
            "if it isn't in the file, and to label records in data_source."
        ),
    )
    parser.add_argument(
        "--source",
        metavar="LABEL",
        help=(
            "Label for the data_source column (e.g. 'CA CCLD', 'TX HHSC'). "
            "Defaults to the state abbreviation if omitted."
        ),
    )
    parser.add_argument(
        "--active-only",
        action="store_true",
        default=True,
        help="Only load facilities with an active license status (default: on)",
    )
    parser.add_argument(
        "--all-facilities",
        action="store_true",
        help="Load all facilities including inactive/revoked (overrides --active-only)",
    )
    parser.add_argument(
        "--columns-only",
        action="store_true",
        help=(
            "Print the column names found in the file and exit without loading. "
            "Use this to see what columns are available and add any missing ones to COLUMN_MAP."
        ),
    )
    parser.add_argument(
        "--year",
        type=int,
        default=None,
        help="Data year to record (defaults to current year)",
    )
    args = parser.parse_args()

    if not os.path.exists(args.file):
        print(f"Error: file not found: {args.file}")
        sys.exit(1)

    print(f"CD Command Center — ECE Facility Data Load")
    print(f"  File: {args.file}")

    raw_df = load_file(args.file)
    print(f"  Raw rows: {len(raw_df):,}  |  columns: {len(raw_df.columns)}")

    raw_df = normalize_columns(raw_df)

    if args.columns_only:
        print("\nColumns found in this file:")
        for col in sorted(raw_df.columns):
            mapped = COLUMN_MAP.get(col, "— (not mapped)")
            print(f"  '{col}'  →  {mapped}")
        print("\nAdd any unmapped columns to COLUMN_MAP in this script to include them.")
        return

    df, unmatched_cols = map_columns(raw_df)

    # Report mapping results
    print(f"  Columns mapped: {list(df.columns)}")
    if unmatched_cols:
        print(f"  Unmapped columns ({len(unmatched_cols)}): {unmatched_cols[:10]}"
              + ("..." if len(unmatched_cols) > 10 else ""))
    print()

    if df.empty or "provider_name" not in df.columns:
        print("ERROR: No usable columns found after mapping.")
        print("  Run with --columns-only to inspect the raw column names, then")
        print("  add them to COLUMN_MAP at the top of this script.")
        sys.exit(1)

    active_only = not args.all_facilities
    df = derive_active_flag(df, active_only)
    df = derive_subsidy_flag(df)

    # Fill state column from --state arg if missing or blank
    if args.state:
        state_upper = args.state.upper()
        if "state" not in df.columns:
            df["state"] = state_upper
        else:
            df["state"] = df["state"].fillna(state_upper)

    # data_source label
    source_label = args.source or (args.state.upper() if args.state else "Unknown")
    df["data_source"] = source_label

    # data_year
    import datetime
    df["data_year"] = args.year or datetime.date.today().year

    # Generate license_id if column is missing (not ideal but allows loading)
    if "license_id" not in df.columns:
        print("WARNING: No license ID column found. Generating synthetic IDs.")
        print("  These IDs are not stable — re-loading will insert duplicates.")
        print("  Add the license ID column to COLUMN_MAP to fix this.")
        df["license_id"] = [f"AUTO_{source_label}_{i+1:07d}" for i in range(len(df))]

    if df.empty:
        print("No records to load after filtering. Check --active-only / --all-facilities.")
        sys.exit(0)

    db.init_db()

    loaded = 0
    errors = 0
    for _, row in df.iterrows():
        record = clean_record(row.to_dict())

        if not record.get("provider_name"):
            errors += 1
            continue

        try:
            db.upsert_ece(record)
            loaded += 1
        except Exception as e:
            lid = record.get("license_id", "?")
            print(f"    DB error for {lid}: {e}")
            errors += 1

        if loaded % 5000 == 0 and loaded > 0:
            print(f"  Loaded {loaded:,}...")

    print()
    print("Done.")
    print(f"  Loaded: {loaded:,} facilities")
    if errors:
        print(f"  Skipped/errors: {errors:,}")

    summary = db.get_ece_summary()
    print()
    print("Database now contains:")
    print(f"  Total ECE centers:   {summary.get('total_centers', 0):,}")
    print(f"  Active centers:      {summary.get('active_centers', 0):,}")
    total_cap = summary.get("total_capacity") or 0
    if total_cap:
        print(f"  Total capacity:      {total_cap:,} children")
    print(f"  States covered:      {summary.get('states_covered', 0):,}")


if __name__ == "__main__":
    main()
