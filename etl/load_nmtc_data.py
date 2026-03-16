"""
etl/load_nmtc_data.py — Load NMTC project and CDE allocation data from CDFI Fund Excel.

Data source: CDFI Fund NMTC Public Data Release
  Download the Excel file from: https://www.cdfifund.gov/documents/data-releases
  (Look for "FY 2024 NMTC Public Data Release: 2003–2022 Data File")
  Save it to: data/raw/nmtc_public_data_2024.xlsx

The Excel file has multiple sheets. This script reads two key sheets:
  - QLICI (project-level investments) → nmtc_projects table
  - CDE (CDE-level allocation awards) → cde_allocations table

Usage:
    python etl/load_nmtc_data.py --file data/raw/nmtc_public_data_2024.xlsx
    python etl/load_nmtc_data.py --file data/raw/nmtc_public_data_2024.xlsx --sheet-names
"""

import argparse
import sys
import os
import pandas as pd
import numpy as np

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import db

# ---------------------------------------------------------------------------
# Column mapping: CDFI Fund Excel column names → our database columns
#
# The CDFI Fund uses verbose column headers that change slightly between
# annual releases. This mapping covers the FY 2024 release (2003–2022 data).
# Column names are normalized (lowercased, stripped) before matching.
# ---------------------------------------------------------------------------

# For the QLICI / project-level sheet
QLICI_COLUMN_MAP = {
    # CDFI Fund header (normalized) → our column name
    "cdfi fund project id":          "cdfi_project_id",
    "project id":                    "cdfi_project_id",
    "cde name":                      "cde_name",
    "cde":                           "cde_name",
    "community development entity":  "cde_name",
    "project name":                  "project_name",
    "project/business name":         "project_name",
    "project type":                  "project_type",
    "investment type":               "project_type",
    "state":                         "state",
    "city":                          "city",
    "address":                       "address",
    "zip":                           "zip_code",
    "zip code":                      "zip_code",
    "census tract":                  "census_tract_id",
    "census tract number":           "census_tract_id",
    "total project cost":            "total_investment",
    "total project size":            "total_investment",
    "qlici amount":                  "qlici_amount",
    "total qlici amount":            "qlici_amount",
    "allocation year":               "allocation_year",
    "fiscal year":                   "fiscal_year",
    "year":                          "fiscal_year",
    "projected ft jobs created":     "jobs_created",
    "projected jobs created":        "jobs_created",
    "projected ft jobs retained":    "jobs_retained",
    "projected jobs retained":       "jobs_retained",
    "project description":           "project_description",
    "description":                   "project_description",
    "latitude":                      "latitude",
    "longitude":                     "longitude",
}

# For the CDE / allocation sheet
CDE_COLUMN_MAP = {
    "cde name":                      "cde_name",
    "community development entity":  "cde_name",
    "cde":                           "cde_name",
    "state":                         "state",
    "city":                          "city",
    "address":                       "hq_address",
    "hq address":                    "hq_address",
    "total allocation amount":       "allocation_amount",
    "allocation amount":             "allocation_amount",
    "calendar year":                 "allocation_year",
    "allocation year":               "allocation_year",
    "nmtc round":                    "round_number",
    "round":                         "round_number",
    "application round":             "round_number",
    "service area":                  "service_areas",
    "service areas":                 "service_areas",
    "geographic focus":              "service_areas",
}


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize DataFrame column names: lowercase and strip whitespace."""
    df.columns = [c.strip().lower() for c in df.columns]
    return df


def apply_column_map(df: pd.DataFrame, col_map: dict) -> pd.DataFrame:
    """
    Rename columns according to col_map (normalized source → target name).
    Columns not in the map are dropped.
    """
    # Build rename dict for columns that exist in both df and col_map
    rename = {col: col_map[col] for col in df.columns if col in col_map}
    df = df.rename(columns=rename)

    # Keep only columns that appear as values in the map
    target_cols = set(col_map.values())
    keep = [c for c in df.columns if c in target_cols]
    return df[keep]


def clean_numeric(df: pd.DataFrame, cols: list) -> pd.DataFrame:
    """Convert listed columns to numeric, replacing non-numeric with None."""
    for col in cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def clean_census_tract_id(val) -> str | None:
    """
    Normalize a census tract FIPS value to an 11-digit string.
    The CDFI Fund data sometimes stores tracts as numbers (dropping leading zeros)
    or with decimal points (e.g. 6037201300.0).
    """
    if val is None or (isinstance(val, float) and np.isnan(val)):
        return None
    try:
        # Remove decimal and zero-pad to 11 digits
        s = str(int(float(val)))
        return s.zfill(11)
    except (TypeError, ValueError):
        return str(val).strip() or None


def detect_sheet(xl: pd.ExcelFile, keywords: list[str]) -> str | None:
    """
    Find the first sheet name containing any of the given keywords (case-insensitive).
    Returns the sheet name or None if not found.
    """
    for sheet in xl.sheet_names:
        if any(kw.lower() in sheet.lower() for kw in keywords):
            return sheet
    return None


def load_projects(xl: pd.ExcelFile, sheet_name: str) -> int:
    """
    Load the QLICI/project sheet into the nmtc_projects table.
    Returns number of records loaded.
    """
    print(f"  Reading project sheet: '{sheet_name}'...")
    df = xl.parse(sheet_name, dtype=str)
    print(f"  Raw columns in sheet ({len(df.columns)}):")
    for c in df.columns:
        print(f"    - '{c}'")
    df = normalize_columns(df)

    # Show which columns matched our mapping
    matched = [col for col in df.columns if col in QLICI_COLUMN_MAP]
    unmatched = [col for col in df.columns if col not in QLICI_COLUMN_MAP]
    print(f"  Mapped columns ({len(matched)}): {matched}")
    if unmatched:
        print(f"  Unmapped columns ({len(unmatched)}): {unmatched}")

    df = apply_column_map(df, QLICI_COLUMN_MAP)

    if df.empty:
        print("  No data found in project sheet after column mapping.")
        print("  This likely means no column names matched QLICI_COLUMN_MAP.")
        return 0

    print(f"  Found {len(df):,} project rows with columns: {list(df.columns)}")

    # Numeric columns
    df = clean_numeric(df, [
        "total_investment", "qlici_amount", "allocation_year",
        "fiscal_year", "jobs_created", "jobs_retained", "latitude", "longitude",
    ])

    # Normalize census tract IDs
    if "census_tract_id" in df.columns:
        df["census_tract_id"] = df["census_tract_id"].apply(clean_census_tract_id)

    # Generate a cdfi_project_id if not present (use row index as fallback)
    if "cdfi_project_id" not in df.columns:
        df["cdfi_project_id"] = [f"AUTO_{i+1:06d}" for i in range(len(df))]

    loaded = 0
    errors = 0
    for _, row in df.iterrows():
        record = {}
        for col in df.columns:
            val = row[col]
            record[col] = None if (isinstance(val, float) and np.isnan(val)) else val

        if not record.get("cde_name") and not record.get("project_name"):
            errors += 1
            continue

        try:
            db.upsert_nmtc_project(record)
            loaded += 1
        except Exception as e:
            print(f"    Error loading project {record.get('cdfi_project_id', '?')}: {e}")
            errors += 1

    print(f"  Loaded: {loaded:,} projects" + (f", {errors} errors" if errors else ""))
    return loaded


def load_cdes(xl: pd.ExcelFile, sheet_name: str) -> int:
    """
    Load the CDE allocation sheet into the cde_allocations table.
    Returns number of records loaded.
    """
    print(f"  Reading CDE sheet: '{sheet_name}'...")
    df = xl.parse(sheet_name, dtype=str)
    print(f"  Raw columns in sheet ({len(df.columns)}):")
    for c in df.columns:
        print(f"    - '{c}'")
    df = normalize_columns(df)

    matched = [col for col in df.columns if col in CDE_COLUMN_MAP]
    unmatched = [col for col in df.columns if col not in CDE_COLUMN_MAP]
    print(f"  Mapped columns ({len(matched)}): {matched}")
    if unmatched:
        print(f"  Unmapped columns ({len(unmatched)}): {unmatched}")

    df = apply_column_map(df, CDE_COLUMN_MAP)

    if df.empty:
        print("  No data found in CDE sheet after column mapping.")
        return 0

    print(f"  Found {len(df):,} CDE rows with columns: {list(df.columns)}")

    df = clean_numeric(df, ["allocation_amount", "allocation_year", "round_number"])

    loaded = 0
    errors = 0
    for _, row in df.iterrows():
        record = {}
        for col in df.columns:
            val = row[col]
            record[col] = None if (isinstance(val, float) and np.isnan(val)) else val

        if not record.get("cde_name"):
            errors += 1
            continue

        # Ensure allocation_year is present (required for unique constraint)
        if not record.get("allocation_year"):
            record["allocation_year"] = 0  # placeholder for CDEs missing year

        try:
            db.upsert_cde_allocation(record)
            loaded += 1
        except Exception as e:
            print(f"    Error loading CDE {record.get('cde_name', '?')}: {e}")
            errors += 1

    print(f"  Loaded: {loaded:,} CDE records" + (f", {errors} errors" if errors else ""))
    return loaded


def main():
    parser = argparse.ArgumentParser(
        description="Load NMTC project and CDE data from CDFI Fund Excel file"
    )
    parser.add_argument(
        "--file",
        required=True,
        help="Path to CDFI Fund NMTC public data Excel file",
    )
    parser.add_argument(
        "--sheet-names",
        action="store_true",
        help="Print the sheet names in the Excel file and exit (useful for debugging)",
    )
    args = parser.parse_args()

    if not os.path.exists(args.file):
        print(f"Error: file not found: {args.file}")
        print("Download the NMTC Public Data Release Excel file from:")
        print("  https://www.cdfifund.gov/documents/data-releases")
        sys.exit(1)

    print(f"Opening {args.file}...")
    xl = pd.ExcelFile(args.file)

    if args.sheet_names:
        print("Sheets in this file:")
        for s in xl.sheet_names:
            print(f"  - {s}")
        return

    print(f"Sheets found ({len(xl.sheet_names)}):")
    for s in xl.sheet_names:
        print(f"  - '{s}'")
    print()

    db.init_db()

    # Auto-detect the QLICI/project sheet and CDE sheet
    project_keywords = ["QLICI", "Project", "Investment", "QALICB"]
    cde_keywords = ["CDE", "Allocation", "Allocatee"]

    project_sheet = detect_sheet(xl, project_keywords)
    cde_sheet = detect_sheet(xl, cde_keywords)

    if project_sheet:
        print(f"  Auto-detected project sheet: '{project_sheet}'")
    else:
        print(f"WARNING: Could not auto-detect project/QLICI sheet.")
        print(f"  Looked for keywords: {project_keywords}")
        print(f"  Available sheets: {xl.sheet_names}")
        print(f"  Try re-running with --sheet-names to inspect, or check if")
        print(f"  the CDFI Fund changed their sheet naming in a newer release.")
        print()

    if cde_sheet:
        print(f"  Auto-detected CDE sheet: '{cde_sheet}'")
    else:
        print(f"WARNING: Could not auto-detect CDE allocation sheet.")
        print(f"  Looked for keywords: {cde_keywords}")
        print(f"  Available sheets: {xl.sheet_names}")
        print()

    total = 0
    if project_sheet:
        total += load_projects(xl, project_sheet)

    if cde_sheet and cde_sheet != project_sheet:
        total += load_cdes(xl, cde_sheet)

    print()
    summary = db.get_nmtc_project_summary()
    print(f"Database now contains:")
    print(f"  NMTC projects:       {summary.get('total_projects', 0):,}")
    total_qlici = summary.get('total_qlici') or 0
    print(f"  Total QLICI:         ${total_qlici/1e9:.1f}B" if total_qlici > 0 else "  Total QLICI: $0")
    print(f"  Unique CDEs:         {summary.get('unique_cdes', 0):,}")
    print(f"  States served:       {summary.get('states_served', 0):,}")
    print()
    print("Done.")


if __name__ == "__main__":
    main()
