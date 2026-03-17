"""
etl/load_cdfi_directory.py — Load certified CDFI data from the CDFI Fund.

The CDFI Fund publishes a list of all certified CDFIs quarterly.
CDFIs are the likely co-investors and leverage lenders for most CD finance deals.
Knowing which CDFIs operate in a given market helps identify deal partners.

Data source:
  CDFI Fund: https://www.cdfifund.gov/research-and-resources/data-resources
  → "CDFI Certification" → Download the certified CDFI list (Excel or CSV)

File format (as of 2024 release):
  One row per certified CDFI. Key columns:
  - Organization Name
  - City
  - State (2-letter abbreviation)
  - Institution Type (Loan Fund / Credit Union / Community Development Bank / etc.)
  - Total Assets (latest reported)
  - Service Areas / Primary Market Description
  - Target Populations

Column names may vary slightly between releases. This script tries multiple
known column names and reports which it found. Use --columns-only to inspect
the file before loading.

Usage:
    python etl/load_cdfi_directory.py --file data/raw/cdfi_certified_list.xlsx
    python etl/load_cdfi_directory.py --file data/raw/cdfi_certified_list.csv
    python etl/load_cdfi_directory.py --file data/raw/cdfi_certified_list.xlsx --states CA TX NY
    python etl/load_cdfi_directory.py --file data/raw/cdfi_certified_list.xlsx --columns-only
"""

import argparse
import sys
import os

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import db

# Known column name candidates for each schema field
# Format: {our_field: [candidate column names in source file]}
COLUMN_CANDIDATES = {
    "cdfi_name":          ["Organization Name", "Org Name", "CDFI Name", "Name", "ORGANIZATION_NAME"],
    "city":               ["City", "CITY", "Hq City", "HQ_CITY"],
    "state":              ["State", "STATE", "Hq State", "HQ_STATE", "State Abbrev"],
    "cdfi_type":          ["Institution Type", "Type", "INSTITUTION_TYPE", "CDFI Type", "Organization Type"],
    "total_assets":       ["Total Assets", "TOTAL_ASSETS", "Assets", "Asset Size"],
    "primary_markets":    ["Service Area", "Primary Market", "Service Areas", "Geographic Service Area", "PRIMARY_MARKET"],
    "target_populations": ["Target Population", "Target Populations", "TARGET_POPULATION", "Targeted Population"],
    "certification_date": ["Certification Date", "CERTIFICATION_DATE", "Cert Date", "Date Certified"],
    "website":            ["Website", "WEBSITE", "Web", "URL"],
}


def find_column(df: pd.DataFrame, candidates: list) -> str | None:
    """Return the first candidate column name found in df, or None."""
    for col in candidates:
        if col in df.columns:
            return col
    return None


def main():
    parser = argparse.ArgumentParser(
        description="Load CDFI Fund certified CDFI directory into the database"
    )
    parser.add_argument(
        "--file",
        required=True,
        help="Path to the CDFI certified list (CSV or Excel). Download from CDFI Fund.",
    )
    parser.add_argument(
        "--states",
        nargs="+",
        metavar="STATE",
        help="2-letter state abbreviations to load (default: all).",
    )
    parser.add_argument(
        "--columns-only",
        action="store_true",
        help="Print column names from the file and exit.",
    )
    parser.add_argument(
        "--sheet",
        default=None,
        help="Excel sheet name (if the file has multiple sheets).",
    )
    args = parser.parse_args()

    if not os.path.exists(args.file):
        print(f"Error: file not found: {args.file}")
        sys.exit(1)

    print(f"CD Command Center — CDFI Directory Load")
    print(f"  File: {args.file}")

    # Load file
    if args.file.endswith((".xlsx", ".xls")):
        df = pd.read_excel(args.file, sheet_name=args.sheet, dtype=str)
    else:
        df = pd.read_csv(args.file, dtype=str)

    print(f"  Rows: {len(df):,}")

    if args.columns_only:
        print("  Columns in file:")
        for col in df.columns:
            print(f"    {col}")
        return

    # Map file columns to our schema
    col_mapping = {}
    for our_field, candidates in COLUMN_CANDIDATES.items():
        found = find_column(df, candidates)
        col_mapping[our_field] = found
        if found:
            print(f"  Mapped '{our_field}' ← '{found}'")
        else:
            print(f"  Warning: '{our_field}' not found (tried: {candidates})")

    name_col = col_mapping.get("cdfi_name")
    state_col = col_mapping.get("state")

    if not name_col:
        print("Error: could not find the organization name column. Use --columns-only to inspect the file.")
        sys.exit(1)

    # Apply state filter
    if args.states and state_col:
        state_upper = [s.upper() for s in args.states]
        df = df[df[state_col].str.upper().fillna("").isin(state_upper)]
        print(f"  After state filter: {len(df):,} rows")

    print()
    db.init_db()

    loaded = 0
    skipped = 0

    for _, row in df.iterrows():
        # Required: organization name
        name = row.get(name_col) if name_col else None
        if not name or str(name).strip() in ("", "nan", "None"):
            skipped += 1
            continue

        state = str(row.get(col_mapping["state"]) or "").strip()[:2].upper() if col_mapping.get("state") else None

        # Parse total_assets — remove $ signs and commas
        total_assets = None
        raw_assets = row.get(col_mapping["total_assets"]) if col_mapping.get("total_assets") else None
        if raw_assets and str(raw_assets).strip() not in ("", "nan", "None"):
            try:
                total_assets = float(str(raw_assets).replace("$", "").replace(",", "").strip())
            except (ValueError, TypeError):
                pass

        record = {
            "cdfi_name":          str(name).strip(),
            "state":              state or None,
            "city":               str(row.get(col_mapping["city"]) or "").strip() or None if col_mapping.get("city") else None,
            "cdfi_type":          str(row.get(col_mapping["cdfi_type"]) or "").strip() or None if col_mapping.get("cdfi_type") else None,
            "total_assets":       total_assets,
            "primary_markets":    str(row.get(col_mapping["primary_markets"]) or "").strip() or None if col_mapping.get("primary_markets") else None,
            "target_populations": str(row.get(col_mapping["target_populations"]) or "").strip() or None if col_mapping.get("target_populations") else None,
            "certification_date": str(row.get(col_mapping["certification_date"]) or "").strip() or None if col_mapping.get("certification_date") else None,
            "website":            str(row.get(col_mapping["website"]) or "").strip() or None if col_mapping.get("website") else None,
        }

        # Remove None values so upsert doesn't overwrite with NULL unnecessarily
        record = {k: v for k, v in record.items() if v is not None}

        try:
            db.upsert_cdfi(record)
            loaded += 1
        except Exception as e:
            skipped += 1
            if skipped <= 3:
                print(f"  Error inserting {name}: {e}")

    print(f"CDFI directory load complete.")
    print(f"  Loaded: {loaded:,}")
    print(f"  Skipped: {skipped:,}")
    print()
    print("CDFIs are now available in the Tools → CDFI Directory tab.")


if __name__ == "__main__":
    main()
