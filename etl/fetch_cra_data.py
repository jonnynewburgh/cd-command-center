"""
etl/fetch_cra_data.py — Load FFIEC CRA institution and assessment area data.

The Community Reinvestment Act (CRA) requires banks to lend in the communities where
they take deposits. FFIEC publishes an annual register of all CRA-reporting institutions
and the geographic assessment areas they've committed to serve.

Why this matters for CD deal origination:
    Banks with assessment areas in a target geography are motivated to make CRA-qualified
    investments. NMTC equity investors, CDFI borrowers, and community facility lenders
    are disproportionately drawn from banks with local CRA obligations. This data shows
    you WHO is obligated to invest WHERE.

Data source:
    FFIEC CRA flat files: https://www.ffiec.gov/cradownload.htm
    Published annually. Two relevant files per year:
      1. Transmittal sheet — institution name, location, asset size
      2. Assessment area file — which counties/MSAs each institution covers

    Both are pipe-delimited ("|") text files. Download the zip for a given year,
    extract, and pass the files via --transmittal and --assessment-area.

Usage:
    # Download the CRA flat files from https://www.ffiec.gov/cradownload.htm
    # Extract the zip — you'll see files like CRA_Flat_2023_Transmittal.dat
    python etl/fetch_cra_data.py --year 2023 \\
        --transmittal data/raw/CRA_Flat_2023_Transmittal.dat \\
        --assessment-area data/raw/CRA_Flat_2023_Agg_Assessment_Area.dat

    # Only load specific states:
    python etl/fetch_cra_data.py --year 2023 \\
        --transmittal data/raw/CRA_Flat_2023_Transmittal.dat \\
        --assessment-area data/raw/CRA_Flat_2023_Agg_Assessment_Area.dat \\
        --states CA TX NY

    # Inspect column names in a file:
    python etl/fetch_cra_data.py --year 2023 \\
        --transmittal data/raw/CRA_Flat_2023_Transmittal.dat \\
        --columns-only

File format notes:
    FFIEC flat files use "|" as delimiter. Column names are in the first row.
    The exact column names vary slightly across years. This script tries common
    variants. Use --columns-only if the script can't find expected columns.
"""

import argparse
import os
import sys

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import db

# ---------------------------------------------------------------------------
# Column name resolution
# Maps our internal name → list of candidate column names in FFIEC files
# ---------------------------------------------------------------------------

TRANSMITTAL_COLS = {
    "respondent_id":        ["RESPONDENT_ID", "RespondentID", "Respondent ID", "respondent_id"],
    "institution_name":     ["RESPONDENT_NAME_PANEL", "RESPONDENT_NAME", "Institution Name",
                             "InstitutionName", "respondent_name"],
    "city":                 ["RESPONDENT_CITY", "City", "city"],
    "state":                ["RESPONDENT_STATE", "State", "state"],
    "zip_code":             ["RESPONDENT_ZIP_CODE", "ZIP", "Zip", "zip_code"],
    "asset_size_indicator": ["ASSET_SIZE_IND", "AssetSizeIndicator", "Asset Size Ind",
                             "asset_size_ind"],
}

ASSESSMENT_COLS = {
    "respondent_id":        ["RESPONDENT_ID", "RespondentID", "Respondent ID"],
    "institution_name":     ["RESPONDENT_NAME_PANEL", "RESPONDENT_NAME", "Institution Name"],
    "state":                ["MSA_MD_STATE_CODE", "STATE_CODE", "State", "state"],
    "assessment_area_name": ["ASSESSMENT_AREA_NAME", "AssessmentAreaName", "Assessment Area Name",
                             "assessment_area_name"],
    "area_type":            ["ASSESSMENT_AREA_TYPE", "AreaType", "Assessment Area Type"],
    "county_fips":          ["COUNTY_CODE", "CountyCode", "County FIPS"],
    "msa_code":             ["MSA_MD_CODE", "MSA_CODE", "MsaCode"],
}


def find_col(df: pd.DataFrame, candidates: list[str]):
    """Return the first column name from candidates that exists in df, or None."""
    cols_lower = {c.lower().strip(): c for c in df.columns}
    for c in candidates:
        if c in df.columns:
            return c
        if c.lower().strip() in cols_lower:
            return cols_lower[c.lower().strip()]
    return None


def read_flat_file(filepath: str) -> pd.DataFrame:
    """Read an FFIEC CRA pipe-delimited flat file."""
    # Try pipe-delimited first (standard FFIEC format), fall back to comma
    try:
        df = pd.read_csv(filepath, sep="|", dtype=str, encoding="latin-1", low_memory=False)
        if len(df.columns) > 3:
            return df
    except Exception:
        pass
    return pd.read_csv(filepath, dtype=str, encoding="latin-1", low_memory=False)


def load_transmittal(filepath: str, year: int, states: list[str], columns_only: bool) -> list[dict]:
    """Parse the FFIEC transmittal (institution register) file."""
    print(f"  Reading transmittal: {filepath}")
    df = read_flat_file(filepath)
    print(f"  Rows: {len(df):,}  Columns: {len(df.columns)}")

    if columns_only:
        print("  Columns:")
        for col in df.columns:
            print(f"    {col}")
        return []

    col = {k: find_col(df, v) for k, v in TRANSMITTAL_COLS.items()}

    missing = [k for k, v in col.items() if v is None and k in ("respondent_id", "institution_name")]
    if missing:
        raise ValueError(
            f"Could not find required columns: {missing}. "
            f"Available: {list(df.columns)}. Use --columns-only to inspect."
        )

    if states and col["state"]:
        df = df[df[col["state"]].isin(states)]

    rows = []
    for _, row in df.iterrows():
        def get(key):
            c = col.get(key)
            return str(row[c]).strip() if c and pd.notna(row[c]) else None

        rows.append({
            "respondent_id":        get("respondent_id"),
            "institution_name":     get("institution_name"),
            "city":                 get("city"),
            "state":                get("state"),
            "zip_code":             get("zip_code"),
            "asset_size_indicator": get("asset_size_indicator"),
            "report_year":          year,
        })

    return [r for r in rows if r["respondent_id"]]


def load_assessment_areas(filepath: str, year: int, states: list[str], columns_only: bool) -> list[dict]:
    """Parse the FFIEC assessment area file."""
    print(f"  Reading assessment areas: {filepath}")
    df = read_flat_file(filepath)
    print(f"  Rows: {len(df):,}  Columns: {len(df.columns)}")

    if columns_only:
        print("  Columns:")
        for col in df.columns:
            print(f"    {col}")
        return []

    col = {k: find_col(df, v) for k, v in ASSESSMENT_COLS.items()}

    missing = [k for k, v in col.items() if v is None and k in ("respondent_id",)]
    if missing:
        raise ValueError(
            f"Could not find required columns: {missing}. "
            f"Available: {list(df.columns)}. Use --columns-only to inspect."
        )

    if states and col["state"]:
        df = df[df[col["state"]].isin(states)]

    rows = []
    for _, row in df.iterrows():
        def get(key):
            c = col.get(key)
            return str(row[c]).strip() if c and pd.notna(row[c]) else None

        rows.append({
            "respondent_id":        get("respondent_id"),
            "institution_name":     get("institution_name"),
            "report_year":          year,
            "state":                get("state"),
            "assessment_area_name": get("assessment_area_name"),
            "area_type":            get("area_type"),
            "county_fips":          get("county_fips"),
            "msa_code":             get("msa_code"),
        })

    return [r for r in rows if r["respondent_id"]]


def main():
    parser = argparse.ArgumentParser(
        description="Load FFIEC CRA institution and assessment area data"
    )
    parser.add_argument(
        "--year",
        type=int,
        required=True,
        help="CRA report year (e.g. 2023). Download files from https://www.ffiec.gov/cradownload.htm",
    )
    parser.add_argument(
        "--transmittal",
        default=None,
        help="Path to the FFIEC CRA transmittal (institution register) flat file.",
    )
    parser.add_argument(
        "--assessment-area",
        default=None,
        dest="assessment_area",
        help="Path to the FFIEC CRA assessment area flat file.",
    )
    parser.add_argument(
        "--states",
        nargs="+",
        metavar="STATE",
        help="Filter to specific state abbreviations (default: all).",
    )
    parser.add_argument(
        "--columns-only",
        action="store_true",
        help="Print column names from the input files and exit.",
    )
    args = parser.parse_args()

    if not args.transmittal and not args.assessment_area:
        print(
            "Error: provide at least one of --transmittal or --assessment-area.\n"
            "Download CRA flat files from: https://www.ffiec.gov/cradownload.htm"
        )
        sys.exit(1)

    print("CD Command Center — FFIEC CRA Load")
    print(f"  Year: {args.year}")
    if args.states:
        print(f"  States: {', '.join(args.states)}")
    print()

    db.init_db()
    run_id = db.log_load_start("cra_data")
    total_loaded = 0

    try:
        if args.transmittal:
            if not os.path.exists(args.transmittal):
                print(f"Error: file not found: {args.transmittal}")
                sys.exit(1)
            rows = load_transmittal(args.transmittal, args.year, args.states or [], args.columns_only)
            if not args.columns_only:
                n = db.upsert_rows("cra_institutions", rows, unique_cols=["respondent_id", "report_year"])
                total_loaded += n
                print(f"  Loaded {n:,} institution records.")

        if args.assessment_area:
            if not os.path.exists(args.assessment_area):
                print(f"Error: file not found: {args.assessment_area}")
                sys.exit(1)
            rows = load_assessment_areas(args.assessment_area, args.year, args.states or [], args.columns_only)
            if not args.columns_only:
                n = db.upsert_rows(
                    "cra_assessment_areas", rows,
                    unique_cols=["respondent_id", "report_year", "assessment_area_name"]
                )
                total_loaded += n
                print(f"  Loaded {n:,} assessment area records.")

    except Exception as e:
        db.log_load_finish(run_id, rows_loaded=total_loaded, error=str(e))
        raise

    db.log_load_finish(run_id, rows_loaded=total_loaded)
    print()
    print(f"Done. Total rows upserted: {total_loaded:,}")


if __name__ == "__main__":
    main()
