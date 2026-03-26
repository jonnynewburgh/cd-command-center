"""
etl/fetch_sba_loans.py — Load SBA 7(a) and 504 loan data into the sba_loans table.

SBA loan data shows existing small-business lending activity by geography. In CD
deal origination, this data is used to:
    - Identify credit deserts: areas with high need but low SBA lending activity
    - Understand prior SBA penetration in a target market
    - Show job support and capital deployment context in impact narratives
    - Find gaps where CDFI small-business lending is underserved

Data source:
    SBA publishes annual loan-level data files at:
        https://data.sba.gov/dataset/sba-7-a-504-foia

    Files are large CSVs (7(a) is ~300MB per year; 504 is smaller).
    Download the file for the year(s) you want, then run this script.

    7(a) file columns include: LoanNbr, BorrName, BorrCity, BorrState, BorrZip,
    BorrCounty, NaicsCode, ApprovalDate, GrossApproval, SBAGuaranteedApproval,
    FranchiseCode, ProjectCounty, ProjectState, LenderName, LenderState, JobsSupported

    504 file columns are similar but structured differently (FDC vs CDC portions).

Usage:
    python etl/fetch_sba_loans.py --file data/raw/foia-7afy2024.csv --program 7a
    python etl/fetch_sba_loans.py --file data/raw/foia-504fy2024.csv --program 504
    python etl/fetch_sba_loans.py --file data/raw/foia-7afy2024.csv --program 7a --states CA TX NY
    python etl/fetch_sba_loans.py --file data/raw/foia-7afy2024.csv --program 7a --columns-only

    # Filter to a single approval year within a multi-year file:
    python etl/fetch_sba_loans.py --file data/raw/foia-7afy2024.csv --program 7a --year 2023

Download:
    https://data.sba.gov/dataset/sba-7-a-504-foia
    Look for "7(a) Loan Data Report" or "504 Loan Data Report" as CSV.
"""

import argparse
import os
import sys

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import db

# ---------------------------------------------------------------------------
# Column mapping — SBA file headers vary slightly between fiscal years.
# Maps our schema column → list of candidate SBA column names.
# ---------------------------------------------------------------------------

SEVEN_A_COLS = {
    "loan_number":             ["LoanNbr", "Loan Number", "loan_number"],
    "borrower_name":           ["BorrName", "BorrowerName", "Borrower Name"],
    "borrower_city":           ["BorrCity", "BorrowerCity", "Borrower City"],
    "borrower_state":          ["BorrState", "BorrowerState", "Borrower State"],
    "borrower_zip":            ["BorrZip", "BorrowerZip", "Borrower Zip"],
    "borrower_county":         ["BorrCounty", "BorrowerCounty", "Borrower County",
                                "ProjectCounty", "Project County"],
    "naics_code":              ["NaicsCode", "NAICS", "naics_code"],
    "business_type":           ["BusinessType", "Business Type", "business_type"],
    "approval_date":           ["ApprovalDate", "Approval Date", "approval_date"],
    "gross_approval":          ["GrossApproval", "Gross Approval", "gross_approval"],
    "sba_guaranteed_portion":  ["SBAGuaranteedApproval", "SBA Guaranteed", "sba_guaranteed"],
    "lender_name":             ["BankName", "LenderName", "Lender Name", "bank_name"],
    "lender_state":            ["BankState", "LenderState", "Lender State"],
    "jobs_supported":          ["JobsSupported", "Jobs Supported", "jobs_supported"],
}

FIVE_O_FOUR_COLS = {
    "loan_number":             ["CDC_Loan_Number", "LoanNbr", "Loan Number"],
    "borrower_name":           ["Borrower_Name", "BorrName"],
    "borrower_city":           ["Borrower_City", "BorrCity"],
    "borrower_state":          ["Borrower_State", "BorrState"],
    "borrower_zip":            ["Borrower_Zip", "BorrZip"],
    "borrower_county":         ["Borrower_County", "BorrCounty", "Project_County"],
    "naics_code":              ["NAICS_Code", "NaicsCode"],
    "business_type":           ["Business_Type"],
    "approval_date":           ["Approval_Date", "ApprovalDate"],
    "gross_approval":          ["Gross_Approval", "GrossApproval", "Total_Project_Amount"],
    "sba_guaranteed_portion":  ["CDC_Gross_Debenture_Amount", "SBA_Guaranteed"],
    "lender_name":             ["CDC_Name", "BankName", "Lender_Name"],
    "lender_state":            ["CDC_State", "BankState"],
    "jobs_supported":          ["JobsSupported", "Jobs_Supported"],
}


def find_col(df: pd.DataFrame, candidates: list[str]):
    cols_lower = {c.lower().strip(): c for c in df.columns}
    for c in candidates:
        if c in df.columns:
            return c
        if c.lower().strip() in cols_lower:
            return cols_lower[c.lower().strip()]
    return None


def to_float(val):
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    try:
        return float(str(val).replace(",", "").replace("$", "").strip())
    except (ValueError, TypeError):
        return None


def to_int(val):
    f = to_float(val)
    return int(f) if f is not None else None


def parse_approval_year(date_str):
    if not date_str:
        return None
    try:
        return int(str(date_str).strip()[:4])
    except (ValueError, TypeError):
        return None


def load_file(filepath: str, program: str, states: list[str], year_filter: int,
              columns_only: bool, chunksize: int = 50_000) -> int:
    """
    Read the SBA CSV in chunks (files can be 300+ MB) and upsert into sba_loans.
    Returns total rows loaded.
    """
    col_map_template = SEVEN_A_COLS if program == "7a" else FIVE_O_FOUR_COLS

    print(f"  Reading ({program}): {filepath}")
    print(f"  Processing in chunks of {chunksize:,}...")

    total = 0
    chunk_num = 0

    for chunk in pd.read_csv(
        filepath,
        dtype=str,
        encoding="latin-1",
        low_memory=False,
        chunksize=chunksize,
    ):
        chunk_num += 1

        if chunk_num == 1:
            if columns_only:
                print("  Columns in file:")
                for col in chunk.columns:
                    print(f"    {col}")
                return 0

            # Resolve column mapping on first chunk
            col = {k: find_col(chunk, v) for k, v in col_map_template.items()}
            state_col = col.get("borrower_state")
            date_col  = col.get("approval_date")

            missing_required = [k for k in ("loan_number",) if col.get(k) is None]
            if missing_required:
                raise ValueError(
                    f"Could not find required columns: {missing_required}. "
                    f"Available: {list(chunk.columns)}. Use --columns-only."
                )

        # State filter
        if states and state_col and state_col in chunk.columns:
            chunk = chunk[chunk[state_col].isin(states)]

        if chunk.empty:
            continue

        # Year filter
        if year_filter and date_col and date_col in chunk.columns:
            chunk = chunk[chunk[date_col].str[:4] == str(year_filter)]

        if chunk.empty:
            continue

        def get(row, key):
            c = col.get(key)
            if not c or c not in chunk.columns:
                return None
            val = row.get(c)
            return str(val).strip() if val and pd.notna(val) else None

        rows = []
        for _, row in chunk.iterrows():
            approval_date = get(row, "approval_date")
            rows.append({
                "loan_number":            get(row, "loan_number"),
                "program":                program,
                "borrower_name":          get(row, "borrower_name"),
                "borrower_city":          get(row, "borrower_city"),
                "borrower_state":         get(row, "borrower_state"),
                "borrower_zip":           get(row, "borrower_zip"),
                "borrower_county":        get(row, "borrower_county"),
                "naics_code":             get(row, "naics_code"),
                "business_type":          get(row, "business_type"),
                "approval_date":          approval_date,
                "approval_year":          parse_approval_year(approval_date),
                "gross_approval":         to_float(get(row, "gross_approval")),
                "sba_guaranteed_portion": to_float(get(row, "sba_guaranteed_portion")),
                "lender_name":            get(row, "lender_name"),
                "lender_state":           get(row, "lender_state"),
                "jobs_supported":         to_int(get(row, "jobs_supported")),
            })

        rows = [r for r in rows if r["loan_number"]]
        if rows:
            n = db.upsert_rows("sba_loans", rows, unique_cols=["loan_number"])
            total += n

        if chunk_num % 10 == 0:
            print(f"    Processed chunk {chunk_num} ({total:,} rows so far)...")

    return total


def main():
    parser = argparse.ArgumentParser(
        description="Load SBA 7(a) and 504 loan data into the sba_loans table"
    )
    parser.add_argument(
        "--file",
        required=True,
        help="Path to the SBA loan CSV file. Download from https://data.sba.gov/dataset/sba-7-a-504-foia",
    )
    parser.add_argument(
        "--program",
        choices=["7a", "504"],
        required=True,
        help="Which SBA program the file contains: '7a' or '504'",
    )
    parser.add_argument(
        "--states",
        nargs="+",
        metavar="STATE",
        help="Filter to specific state abbreviations (default: all).",
    )
    parser.add_argument(
        "--year",
        type=int,
        default=None,
        help="Filter to a specific approval year (optional; for multi-year files).",
    )
    parser.add_argument(
        "--columns-only",
        action="store_true",
        help="Print column names from the file and exit.",
    )
    args = parser.parse_args()

    if not os.path.exists(args.file):
        print(f"Error: file not found: {args.file}")
        sys.exit(1)

    print("CD Command Center — SBA Loan Load")
    print(f"  Program: SBA {args.program}")
    if args.states:
        print(f"  States: {', '.join(args.states)}")
    if args.year:
        print(f"  Year filter: {args.year}")
    print()

    db.init_db()
    run_id = db.log_load_start("sba_loans")
    total_loaded = 0

    try:
        total_loaded = load_file(
            filepath=args.file,
            program=args.program,
            states=args.states or [],
            year_filter=args.year,
            columns_only=args.columns_only,
        )
    except Exception as e:
        db.log_load_finish(run_id, rows_loaded=total_loaded, error=str(e))
        raise

    if args.columns_only:
        return

    db.log_load_finish(run_id, rows_loaded=total_loaded)
    print()
    print(f"Done. Total rows upserted: {total_loaded:,}")


if __name__ == "__main__":
    main()
