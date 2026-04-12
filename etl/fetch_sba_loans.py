"""
etl/fetch_sba_loans.py — Load SBA 7(a) and 504 loan data into the sba_loans table.

SBA loan data shows existing small-business lending activity by geography. In CD
deal origination, this data is used to:
    - Identify credit deserts: areas with high need but low SBA lending activity
    - Understand prior SBA penetration in a target market
    - Show job support and capital deployment context in impact narratives
    - Find gaps where CDFI small-business lending is underserved

Data source:
    SBA publishes loan-level data files at:
        https://data.sba.gov/dataset/7-a-504-foia

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
import requests

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import db

RAW_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "raw")


def download_sba_file(program: str, fiscal_year: int, dest_dir: str = RAW_DIR) -> str:
    """
    Download an SBA FOIA loan file from data.sba.gov using the CKAN API.
    Returns the local file path. Uses a cached copy if already downloaded.
    """
    api_url = "https://data.sba.gov/api/3/action/package_show?id=7-a-504-foia"
    print(f"  Querying SBA data portal for {program.upper()} FY{fiscal_year} file...")
    r = requests.get(api_url, timeout=30)
    r.raise_for_status()
    resources = r.json()["result"]["resources"]

    program_terms = ["7(a)", "7a", "7A"] if program == "7a" else ["504"]
    year_terms = [str(fiscal_year), f"FY{fiscal_year}", f"fy{fiscal_year}",
                  f"FY {fiscal_year}", f"fy {fiscal_year}"]

    # Only consider CSV resources (skip data dictionaries, XLSX, etc.)
    csv_resources = [r for r in resources if (r.get("format") or "").upper() == "CSV"]

    # Try exact year match first (e.g. "FY2024" in name)
    match = None
    for res in csv_resources:
        name = (res.get("name") or "") + " " + (res.get("description") or "")
        if any(pt in name for pt in program_terms) and any(yt in name for yt in year_terms):
            match = res
            break

    # SBA now publishes era-split files (FY2020-Present, FY2010-FY2019, etc.)
    # For recent fiscal years, prefer the "Present" file over older era files.
    if not match:
        present_res = None
        fallback_res = None
        for res in csv_resources:
            name = (res.get("name") or "") + " " + (res.get("description") or "")
            if any(pt in name for pt in program_terms):
                if "present" in name.lower():
                    present_res = res
                elif fallback_res is None:
                    fallback_res = res
        match = present_res or fallback_res
        if match:
            print(f"  No exact FY{fiscal_year} file; using era file: {match.get('name')}")

    if not match:
        names = [r.get("name") for r in resources]
        raise RuntimeError(
            f"Could not find a {program.upper()} CSV resource on data.sba.gov.\n"
            f"Available resources: {names}"
        )

    url = match["url"]
    filename = url.split("/")[-1].split("?")[0] or f"foia-{program}fy{fiscal_year}.csv"
    dest_path = os.path.join(dest_dir, filename)

    if os.path.exists(dest_path):
        print(f"  Cached file found: {dest_path}")
        return dest_path

    print(f"  Downloading: {url}")
    print(f"  -> {dest_path}")
    print(f"  (SBA files can be 300+ MB — this may take several minutes)")
    os.makedirs(dest_dir, exist_ok=True)

    with requests.get(url, stream=True, timeout=600) as resp:
        resp.raise_for_status()
        total = int(resp.headers.get("Content-Length", 0))
        downloaded = 0
        with open(dest_path, "wb") as f:
            for chunk in resp.iter_content(chunk_size=1024 * 1024):
                f.write(chunk)
                downloaded += len(chunk)
                if total:
                    print(f"\r  {downloaded / 1e6:.0f} MB / {total / 1e6:.0f} MB "
                          f"({downloaded / total * 100:.0f}%)", end="", flush=True)
        if total:
            print()

    print(f"  Download complete: {dest_path}")
    return dest_path

# ---------------------------------------------------------------------------
# Column mapping — SBA file headers vary slightly between fiscal years.
# Maps our schema column → list of candidate SBA column names.
# ---------------------------------------------------------------------------

SEVEN_A_COLS = {
    # Note: l2locid in the FY2020-Present file is a lender location ID (not unique per loan).
    # Old files had LoanNbr which was unique. New era files have no single unique loan ID.
    "loan_number":             ["LoanNbr", "Loan Number", "loan_number"],
    "borrower_name":           ["BorrName", "BorrowerName", "Borrower Name", "borrname"],
    "borrower_city":           ["BorrCity", "BorrowerCity", "Borrower City", "borrcity"],
    "borrower_state":          ["BorrState", "BorrowerState", "Borrower State", "borrstate"],
    "borrower_zip":            ["BorrZip", "BorrowerZip", "Borrower Zip", "borrzip"],
    "borrower_county":         ["BorrCounty", "BorrowerCounty", "Borrower County",
                                "ProjectCounty", "Project County", "projectcounty"],
    "naics_code":              ["NaicsCode", "NAICS", "naics_code", "naicscode"],
    "business_type":           ["BusinessType", "Business Type", "business_type", "businesstype"],
    "approval_date":           ["ApprovalDate", "Approval Date", "approval_date", "approvaldate"],
    "gross_approval":          ["GrossApproval", "Gross Approval", "gross_approval", "grossapproval"],
    "sba_guaranteed_portion":  ["SBAGuaranteedApproval", "SBA Guaranteed", "sba_guaranteed",
                                "sbaguaranteedapproval"],
    "lender_name":             ["BankName", "LenderName", "Lender Name", "bank_name", "bankname"],
    "lender_state":            ["BankState", "LenderState", "Lender State", "bankstate"],
    "jobs_supported":          ["JobsSupported", "Jobs Supported", "jobs_supported", "jobssupported"],
}

FIVE_O_FOUR_COLS = {
    "loan_number":             ["CDC_Loan_Number", "LoanNbr", "Loan Number"],
    "borrower_name":           ["Borrower_Name", "BorrName", "borrname"],
    "borrower_city":           ["Borrower_City", "BorrCity", "borrcity"],
    "borrower_state":          ["Borrower_State", "BorrState", "borrstate"],
    "borrower_zip":            ["Borrower_Zip", "BorrZip", "borrzip"],
    "borrower_county":         ["Borrower_County", "BorrCounty", "Project_County", "projectcounty"],
    "naics_code":              ["NAICS_Code", "NaicsCode", "naicscode"],
    "business_type":           ["Business_Type", "businesstype"],
    "approval_date":           ["Approval_Date", "ApprovalDate", "approvaldate"],
    "gross_approval":          ["Gross_Approval", "GrossApproval", "Total_Project_Amount",
                                "grossapproval"],
    "sba_guaranteed_portion":  ["CDC_Gross_Debenture_Amount", "SBA_Guaranteed",
                                "sbaguaranteedapproval"],
    "lender_name":             ["CDC_Name", "BankName", "Lender_Name", "cdc_name"],
    "lender_state":            ["CDC_State", "BankState", "cdc_state"],
    "jobs_supported":          ["JobsSupported", "Jobs_Supported", "jobssupported"],
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
        s = str(date_str).strip()
        # ISO format: YYYY-MM-DD -> first 4 chars
        if len(s) >= 4 and s[4:5] == "-":
            return int(s[:4])
        # M/D/YYYY or MM/DD/YYYY -> last 4 chars after last slash
        if "/" in s:
            return int(s.rsplit("/", 1)[-1])
        return int(s[:4])
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

            # loan_number is preferred but not strictly required —
            # the FY2020-Present era file uses 'l2locid' which maps to it above.
            # Warn if missing but continue (upsert will use borrower+date as key).
            missing_required = [k for k in ("borrower_name", "approval_date", "borrower_state")
                                 if col.get(k) is None]
            if missing_required:
                raise ValueError(
                    f"Could not find required columns: {missing_required}. "
                    f"Available: {list(chunk.columns)}. Use --columns-only."
                )
            if col.get("loan_number") is None:
                print("  Note: no loan_number column found; upsert key falls back to "
                      "borrower+date+state.")

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

        # Filter out rows with no identifying info at all
        rows = [r for r in rows if r["borrower_name"] or r["loan_number"]]
        if rows:
            # Use loan_number as unique key if available (old annual files have LoanNbr).
            # FY2020-Present era files have no per-loan unique ID, so use a composite
            # key that's practically unique: borrower + date + amount + state.
            has_loan_num = any(r["loan_number"] for r in rows)
            unique_cols = (["loan_number"] if has_loan_num
                           else ["borrower_name", "approval_date",
                                 "gross_approval", "borrower_state"])
            n = db.upsert_rows("sba_loans", rows, unique_cols=unique_cols)
            total += n

        if chunk_num % 10 == 0:
            print(f"    Processed chunk {chunk_num} ({total:,} rows so far)...")

    return total


def main():
    import datetime
    parser = argparse.ArgumentParser(
        description="Load SBA 7(a) and 504 loan data into the sba_loans table"
    )
    parser.add_argument(
        "--file",
        default=None,
        help="Path to the SBA loan CSV file. Download from https://data.sba.gov/dataset/sba-7-a-504-foia",
    )
    parser.add_argument(
        "--auto",
        action="store_true",
        help="Auto-download from data.sba.gov via CKAN API. Requires --program.",
    )
    parser.add_argument(
        "--fiscal-year",
        type=int,
        default=None,
        dest="fiscal_year",
        help="Fiscal year to download when using --auto (default: current year - 1).",
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
        help="Filter to a specific approval year within the file (optional).",
    )
    parser.add_argument(
        "--columns-only",
        action="store_true",
        help="Print column names from the file and exit.",
    )
    args = parser.parse_args()

    if not args.file and not args.auto:
        print("Error: provide --file or --auto.")
        sys.exit(1)

    filepath = args.file

    if args.auto:
        fiscal_year = args.fiscal_year or (datetime.datetime.now().year - 1)
        try:
            filepath = download_sba_file(program=args.program, fiscal_year=fiscal_year)
        except Exception as e:
            print(f"Error downloading SBA file: {e}")
            sys.exit(1)

    if not os.path.exists(filepath):
        print(f"Error: file not found: {filepath}")
        sys.exit(1)

    print("CD Command Center — SBA Loan Load")
    print(f"  Program: SBA {args.program}")
    print(f"  File: {filepath}")
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
            filepath=filepath,
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
