"""
etl/fetch_cdfi_awards.py — Load CDFI Fund award data (Financial Assistance and related programs).

Downloads or reads CDFI Fund award data and loads it into the cdfi_awards table.
This shows which CDFIs are active in each market — useful context when evaluating
deal opportunities: "Who else is lending here?"

DATA SOURCE:
  The CDFI Fund publishes award data through the CDFI Fund Awards Database:
  https://www.cdfifund.gov/research-and-resources/data-resources

  Download the "Awards" dataset as an Excel or CSV file. The file typically has
  one row per award, with columns for awardee name, state, program, year, amount.

  Programs covered:
  - FA  = Financial Assistance (grants and loans to CDFIs for capitalization)
  - TA  = Technical Assistance (small capacity-building grants)
  - BEA = Bank Enterprise Award (CRA incentive for banks working with CDFIs)
  - CMF = Capital Magnet Fund (for affordable housing)
  - NMTC = New Markets Tax Credit allocations (also in nmtc_projects/cde_allocations)
  - Bond = CDFI Bond Guarantee Program

Usage:
    python etl/fetch_cdfi_awards.py --file data/raw/cdfi_awards.xlsx
    python etl/fetch_cdfi_awards.py --file data/raw/cdfi_awards.csv --states CA TX
    python etl/fetch_cdfi_awards.py --file data/raw/cdfi_awards.xlsx --columns-only
    python etl/fetch_cdfi_awards.py --file data/raw/cdfi_awards.xlsx --sheet "FA Awards"
"""

import argparse
import sys
import os

import pandas as pd
import requests

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import db

RAW_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "raw")


def download_cdfi_awards(dest_dir: str = RAW_DIR) -> str:
    """
    Download the CDFI Fund awards dataset from data.gov (CKAN API).
    Returns local file path.
    """
    os.makedirs(dest_dir, exist_ok=True)

    # CDFI Fund awards data is published on data.gov
    # Package slug: cdfi-fund-awards-data  (may vary — try CKAN search)
    PACKAGE_IDS = [
        "cdfi-fund-awards-data",
        "cdfi-awards",
        "community-development-financial-institutions-fund-awards",
    ]

    resource_url = None
    for pkg_id in PACKAGE_IDS:
        try:
            r = requests.get(
                f"https://catalog.data.gov/api/3/action/package_show?id={pkg_id}",
                timeout=20,
            )
            if r.status_code == 200 and r.json().get("success"):
                resources = r.json()["result"]["resources"]
                for res in resources:
                    fmt = (res.get("format") or "").lower()
                    if fmt in ("csv", "xlsx", "xls"):
                        resource_url = res["url"]
                        break
                if resource_url:
                    break
        except requests.RequestException:
            continue

    if not resource_url:
        raise RuntimeError(
            "Could not find CDFI Fund awards data on data.gov.\n"
            "Download manually from: https://www.cdfifund.gov/research-and-resources/data-resources\n"
            "Then run: python etl/fetch_cdfi_awards.py --file <downloaded_file>"
        )

    ext = resource_url.split(".")[-1].split("?")[0].lower()
    dest_path = os.path.join(dest_dir, f"cdfi_awards.{ext}")

    if os.path.exists(dest_path):
        print(f"  Cached file found: {dest_path}")
        return dest_path

    print(f"  Downloading CDFI awards: {resource_url}")
    r = requests.get(resource_url, stream=True, timeout=120)
    r.raise_for_status()
    with open(dest_path, "wb") as f:
        for chunk in r.iter_content(chunk_size=512 * 1024):
            f.write(chunk)
    print(f"  Downloaded: {dest_path}")
    return dest_path


# ---------------------------------------------------------------------------
# Column name candidates — the CDFI Fund award spreadsheet doesn't always use
# consistent column names. We try multiple candidates per field.
# ---------------------------------------------------------------------------

COLUMN_CANDIDATES = {
    "awardee_name":   ["Awardee", "Awardee Name", "Organization Name", "CDFI Name",
                       "Recipient Name", "awardee_name", "org_name"],
    "awardee_state":  ["State", "Awardee State", "State Abbr", "state", "awardee_state"],
    "awardee_city":   ["City", "Awardee City", "city"],
    "award_year":     ["Award Year", "Fiscal Year", "Year", "award_year", "fiscal_year"],
    "program":        ["Program", "Fund Program", "Program Name", "Award Program", "program"],
    "award_amount":   ["Award Amount", "Amount", "Total Award", "Grant Amount",
                       "Loan Amount", "award_amount"],
    "award_type":     ["Award Type", "Type", "Funding Type", "award_type"],
    "cdfi_type":      ["Institution Type", "CDFI Type", "Org Type", "cdfi_type"],
    "purpose":        ["Purpose", "Project Description", "Award Purpose", "Notes", "purpose"],
}


def detect_columns(df: pd.DataFrame) -> dict:
    """Map our canonical field names to actual DataFrame column names."""
    actual_cols = {c.lower().strip(): c for c in df.columns}
    mapping = {}

    for field, candidates in COLUMN_CANDIDATES.items():
        for candidate in candidates:
            key = candidate.lower().strip()
            if key in actual_cols:
                mapping[field] = actual_cols[key]
                break

    return mapping


def _clean_amount(val) -> float | None:
    """Parse award amount — handles '$', ',', 'M', 'K' suffixes."""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    s = str(val).strip().replace("$", "").replace(",", "").upper()
    try:
        if s.endswith("M"):
            return float(s[:-1]) * 1_000_000
        if s.endswith("K"):
            return float(s[:-1]) * 1_000
        return float(s) if s else None
    except (ValueError, TypeError):
        return None


def load_awards(filepath: str, sheet: str = None, states: list = None,
                columns_only: bool = False, verbose: bool = False):
    """Load CDFI award data from an Excel or CSV file into the cdfi_awards table."""

    # Read file
    if filepath.endswith((".xlsx", ".xls")):
        if sheet:
            df = pd.read_excel(filepath, sheet_name=sheet, dtype=str)
        else:
            # Try to find the right sheet
            xf = pd.ExcelFile(filepath)
            print(f"  Sheets: {xf.sheet_names}")
            sheet_name = xf.sheet_names[0]
            df = pd.read_excel(filepath, sheet_name=sheet_name, dtype=str)
    else:
        df = pd.read_csv(filepath, dtype=str, encoding="latin-1")

    df.columns = [str(c).strip() for c in df.columns]

    if columns_only:
        print("Columns in file:")
        for col in df.columns:
            print(f"  {col}")
        return

    col_map = detect_columns(df)
    if verbose:
        print("Column mapping:")
        for field, col in col_map.items():
            print(f"  {field} → {col}")

    required = ["awardee_name", "award_year", "program"]
    missing = [f for f in required if f not in col_map]
    if missing:
        print(f"ERROR: Could not find required columns: {missing}")
        print("Available columns:", list(df.columns))
        print("Use --columns-only to inspect the file, then adjust.")
        sys.exit(1)

    # Filter by state if requested
    if states and "awardee_state" in col_map:
        state_col = col_map["awardee_state"]
        df = df[df[state_col].str.strip().str.upper().isin([s.upper() for s in states])]
        print(f"  Filtered to {len(df):,} rows for states: {states}")

    loaded = 0
    skipped = 0

    for _, row in df.iterrows():
        def get(field):
            col = col_map.get(field)
            if col and col in row:
                val = row[col]
                if pd.isna(val) or str(val).strip().lower() in ("nan", "none", ""):
                    return None
                return str(val).strip()
            return None

        name = get("awardee_name")
        year_raw = get("award_year")
        program  = get("program")

        if not name or not year_raw or not program:
            skipped += 1
            continue

        try:
            year = int(float(year_raw))
        except (ValueError, TypeError):
            skipped += 1
            continue

        record = {
            "awardee_name":  name,
            "award_year":    year,
            "program":       program.upper()[:20],   # normalize program codes
            "awardee_state": get("awardee_state"),
            "awardee_city":  get("awardee_city"),
            "award_amount":  _clean_amount(get("award_amount")),
            "award_type":    get("award_type"),
            "cdfi_type":     get("cdfi_type"),
            "purpose":       get("purpose"),
        }

        db.upsert_cdfi_award(record)
        loaded += 1

        if verbose and loaded % 500 == 0:
            print(f"  {loaded:,} records loaded...")

    print(f"  Loaded: {loaded:,} | Skipped (missing required fields): {skipped:,}")


def main():
    parser = argparse.ArgumentParser(
        description="Load CDFI Fund award data into the CD Command Center database"
    )
    parser.add_argument("--file",         default=None, help="Path to CDFI awards Excel or CSV file")
    parser.add_argument("--auto",         action="store_true",
                        help="Auto-download CDFI awards data from data.gov")
    parser.add_argument("--sheet",        help="Sheet name (Excel only; default = first sheet)")
    parser.add_argument("--states",       nargs="+", metavar="ST",
                        help="Only load awards for these states (e.g. CA TX NY)")
    parser.add_argument("--columns-only", action="store_true",
                        help="Print column names from file and exit — use to inspect before loading")
    parser.add_argument("--verbose",      action="store_true", help="Print progress")
    args = parser.parse_args()

    if not args.file and not args.auto:
        print("Error: provide --file or --auto.")
        sys.exit(1)

    filepath = args.file

    if args.auto:
        try:
            filepath = download_cdfi_awards()
        except Exception as e:
            print(f"Error downloading CDFI awards: {e}")
            sys.exit(1)

    if not os.path.exists(filepath):
        print(f"ERROR: File not found: {filepath}")
        sys.exit(1)

    db.init_db()

    print("CD Command Center — CDFI Awards Loader")
    print(f"  File: {filepath}")
    if args.states:
        print(f"  States: {args.states}")
    print()

    load_awards(
        filepath=filepath,
        sheet=args.sheet,
        states=args.states,
        columns_only=args.columns_only,
        verbose=args.verbose,
    )

    print()
    print("Done.")


if __name__ == "__main__":
    main()
