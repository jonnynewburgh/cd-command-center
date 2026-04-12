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
    Files are fixed-width (no delimiter). Layout is consistent 1996-present:
      [0:10]   respondent_id
      [10:11]  agency_code
      [11:15]  report_year
      [15:45]  respondent_name (30 chars)
      [45:85]  street_address (40 chars)
      [85:110] city (25 chars)
      [110:112] state (2 chars)
      [112:122] zip_code (10 chars)
      [122:132] tax_id (10 chars)
      [132:152] total_assets (20 chars -- only in 2014+ files)

    Batch-load all years at once using --dir:
        python etl/fetch_cra_data.py --dir data/raw/cra
"""

import argparse
import io
import os
import re
import sys
import zipfile

import pandas as pd
import requests

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import db

RAW_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "raw")


def download_cra_files(year: int, dest_dir: str = RAW_DIR) -> tuple[str, str]:
    """
    Download FFIEC CRA flat files for a given year.
    Returns (transmittal_path, assessment_area_path).
    FFIEC publishes annual ZIP files at a predictable URL pattern.
    """
    os.makedirs(dest_dir, exist_ok=True)

    # FFIEC URL patterns (2-digit year is the current format as of 2024)
    yy = str(year)[-2:]
    zip_url_candidates = [
        f"https://www.ffiec.gov/cra/xls/{yy}_CRA_Flat.zip",
        f"https://www.ffiec.gov/cra/xls/{year}_CRA_Flat.zip",
        f"https://www.ffiec.gov/cra/xls/CRA_{year}_Flat.zip",
        f"https://www.ffiec.gov/cra/{year}_CRA_Flat.zip",
    ]

    # FFIEC blocks requests without a browser User-Agent
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Referer": "https://www.ffiec.gov/cradownload.htm",
        "Accept": "application/zip,application/octet-stream,*/*",
    }

    zip_path = os.path.join(dest_dir, f"CRA_{year}_Flat.zip")

    if not os.path.exists(zip_path):
        downloaded = False
        for url in zip_url_candidates:
            print(f"  Trying: {url}")
            try:
                r = requests.get(url, stream=True, timeout=120, headers=headers)
                ct = r.headers.get("Content-Type", "").lower()
                if r.status_code == 200 and ("zip" in ct or "octet" in ct):
                    with open(zip_path, "wb") as f:
                        for chunk in r.iter_content(chunk_size=1024 * 512):
                            f.write(chunk)
                    print(f"  Downloaded: {zip_path}")
                    downloaded = True
                    break
                elif r.status_code == 403:
                    print(f"  403 Forbidden (FFIEC blocks automated downloads)")
                    break  # All URLs will fail the same way — don't keep trying
            except requests.RequestException:
                continue

        if not downloaded:
            raise RuntimeError(
                f"Could not download FFIEC CRA flat files for {year}.\n"
                f"FFIEC.gov blocks automated downloads. Download manually:\n"
                f"  1. Go to https://www.ffiec.gov/cradownload.htm\n"
                f"  2. Download the '{year} CRA Flat File' zip\n"
                f"  3. Extract and run:\n"
                f"     python etl/fetch_cra_data.py --year {year} "
                f"--transmittal data/raw/CRA_Flat_{year}_Transmittal.dat "
                f"--assessment-area data/raw/CRA_Flat_{year}_Agg_Assessment_Area.dat"
            )
    else:
        print(f"  Cached ZIP found: {zip_path}")

    # Extract the zip and find the transmittal and assessment area files
    transmittal_path = None
    aa_path = None

    with zipfile.ZipFile(zip_path) as zf:
        names = zf.namelist()
        print(f"  ZIP contents: {names}")
        for name in names:
            lower = name.lower()
            if "transmit" in lower and transmittal_path is None:
                dest = os.path.join(dest_dir, os.path.basename(name))
                with zf.open(name) as src, open(dest, "wb") as dst:
                    dst.write(src.read())
                transmittal_path = dest
                print(f"  Extracted transmittal: {dest}")
            elif ("assessment" in lower or "assess" in lower or "aa" in lower) and aa_path is None:
                dest = os.path.join(dest_dir, os.path.basename(name))
                with zf.open(name) as src, open(dest, "wb") as dst:
                    dst.write(src.read())
                aa_path = dest
                print(f"  Extracted assessment areas: {dest}")

    if not transmittal_path:
        raise RuntimeError(f"Could not find transmittal file in ZIP. Contents: {names}")
    if not aa_path:
        raise RuntimeError(f"Could not find assessment area file in ZIP. Contents: {names}")

    return transmittal_path, aa_path

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
    """Read an FFIEC CRA fixed-width transmittal file (1996-present).

    Layout (0-indexed):
      [0:10]   respondent_id
      [10:11]  agency_code
      [11:15]  report_year
      [15:45]  respondent_name
      [45:85]  street_address
      [85:110] city
      [110:112] state
      [112:122] zip_code
      [122:132] tax_id
      [132:152] total_assets  (only in newer files; absent in 1996-2013 132-char lines)
    """
    colspecs = [
        (0,  10),   # respondent_id
        (10, 11),   # agency_code
        (11, 15),   # report_year
        (15, 45),   # respondent_name
        (45, 85),   # street_address
        (85, 110),  # city
        (110, 112), # state
        (112, 122), # zip_code
        (122, 132), # tax_id
        (132, 152), # total_assets
    ]
    names = [
        "respondent_id",
        "agency_code",
        "report_year",
        "respondent_name",
        "street_address",
        "city",
        "state",
        "zip_code",
        "tax_id",
        "total_assets",
    ]
    df = pd.read_fwf(
        filepath,
        colspecs=colspecs,
        names=names,
        dtype=str,
        encoding="latin-1",
    )
    # Strip whitespace from all string columns
    for col in df.columns:
        df[col] = df[col].apply(lambda x: x.strip() if isinstance(x, str) else x)
    # Drop completely empty rows (blank lines at end of file)
    df = df.dropna(subset=["respondent_id"])
    return df


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


def infer_year_from_filename(filename: str) -> int | None:
    """
    Infer the CRA report year from a transmittal filename.

    Handles:
      96exp_trans.dat  -> 1996
      00exp_trans.dat  -> 2000
      13exp_trans.dat  -> 2013
      CRA2014_Transmittal.dat -> 2014
      CRA2023_Transmittal.dat -> 2023
    """
    base = os.path.basename(filename).lower()

    # CRA####_Transmittal.dat style (2014+)
    m = re.search(r"cra(\d{4})", base)
    if m:
        return int(m.group(1))

    # ##exp_trans.dat style (1996-2013, 2-digit year)
    m = re.match(r"^(\d{2})exp", base)
    if m:
        yy = int(m.group(1))
        # 96-99 -> 1996-1999; 00-13 -> 2000-2013
        return (1900 + yy) if yy >= 96 else (2000 + yy)

    return None


def find_transmittal_files(directory: str) -> list[tuple[int, str]]:
    """
    Return sorted list of (year, filepath) for all transmittal files in directory.
    """
    results = []
    for fname in os.listdir(directory):
        if not fname.lower().endswith(".dat"):
            continue
        # Identify transmittal files (not assessment area files)
        fl = fname.lower()
        if "trans" not in fl and "transmittal" not in fl:
            continue
        year = infer_year_from_filename(fname)
        if year:
            results.append((year, os.path.join(directory, fname)))
    return sorted(results)


def main():  # noqa: C901
    parser = argparse.ArgumentParser(
        description="Load FFIEC CRA institution and assessment area data"
    )
    parser.add_argument(
        "--year",
        type=int,
        default=None,
        help="CRA report year (e.g. 2023). Required with --auto or --transmittal.",
    )
    parser.add_argument(
        "--dir",
        default=None,
        help="Directory containing CRA transmittal .dat files for all years (batch load).",
    )
    parser.add_argument(
        "--auto",
        action="store_true",
        help="Auto-download CRA flat files from ffiec.gov for the given --year.",
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

    # --- Batch mode: load an entire directory ---
    if args.dir:
        if not os.path.isdir(args.dir):
            print(f"Error: directory not found: {args.dir}")
            sys.exit(1)

        files = find_transmittal_files(args.dir)
        if not files:
            print(f"Error: no CRA transmittal .dat files found in {args.dir}")
            sys.exit(1)

        print("CD Command Center -- FFIEC CRA Batch Load")
        print(f"  Directory: {args.dir}")
        print(f"  Files found: {len(files)} ({files[0][0]}-{files[-1][0]})")
        if args.states:
            print(f"  States: {', '.join(args.states)}")
        print()

        db.init_db()
        grand_total = 0
        for year, filepath in files:
            print(f"  [{year}] {os.path.basename(filepath)}")
            try:
                rows = load_transmittal(filepath, year, args.states or [], columns_only=False)
                n = db.upsert_rows("cra_institutions", rows, unique_cols=["respondent_id", "report_year"])
                grand_total += n
                print(f"    Loaded {n:,} institution records.")
            except Exception as e:
                print(f"    ERROR: {e}")
        print()
        print(f"Done. Total rows upserted: {grand_total:,}")
        return

    # --- Single-year mode ---
    if args.auto:
        if not args.year:
            print("Error: --auto requires --year.")
            sys.exit(1)
        try:
            args.transmittal, args.assessment_area = download_cra_files(year=args.year)
        except Exception as e:
            print(f"Error downloading CRA files: {e}")
            sys.exit(1)

    if not args.transmittal and not args.assessment_area:
        print(
            "Error: provide --dir, --auto, or at least one of --transmittal / --assessment-area.\n"
            "  Batch load all years: python etl/fetch_cra_data.py --dir data/raw/cra\n"
            "  Download CRA flat files from: https://www.ffiec.gov/cradownload.htm"
        )
        sys.exit(1)

    if not args.year:
        # Try to infer from filename
        fn = args.transmittal or args.assessment_area
        args.year = infer_year_from_filename(fn)
        if not args.year:
            print("Error: could not infer year from filename. Please provide --year.")
            sys.exit(1)

    print("CD Command Center -- FFIEC CRA Load")
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
