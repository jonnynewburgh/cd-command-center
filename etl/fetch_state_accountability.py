"""
etl/fetch_state_accountability.py — Load state DOE accountability data into lea_accountability.

Each state publishes accountability data differently: different URLs, file formats,
and column names. This script has one function per state that handles that state's
data. Each function normalizes to the same output schema and loads via
db.upsert_lea_accountability().

Supported states: TX, CA, NY, FL

The key challenge for state data is matching state-assigned district IDs to
NCES LEA IDs. Our schools table has NCES IDs, so we can build a lookup.

Usage:
    # Texas (accepts Excel or CSV from TEA)
    python etl/fetch_state_accountability.py --state TX --year 2023 --file data/raw/tx_accountability.xlsx

    # California (state CSV from CDE)
    python etl/fetch_state_accountability.py --state CA --year 2023 --file data/raw/ca_accountability.csv

    # New York (CSV from NYSED)
    python etl/fetch_state_accountability.py --state NY --year 2023 --file data/raw/ny_accountability.csv

    # Florida (Excel from FLDOE)
    python etl/fetch_state_accountability.py --state FL --year 2023 --file data/raw/fl_district_grades.xlsx

    # Run without --file to get download instructions for that state
    python etl/fetch_state_accountability.py --state TX --year 2023

Where to download:
    TX: https://tea.texas.gov/texas-schools/accountability/academic-accountability/performance-reporting
    CA: https://www.cde.ca.gov/ta/ac/cm/
    NY: https://data.nysed.gov/downloads.php
    FL: https://www.fldoe.org/accountability/accountability-reporting/school-grades/
"""

import argparse
import sys
import os
import pandas as pd
import numpy as np

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import db


# ---------------------------------------------------------------------------
# Shared utilities
# ---------------------------------------------------------------------------

def normalize_cols(df):
    """Lowercase and strip whitespace from column names."""
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    return df


def _find_col(df, candidates):
    """Return the first matching column name from candidates, or None."""
    for c in candidates:
        if c in df.columns:
            return c
    return None


def pct_to_float(val):
    """
    Convert a string like '72.4%' or '72.4' to float 72.4.
    Returns None if conversion fails.
    """
    if val is None:
        return None
    try:
        return float(str(val).replace("%", "").strip())
    except (ValueError, TypeError):
        return None


def _read_file(filepath):
    """
    Read a CSV or Excel file into a DataFrame.
    Tries CSV first; falls back to Excel if the extension suggests it.
    """
    if filepath.endswith((".xlsx", ".xls")):
        return pd.read_excel(filepath, dtype=str)
    else:
        return pd.read_csv(filepath, dtype=str, low_memory=False)


def _upsert_rows(rows, state_label):
    """
    Upsert a list of record dicts into lea_accountability.
    Prints a summary. Returns (loaded, errors).
    """
    loaded = 0
    errors = 0
    for record in rows:
        # Remove None/NaN values so we don't overwrite existing data with NULL
        clean = {
            k: v for k, v in record.items()
            if v is not None and not (isinstance(v, float) and np.isnan(v))
        }
        if not clean.get("lea_id"):
            errors += 1
            continue
        try:
            db.upsert_lea_accountability(clean)
            loaded += 1
        except Exception as e:
            print(f"  Error: {e}")
            errors += 1
    print(f"  {state_label}: {loaded} loaded, {errors} errors")
    return loaded, errors


# ---------------------------------------------------------------------------
# State loaders
# ---------------------------------------------------------------------------

def load_texas(year, filepath=None):
    """
    Load Texas TEA district accountability data.

    TEA publishes district-level ratings (A/B/C/D/F) and domain scores.
    The file is an Excel download from TEA's performance reporting page.
    TEA uses NCES LEA IDs directly, so no ID mapping is needed.

    Download from:
      https://tea.texas.gov/texas-schools/accountability/academic-accountability/performance-reporting
      Look for "District Accountability Summary" for the target year.
    """
    if not filepath:
        print("TX: Download the district accountability file from:")
        print("  https://tea.texas.gov/texas-schools/accountability/academic-accountability/performance-reporting")
        print("Look for 'District Accountability Summary' and save to data/raw/")
        print("Then re-run: python etl/fetch_state_accountability.py --state TX --year 2023 --file data/raw/tx_accountability.xlsx")
        return

    print(f"Loading Texas accountability from {filepath}...")
    try:
        df = _read_file(filepath)
    except Exception as e:
        print(f"  Error reading file: {e}")
        return

    df = normalize_cols(df)

    # TEA column names vary slightly by year — we try several known names
    lea_col    = _find_col(df, ["district", "district_id", "distid", "lea_id", "nces_district_id", "district_number"])
    name_col   = _find_col(df, ["district_name", "distname", "lea_name", "district_name_"])
    rating_col = _find_col(df, ["accountability_rating", "district_rating", "overall_rating", "rating_cd"])
    score_col  = _find_col(df, ["overall_scaled_score", "scaled_score", "total_score", "domain_i_score"])

    if not lea_col:
        print("  Error: could not find LEA ID column.")
        print(f"  Available columns: {list(df.columns)}")
        return

    rows = []
    for _, row in df.iterrows():
        lea_id = str(row[lea_col]).strip() if row.get(lea_col) else None
        if not lea_id or lea_id.lower() in ("nan", "none", ""):
            continue

        record = {"lea_id": lea_id, "state": "TX", "data_year": year}
        if name_col:
            record["lea_name"] = row.get(name_col)
        if rating_col:
            record["accountability_rating"] = row.get(rating_col)
        if score_col:
            record["accountability_score"] = pct_to_float(row.get(score_col))
        rows.append(record)

    _upsert_rows(rows, "TX")


def load_california(year, filepath=None):
    """
    Load California Dashboard district-level accountability data.

    CA publishes district-level indicator data through the CA Dashboard.
    CA uses state-assigned County-District codes (7-digit CDS codes), NOT NCES IDs.
    We map CA CDS codes to NCES IDs using the schools table.

    Download from:
      https://www.cde.ca.gov/ta/ac/cm/
      Look for the "District" level data download (CSV or Excel).
    """
    if not filepath:
        print("CA: Download district accountability data from:")
        print("  https://www.cde.ca.gov/ta/ac/cm/")
        print("Look for 'Dashboard Data Download' at the District level.")
        print("Then re-run: python etl/fetch_state_accountability.py --state CA --year 2023 --file data/raw/ca_accountability.csv")
        return

    print(f"Loading California accountability from {filepath}...")
    try:
        df = _read_file(filepath)
    except Exception as e:
        print(f"  Error reading file: {e}")
        return

    df = normalize_cols(df)

    # CA CDS district code column
    lea_col    = _find_col(df, ["district_code", "districtcode", "cds_code", "district_cds", "lea_id"])
    name_col   = _find_col(df, ["district_name", "districtname", "lea_name"])
    rating_col = _find_col(df, ["overall_status", "status", "color", "rating", "accountability_rating"])
    score_col  = _find_col(df, ["overall_score", "score", "accountability_score", "ela_status"])

    if not lea_col:
        print("  Error: could not find LEA/district code column.")
        print(f"  Available columns: {list(df.columns)}")
        return

    # Build NCES ID lookup from the schools table for CA
    # CA CDS codes in the dashboard are 7-digit district codes; NCES IDs are also 7 digits
    # If they match directly, no mapping is needed. Otherwise, we try a prefix match.
    print("  Building NCES ID lookup from schools table for CA...")
    schools_df = db.get_schools(states=["CA"])
    nces_lookup = {}
    if not schools_df.empty and "lea_id" in schools_df.columns:
        for _, srow in schools_df.iterrows():
            nces_id = str(srow.get("lea_id") or "").strip()
            if nces_id:
                nces_lookup[nces_id] = nces_id

    rows = []
    unmapped = 0
    for _, row in df.iterrows():
        raw_id = str(row[lea_col]).strip() if row.get(lea_col) else None
        if not raw_id or raw_id.lower() in ("nan", "none", ""):
            continue

        # Try direct match first, then without leading zeros
        lea_id = nces_lookup.get(raw_id) or nces_lookup.get(raw_id.lstrip("0")) or raw_id
        if lea_id not in nces_lookup:
            unmapped += 1

        record = {"lea_id": lea_id, "state": "CA", "data_year": year}
        if name_col:
            record["lea_name"] = row.get(name_col)
        if rating_col:
            record["accountability_rating"] = str(row.get(rating_col)) if row.get(rating_col) else None
        if score_col:
            record["accountability_score"] = pct_to_float(row.get(score_col))
        rows.append(record)

    if unmapped:
        print(f"  Note: {unmapped} rows had district codes not found in the schools table (will still load).")

    _upsert_rows(rows, "CA")


def load_new_york(year, filepath=None):
    """
    Load New York NYSED district accountability data.

    NY publishes district report card data at data.nysed.gov.
    NY uses state-assigned district codes; NCES IDs are available in separate files
    but we fall back to using state IDs directly.

    Download from:
      https://data.nysed.gov/downloads.php
      Look for 'Report Card Database' or 'Accountability' downloads.
    """
    if not filepath:
        print("NY: Download district accountability data from:")
        print("  https://data.nysed.gov/downloads.php")
        print("Look for 'Report Card Database' or 'Accountability' downloads.")
        print("Then re-run: python etl/fetch_state_accountability.py --state NY --year 2023 --file data/raw/ny_accountability.csv")
        return

    print(f"Loading New York accountability from {filepath}...")
    try:
        df = _read_file(filepath)
    except Exception as e:
        print(f"  Error reading file: {e}")
        return

    df = normalize_cols(df)

    lea_col    = _find_col(df, ["district_beds_code", "bedscode", "entity_cd", "lea_id", "district_code", "nces_district_id"])
    name_col   = _find_col(df, ["district_name", "entity_name", "lea_name", "name"])
    rating_col = _find_col(df, ["accountability_status", "status", "designation", "rating"])
    read_col   = _find_col(df, ["ela_proficiency", "pct_proficient_ela", "reading_proficiency", "proficiency_ela"])
    math_col   = _find_col(df, ["math_proficiency", "pct_proficient_math", "proficiency_math"])
    grad_col   = _find_col(df, ["graduation_rate", "grad_rate", "cohort_grad_rate"])

    if not lea_col:
        print("  Error: could not find LEA/district code column.")
        print(f"  Available columns: {list(df.columns)}")
        return

    rows = []
    for _, row in df.iterrows():
        lea_id = str(row[lea_col]).strip() if row.get(lea_col) else None
        if not lea_id or lea_id.lower() in ("nan", "none", ""):
            continue

        record = {"lea_id": lea_id, "state": "NY", "data_year": year}
        if name_col:
            record["lea_name"] = row.get(name_col)
        if rating_col:
            record["accountability_rating"] = str(row.get(rating_col)) if row.get(rating_col) else None
        if read_col:
            record["proficiency_reading"] = pct_to_float(row.get(read_col))
        if math_col:
            record["proficiency_math"] = pct_to_float(row.get(math_col))
        if grad_col:
            record["graduation_rate"] = pct_to_float(row.get(grad_col))
        rows.append(record)

    _upsert_rows(rows, "NY")


def load_florida(year, filepath=None):
    """
    Load Florida FLDOE district grades data.

    FL publishes district grades (A through F) along with component scores.
    FL uses NCES LEA IDs directly, so no ID mapping is needed.

    Download from:
      https://www.fldoe.org/accountability/accountability-reporting/school-grades/
      Look for 'District Grades' Excel file for the target year.
    """
    if not filepath:
        print("FL: Download district grades data from:")
        print("  https://www.fldoe.org/accountability/accountability-reporting/school-grades/")
        print("Look for 'District Grades' Excel for the target year.")
        print("Then re-run: python etl/fetch_state_accountability.py --state FL --year 2023 --file data/raw/fl_district_grades.xlsx")
        return

    print(f"Loading Florida accountability from {filepath}...")
    try:
        df = _read_file(filepath)
    except Exception as e:
        print(f"  Error reading file: {e}")
        return

    df = normalize_cols(df)

    lea_col    = _find_col(df, ["district_number", "district_id", "nces_id", "lea_id", "district_code"])
    name_col   = _find_col(df, ["district_name", "lea_name", "district"])
    rating_col = _find_col(df, ["district_grade", "grade", "overall_grade", "accountability_rating"])
    score_col  = _find_col(df, ["total_points", "overall_score", "accountability_score", "points_earned"])
    read_col   = _find_col(df, ["ela_achievement", "reading_proficiency", "ela_pct_proficient"])
    math_col   = _find_col(df, ["math_achievement", "math_proficiency", "math_pct_proficient"])

    if not lea_col:
        print("  Error: could not find LEA ID column.")
        print(f"  Available columns: {list(df.columns)}")
        return

    rows = []
    for _, row in df.iterrows():
        lea_id = str(row[lea_col]).strip() if row.get(lea_col) else None
        if not lea_id or lea_id.lower() in ("nan", "none", ""):
            continue

        record = {"lea_id": lea_id, "state": "FL", "data_year": year}
        if name_col:
            record["lea_name"] = row.get(name_col)
        if rating_col:
            record["accountability_rating"] = str(row.get(rating_col)) if row.get(rating_col) else None
        if score_col:
            record["accountability_score"] = pct_to_float(row.get(score_col))
        if read_col:
            record["proficiency_reading"] = pct_to_float(row.get(read_col))
        if math_col:
            record["proficiency_math"] = pct_to_float(row.get(math_col))
        rows.append(record)

    _upsert_rows(rows, "FL")


# ---------------------------------------------------------------------------
# State loader registry
# ---------------------------------------------------------------------------

STATE_LOADERS = {
    "TX": load_texas,
    "CA": load_california,
    "NY": load_new_york,
    "FL": load_florida,
}

STATE_DOWNLOAD_URLS = {
    "TX": "https://tea.texas.gov/texas-schools/accountability/academic-accountability/performance-reporting",
    "CA": "https://www.cde.ca.gov/ta/ac/cm/",
    "NY": "https://data.nysed.gov/downloads.php",
    "FL": "https://www.fldoe.org/accountability/accountability-reporting/school-grades/",
}


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Load state DOE accountability data into the lea_accountability table"
    )
    parser.add_argument(
        "--state", required=True,
        choices=list(STATE_LOADERS.keys()),
        help=f"State to load ({', '.join(STATE_LOADERS.keys())})",
    )
    parser.add_argument(
        "--year", type=int, required=True,
        help="Data year (e.g. 2023)",
    )
    parser.add_argument(
        "--file",
        help="Path to local data file. If omitted, prints download instructions.",
    )
    args = parser.parse_args()

    db.init_db()

    loader = STATE_LOADERS[args.state]
    loader(year=args.year, filepath=args.file)

    print("Done.")


if __name__ == "__main__":
    main()
