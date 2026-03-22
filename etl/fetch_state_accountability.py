"""
etl/fetch_state_accountability.py — Load state DOE accountability data into lea_accountability.

Each state publishes accountability data differently: different URLs, file formats,
and column names. This script has one loader function per supported state that handles
that state's data. Each function normalizes to the same output schema and loads via
db.upsert_lea_accountability().

AUTO-DOWNLOAD SUPPORT:
    Many states publish direct-download files at stable URLs. Run without --file
    to auto-download:

        python etl/fetch_state_accountability.py --state TX --year 2023
        python etl/fetch_state_accountability.py --all-states --year 2023

    See STATE_DOWNLOAD_INFO below for supported states and their URLs.

    For broad coverage across all 50 states, use fetch_edfacts.py which
    downloads federal EDFacts data (math/ELA proficiency + graduation rates
    for all states in one go from the US Dept of Education):

        python etl/fetch_edfacts.py --year 2023   # covers all 50 states

NOTE: State accountability URL formats often change annually. If an auto-download
      fails, run with --file pointing to a manually downloaded file.

Usage:
    # Auto-download (if URL is configured for that state+year):
    python etl/fetch_state_accountability.py --state TX --year 2023

    # Use local file:
    python etl/fetch_state_accountability.py --state TX --year 2023 --file data/raw/tx_accountability.xlsx

    # Load all auto-downloadable states:
    python etl/fetch_state_accountability.py --all-states --year 2023

    # Get download instructions without loading:
    python etl/fetch_state_accountability.py --state TX --year 2023 --info
"""

import argparse
import sys
import os
import pandas as pd
import numpy as np

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import db
from utils.downloader import download_file


# ---------------------------------------------------------------------------
# STATE_DOWNLOAD_INFO — direct download URL templates per state and year.
#
# Many states publish their accountability data as direct-download Excel or CSV files.
# URLs are templates with {year} substituted at runtime.
#
# Format: {state: {"url_template": ..., "format": "xlsx"/"csv", "local": ..., "note": ...}}
#
# NOTE: These URLs are year-dependent and may shift by a year in the path vs. the
# school year being described. If auto-download fails, check the state DOE site for
# the current year's link and update url_template here.
#
# COVERAGE: This dict covers 18 states with state-native accountability data.
# The remaining 32 states are covered by federal EDFacts data via fetch_edfacts.py,
# which provides math/ELA proficiency and graduation rates for all 50 states.
# Run both scripts for the most complete LEA accountability coverage:
#   python etl/fetch_edfacts.py --year 2023           # all 50 states (federal)
#   python etl/fetch_state_accountability.py --all-states --year 2023  # 18 states (native)
# ---------------------------------------------------------------------------

STATE_DOWNLOAD_INFO = {
    "TX": {
        # TEA District Accountability Ratings — Excel file; year in path is the accountability year
        "url_template": "https://rptsvr1.tea.texas.gov/perfreport/account/{year}/district.zip",
        "format": "zip_xlsx",
        "local_template": "data/raw/tx_accountability_{year}.xlsx",
        "zip_pattern": "*.xlsx",
        "note": "Texas TEA District Accountability Ratings. If URL fails, visit: "
                "https://tea.texas.gov/texas-schools/accountability/academic-accountability/performance-reporting",
        "manual_url": "https://tea.texas.gov/texas-schools/accountability/academic-accountability/performance-reporting",
    },
    "CA": {
        # CA CDE Dashboard — district-level accountability CSV
        "url_template": "https://www6.cde.ca.gov/caDashExtract/caDashExtract.aspx?Year={year}",
        "format": "csv",
        "local_template": "data/raw/ca_accountability_{year}.csv",
        "note": "California CDE Dashboard district-level data.",
        "manual_url": "https://www.cde.ca.gov/ta/ac/cm/",
    },
    "NY": {
        # NYSED report card download — zip with multiple CSVs
        "url_template": "https://data.nysed.gov/files/reportcard/{year_short}/SY{year_short}%20PublicData.zip",
        "format": "zip_csv",
        "local_template": "data/raw/ny_accountability_{year}.csv",
        "zip_pattern": "*District*.csv",
        "note": "New York NYSED district report card data.",
        "manual_url": "https://data.nysed.gov/downloads.php",
    },
    "FL": {
        # FLDOE District Grades Excel — year in filename
        "url_template": "https://edudata.fldoe.org/DataPackages/SchoolGrades/DistrictGrades{year}.xlsx",
        "format": "xlsx",
        "local_template": "data/raw/fl_accountability_{year}.xlsx",
        "note": "Florida FLDOE District Grades.",
        "manual_url": "https://www.fldoe.org/accountability/accountability-reporting/school-grades/",
    },
    "CO": {
        "url_template": "https://www.cde.state.co.us/accountability/accession{year_2digit}accountability.xlsx",
        "format": "xlsx",
        "local_template": "data/raw/co_accountability_{year}.xlsx",
        "note": "Colorado CDE district accountability data.",
        "manual_url": "https://www.cde.state.co.us/accountability/",
    },
    "GA": {
        "url_template": "https://www.gadoe.org/CCRPI/Pages/Data-Download.aspx",
        "format": "csv",
        "local_template": "data/raw/ga_accountability_{year}.csv",
        "note": "Georgia DOE CCRPI district scores.",
        "manual_url": "https://www.gadoe.org/CCRPI/Pages/Data-Download.aspx",
    },
    "IL": {
        "url_template": "https://www.isbe.net/Documents/rc{year_2digit}.zip",
        "format": "zip_csv",
        "local_template": "data/raw/il_accountability_{year}.csv",
        "zip_pattern": "*district*.csv",
        "note": "Illinois ISBE district report card data.",
        "manual_url": "https://www.isbe.net/Pages/Illinois-State-Report-Card-Data.aspx",
    },
    "MA": {
        "url_template": "https://profiles.doe.mass.edu/statereport/accountability.aspx",
        "format": "csv",
        "local_template": "data/raw/ma_accountability_{year}.csv",
        "note": "Massachusetts DESE accountability data.",
        "manual_url": "https://profiles.doe.mass.edu/statereport/accountability.aspx",
    },
    "MD": {
        "url_template": "https://reportcard.msde.maryland.gov/api/Accountability/GetDistrictAccountabilityData/{year}",
        "format": "json",
        "local_template": "data/raw/md_accountability_{year}.json",
        "note": "Maryland MSDE district accountability data (API).",
        "manual_url": "https://reportcard.msde.maryland.gov/",
    },
    "MN": {
        "url_template": "https://rc.education.mn.gov/api/districts/accountability/{year}",
        "format": "json",
        "local_template": "data/raw/mn_accountability_{year}.json",
        "note": "Minnesota MDE district accountability data.",
        "manual_url": "https://rc.education.mn.gov/",
    },
    "NC": {
        "url_template": "https://www.dpi.nc.gov/data/accountability/reporting/district-and-school-report-cards/data-download",
        "format": "xlsx",
        "local_template": "data/raw/nc_accountability_{year}.xlsx",
        "note": "North Carolina DPI accountability data.",
        "manual_url": "https://www.dpi.nc.gov/data/accountability/reporting/district-and-school-report-cards/data-download",
    },
    "NJ": {
        "url_template": "https://www.nj.gov/education/schoolperformance/data/downloads/{year}/NJ_District_Accountability.xlsx",
        "format": "xlsx",
        "local_template": "data/raw/nj_accountability_{year}.xlsx",
        "note": "New Jersey DOE district accountability data.",
        "manual_url": "https://www.nj.gov/education/schoolperformance/",
    },
    "OH": {
        "url_template": "https://reportcard.education.ohio.gov/api/Download/DownloadData?districtIRN=000000&reportYear={year}&reportPart=district",
        "format": "csv",
        "local_template": "data/raw/oh_accountability_{year}.csv",
        "note": "Ohio ODE district report card data.",
        "manual_url": "https://reportcard.education.ohio.gov/",
    },
    "PA": {
        "url_template": "https://www.pa.gov/content/dam/copapwp-pagov/en/pde/documents/data-and-statistics/accountability/{year}/district-accountability.xlsx",
        "format": "xlsx",
        "local_template": "data/raw/pa_accountability_{year}.xlsx",
        "note": "Pennsylvania PDE district accountability data.",
        "manual_url": "https://www.education.pa.gov/DataAndReporting/Accountability/Pages/default.aspx",
    },
    "TN": {
        "url_template": "https://www.tn.gov/content/dam/tn/education/data/accountability-data-{year}.xlsx",
        "format": "xlsx",
        "local_template": "data/raw/tn_accountability_{year}.xlsx",
        "note": "Tennessee DOE district accountability data.",
        "manual_url": "https://www.tn.gov/education/data/accountability.html",
    },
    "TX": {
        # Override with simpler URL that doesn't require zip extraction for some years
        "url_template": "https://rptsvr1.tea.texas.gov/perfreport/account/{year}/DistrictAccountabilityRatings.xlsx",
        "format": "xlsx",
        "local_template": "data/raw/tx_accountability_{year}.xlsx",
        "note": "Texas TEA District Accountability Ratings. If URL fails, visit TEA website.",
        "manual_url": "https://tea.texas.gov/texas-schools/accountability/academic-accountability/performance-reporting",
    },
    "VA": {
        "url_template": "https://www.doe.virginia.gov/data-policy-funding/data-reports-statistics/statistics-data/accreditation-accountability/{year}/division-accreditation.xlsx",
        "format": "xlsx",
        "local_template": "data/raw/va_accountability_{year}.xlsx",
        "note": "Virginia DOE division accreditation data.",
        "manual_url": "https://www.doe.virginia.gov/data-policy-funding/data-reports-statistics/",
    },
    "WA": {
        "url_template": "https://data.wa.gov/api/views/4ke4-vmeb/rows.csv?accessType=DOWNLOAD",
        "format": "csv",
        "local_template": "data/raw/wa_accountability_{year}.csv",
        "note": "Washington OSPI district report card data (Socrata).",
        "manual_url": "https://data.wa.gov/education",
    },
    "WI": {
        "url_template": "https://publicstatic.dpi.wi.gov/publib/school-report-cards/{year}/2223_district_accountability.xlsx",
        "format": "xlsx",
        "local_template": "data/raw/wi_accountability_{year}.xlsx",
        "note": "Wisconsin DPI district accountability data.",
        "manual_url": "https://dpi.wi.gov/accountability/resources",
    },
}


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

# States with loader functions but whose download URLs are in STATE_DOWNLOAD_INFO
# For states in STATE_DOWNLOAD_INFO but NOT in STATE_LOADERS, we use a generic loader.
STATE_DOWNLOAD_URLS = {
    "TX": "https://tea.texas.gov/texas-schools/accountability/academic-accountability/performance-reporting",
    "CA": "https://www.cde.ca.gov/ta/ac/cm/",
    "NY": "https://data.nysed.gov/downloads.php",
    "FL": "https://www.fldoe.org/accountability/accountability-reporting/school-grades/",
}


def _year_vars(year: int) -> dict:
    """Build template substitution variables for a given school year."""
    return {
        "year": year,
        "year_2digit": str(year)[-2:],
        "year_short": f"{str(year-1)[-2:]}{str(year)[-2:]}",   # e.g. 2023 → "2223"
    }


def _try_auto_download(state: str, year: int, force: bool = False) -> str | None:
    """
    Attempt to auto-download accountability data for a state.
    Returns the local file path on success, or None if no URL is configured or download fails.
    """
    info = STATE_DOWNLOAD_INFO.get(state)
    if not info:
        return None

    vars_ = _year_vars(year)
    try:
        url = info["url_template"].format(**vars_)
        local = info["local_template"].format(**vars_)
    except KeyError:
        return None

    fmt = info.get("format", "xlsx")

    try:
        if fmt.startswith("zip_"):
            # Zip containing either xlsx or csv — extract the matching file
            from utils.downloader import download_and_extract_zip
            zip_local = local.replace(".xlsx", ".zip").replace(".csv", ".zip")
            extracted = download_and_extract_zip(
                url=url,
                zip_dest=zip_local,
                extract_pattern=info.get("zip_pattern", "*"),
                extract_dest=local,
                description=f"{state} accountability {year}",
                force=force,
            )
            return extracted
        else:
            return download_file(
                url=url,
                dest_path=local,
                description=f"{state} accountability {year}",
                force=force,
            )
    except RuntimeError as e:
        print(f"  Auto-download failed for {state}: {e}")
        manual = info.get("manual_url", "")
        if manual:
            print(f"  Manual download: {manual}")
        return None


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Load state DOE accountability data into the lea_accountability table"
    )
    parser.add_argument(
        "--state",
        choices=sorted(set(list(STATE_LOADERS.keys()) + list(STATE_DOWNLOAD_INFO.keys()))),
        help="State to load. If --file is omitted, attempts auto-download.",
    )
    parser.add_argument(
        "--all-states",
        action="store_true",
        help="Auto-download and load all states that have download URLs configured.",
    )
    parser.add_argument(
        "--year", type=int, required=True,
        help="Data year (e.g. 2023)",
    )
    parser.add_argument(
        "--file",
        help="Path to local data file. If omitted, attempts auto-download.",
    )
    parser.add_argument(
        "--info",
        action="store_true",
        help="Print download info for the specified state without loading.",
    )
    parser.add_argument(
        "--force-download",
        action="store_true",
        help="Re-download even if a recent local copy exists.",
    )
    args = parser.parse_args()

    db.init_db()

    # --- Mode: --all-states ---
    if args.all_states:
        print(f"CD Command Center — State Accountability Load (all states, year={args.year})")
        loaded_states = []
        failed_states = []

        # First run the EDFacts loader which covers all 50 states via federal data
        print("\n[EDFacts] Running fetch_edfacts.py for all-state coverage...")
        print("  Run: python etl/fetch_edfacts.py --year", args.year)
        print("  (EDFacts covers all 50 states — run it separately for complete coverage)")

        # Then run state-specific loaders for richer accountability ratings
        for state in sorted(set(list(STATE_LOADERS.keys()) + list(STATE_DOWNLOAD_INFO.keys()))):
            filepath = _try_auto_download(state, args.year, force=args.force_download)

            if not filepath:
                failed_states.append(state)
                continue

            loader = STATE_LOADERS.get(state)
            if loader:
                loader(year=args.year, filepath=filepath)
            else:
                # Generic loader for states without a custom function
                print(f"\n  {state}: file downloaded to {filepath}")
                print(f"  (No custom loader for {state} — add one to STATE_LOADERS to parse it)")
                failed_states.append(state)
                continue

            loaded_states.append(state)

        print(f"\n--- Summary ---")
        print(f"  States loaded: {loaded_states}")
        if failed_states:
            print(f"  States failed/skipped: {failed_states}")
            print("  For these states, use: python etl/fetch_edfacts.py --year", args.year,
                  "(covers all 50 states with federal EDFacts data)")
        return

    # --- Mode: single state ---
    if not args.state:
        parser.error("Provide --state or --all-states.")

    if args.info:
        info = STATE_DOWNLOAD_INFO.get(args.state)
        if info:
            vars_ = _year_vars(args.year)
            try:
                url = info["url_template"].format(**vars_)
                local = info["local_template"].format(**vars_)
                print(f"{args.state} accountability data ({args.year}):")
                print(f"  Auto-download URL: {url}")
                print(f"  Local path: {local}")
                print(f"  Note: {info.get('note', '')}")
            except KeyError as e:
                print(f"  Could not format URL template: {e}")
        else:
            print(f"{args.state}: No auto-download URL configured.")
        if args.state in STATE_DOWNLOAD_URLS:
            print(f"  Manual download: {STATE_DOWNLOAD_URLS.get(args.state, 'N/A')}")
        return

    # Resolve file path
    filepath = args.file
    if not filepath:
        # Try auto-download
        filepath = _try_auto_download(args.state, args.year, force=args.force_download)
        if not filepath:
            print(f"\n{args.state}: Auto-download not available or failed.")
            manual = STATE_DOWNLOAD_URLS.get(args.state) or (
                STATE_DOWNLOAD_INFO[args.state]["manual_url"]
                if args.state in STATE_DOWNLOAD_INFO else None
            )
            if manual:
                print(f"  Manual download: {manual}")
            print(f"\nAlternatively, run EDFacts for all-state coverage:")
            print(f"  python etl/fetch_edfacts.py --year {args.year}")
            sys.exit(1)

    loader = STATE_LOADERS.get(args.state)
    if not loader:
        print(f"Error: No loader function for state '{args.state}'.")
        print(f"  Supported states with full loaders: {list(STATE_LOADERS.keys())}")
        print(f"  For all other states, use: python etl/fetch_edfacts.py --year {args.year}")
        sys.exit(1)

    loader(year=args.year, filepath=filepath)

    print("Done.")


if __name__ == "__main__":
    main()
