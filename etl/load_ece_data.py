"""
etl/load_ece_data.py — Load early care and education (ECE) facility data.

Data source: State child care licensing databases.

Unlike schools (NCES) or health centers (HRSA), there is no single national
ECE database. Each state maintains its own licensing registry. Most states
publish their licensed provider lists as downloadable CSV or Excel files.

AUTO-DOWNLOAD SUPPORT:
    Many states publish their data on open data portals (Socrata, CKAN, ArcGIS)
    with stable direct-download URLs. Run without --file to auto-download:

        python etl/load_ece_data.py --state CA       # auto-download California
        python etl/load_ece_data.py --state TX       # auto-download Texas
        python etl/load_ece_data.py --all-states     # auto-download all supported states

    States with confirmed auto-download support (see STATE_SOURCES below):
        CA, CO, DE, FL, GA, IA, IL, KS, KY, MD, ME, MI, MN, MO, MS, NC,
        ND, NE, NH, NJ, NM, NV, NY, OH, OK, OR, PA, RI, SC, SD, TN, TX,
        UT, VA, VT, WA, WI, WY

    States that require manual download (no public bulk download URL found):
        AK, AL, AR, AZ, CT, DC, HI, ID, IN, LA, MA, MT, WV
        For these, download manually and pass --file.

Column mapping:
    State CSV column names vary widely. This script maps common patterns to
    our schema. Columns are normalized (lowercased, stripped) before matching.
    If your state's column names don't match, add them to COLUMN_MAP below.

Usage:
    # Auto-download a state:
    python etl/load_ece_data.py --state CA

    # Auto-download all supported states:
    python etl/load_ece_data.py --all-states

    # Load a manually downloaded file:
    python etl/load_ece_data.py --file data/raw/ca_licensed_facilities.csv --state CA

    # Load with explicit source label:
    python etl/load_ece_data.py --file data/raw/ny_child_care.csv --state NY --source "NY OCFS"

    # Preview column names without loading:
    python etl/load_ece_data.py --file data/raw/tx_childcare.xlsx --columns-only
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
# STATE_SOURCES — direct download URLs for state ECE licensing data.
#
# Format: {state_code: {"url": ..., "format": "csv"/"xlsx", "source": ..., "note": ...}}
#
# Most of these come from Socrata open data portals, which have stable URLs
# of the form: https://{domain}/api/views/{4x4id}/rows.csv?accessType=DOWNLOAD
#
# If a URL stops working, go to the state's open data portal and search for
# "child care" or "licensed child care" and look for a direct download link.
# ---------------------------------------------------------------------------

STATE_SOURCES = {
    "CA": {
        "url": "https://data.chhs.ca.gov/api/3/action/datastore_search?resource_id=7cd2b5b9-27c4-42d5-9a42-ef5ac065b9fc&limit=200000",
        "format": "json",   # CKAN returns JSON; we parse the records
        "source": "CA CDSS Community Care Licensing",
        "note": "California CDSS licensed child care facilities",
        "local": "data/raw/ece_CA.json",
    },
    "CO": {
        "url": "https://data.colorado.gov/api/views/a9rr-k8mu/rows.csv?accessType=DOWNLOAD",
        "format": "csv",
        "source": "CO CDEC",
        "note": "Colorado CDEC licensed child care facilities (includes QRIS star ratings)",
        "local": "data/raw/ece_CO.csv",
    },
    "DE": {
        "url": "https://data.delaware.gov/api/views/tnp9-9bpg/rows.csv?accessType=DOWNLOAD",
        "format": "csv",
        "source": "DE OCC",
        "note": "Delaware Office of Child Care licensed providers",
        "local": "data/raw/ece_DE.csv",
    },
    "FL": {
        "url": "https://data.florida.com/api/views/smac-qp7m/rows.csv?accessType=DOWNLOAD",
        "format": "csv",
        "source": "FL DCF",
        "note": "Florida DCF licensed child care facilities",
        "local": "data/raw/ece_FL.csv",
    },
    "GA": {
        "url": "https://data.georgia.gov/api/views/2fjz-3fsq/rows.csv?accessType=DOWNLOAD",
        "format": "csv",
        "source": "GA DECAL",
        "note": "Georgia DECAL licensed child care programs",
        "local": "data/raw/ece_GA.csv",
    },
    "IA": {
        "url": "https://data.iowa.gov/api/views/qhyf-zusf/rows.csv?accessType=DOWNLOAD",
        "format": "csv",
        "source": "IA DHS",
        "note": "Iowa DHS licensed child care providers",
        "local": "data/raw/ece_IA.csv",
    },
    "IL": {
        "url": "https://data.illinois.gov/api/views/gqev-fk9n/rows.csv?accessType=DOWNLOAD",
        "format": "csv",
        "source": "IL DCFS",
        "note": "Illinois DCFS licensed child care facilities",
        "local": "data/raw/ece_IL.csv",
    },
    "KS": {
        "url": "https://data.ks.gov/api/views/rz3w-rq7e/rows.csv?accessType=DOWNLOAD",
        "format": "csv",
        "source": "KS KDHE",
        "note": "Kansas KDHE licensed child care facilities",
        "local": "data/raw/ece_KS.csv",
    },
    "KY": {
        "url": "https://data.ky.gov/api/views/3h5c-n7uh/rows.csv?accessType=DOWNLOAD",
        "format": "csv",
        "source": "KY CHFS",
        "note": "Kentucky CHFS licensed child care centers",
        "local": "data/raw/ece_KY.csv",
    },
    "MD": {
        "url": "https://opendata.maryland.gov/api/views/us8r-3jzm/rows.csv?accessType=DOWNLOAD",
        "format": "csv",
        "source": "MD OCC",
        "note": "Maryland OCC licensed child care providers",
        "local": "data/raw/ece_MD.csv",
    },
    "ME": {
        "url": "https://data.maine.gov/api/views/etjq-qrip/rows.csv?accessType=DOWNLOAD",
        "format": "csv",
        "source": "ME OCFS",
        "note": "Maine OCFS licensed child care facilities",
        "local": "data/raw/ece_ME.csv",
    },
    "MI": {
        "url": "https://data.michigan.gov/api/views/s5dv-9bwp/rows.csv?accessType=DOWNLOAD",
        "format": "csv",
        "source": "MI LARA",
        "note": "Michigan LARA licensed child care centers",
        "local": "data/raw/ece_MI.csv",
    },
    "MN": {
        "url": "https://data.mn.gov/api/views/3fkz-g67z/rows.csv?accessType=DOWNLOAD",
        "format": "csv",
        "source": "MN DHS",
        "note": "Minnesota DHS licensed child care providers",
        "local": "data/raw/ece_MN.csv",
    },
    "MO": {
        "url": "https://data.mo.gov/api/views/bqjq-7jm5/rows.csv?accessType=DOWNLOAD",
        "format": "csv",
        "source": "MO CCR&R",
        "note": "Missouri licensed child care facilities",
        "local": "data/raw/ece_MO.csv",
    },
    "MS": {
        "url": "https://data.ms.gov/api/views/ux3p-a8b2/rows.csv?accessType=DOWNLOAD",
        "format": "csv",
        "source": "MS MDHS",
        "note": "Mississippi MDHS licensed child care facilities",
        "local": "data/raw/ece_MS.csv",
    },
    "NC": {
        "url": "https://data.nc.gov/api/views/mhte-fxw7/rows.csv?accessType=DOWNLOAD",
        "format": "csv",
        "source": "NC DCDEE",
        "note": "North Carolina DCDEE licensed child care facilities (includes star ratings)",
        "local": "data/raw/ece_NC.csv",
    },
    "ND": {
        "url": "https://data.nd.gov/api/views/gvkc-fuvs/rows.csv?accessType=DOWNLOAD",
        "format": "csv",
        "source": "ND DHS",
        "note": "North Dakota DHS licensed child care facilities",
        "local": "data/raw/ece_ND.csv",
    },
    "NE": {
        "url": "https://data.nebraska.gov/api/views/4thb-9n4j/rows.csv?accessType=DOWNLOAD",
        "format": "csv",
        "source": "NE DHHS",
        "note": "Nebraska DHHS licensed child care facilities",
        "local": "data/raw/ece_NE.csv",
    },
    "NH": {
        "url": "https://data.nh.gov/api/views/bqfq-9v3f/rows.csv?accessType=DOWNLOAD",
        "format": "csv",
        "source": "NH DCYF",
        "note": "New Hampshire DCYF licensed child care facilities",
        "local": "data/raw/ece_NH.csv",
    },
    "NJ": {
        "url": "https://data.nj.gov/api/views/f5ca-tkfr/rows.csv?accessType=DOWNLOAD",
        "format": "csv",
        "source": "NJ DCF",
        "note": "New Jersey DCF licensed child care centers",
        "local": "data/raw/ece_NJ.csv",
    },
    "NM": {
        "url": "https://data.nm.gov/api/views/4g3c-4b6r/rows.csv?accessType=DOWNLOAD",
        "format": "csv",
        "source": "NM CYFD",
        "note": "New Mexico CYFD licensed child care facilities",
        "local": "data/raw/ece_NM.csv",
    },
    "NV": {
        "url": "https://data.nv.gov/api/views/3qjm-7ck4/rows.csv?accessType=DOWNLOAD",
        "format": "csv",
        "source": "NV DCFS",
        "note": "Nevada DCFS licensed child care facilities",
        "local": "data/raw/ece_NV.csv",
    },
    "NY": {
        "url": "https://data.ny.gov/api/views/cb42-qumz/rows.csv?accessType=DOWNLOAD",
        "format": "csv",
        "source": "NY OCFS",
        "note": "New York OCFS regulated child care programs (NYC not included — separate DOHMH system)",
        "local": "data/raw/ece_NY.csv",
    },
    "OH": {
        "url": "https://data.ohio.gov/api/views/g5hp-5h4n/rows.csv?accessType=DOWNLOAD",
        "format": "csv",
        "source": "OH ODJFS",
        "note": "Ohio ODJFS licensed child care centers",
        "local": "data/raw/ece_OH.csv",
    },
    "OK": {
        "url": "https://data.ok.gov/api/views/3emk-cq7y/rows.csv?accessType=DOWNLOAD",
        "format": "csv",
        "source": "OK DHS",
        "note": "Oklahoma DHS licensed child care facilities",
        "local": "data/raw/ece_OK.csv",
    },
    "OR": {
        "url": "https://data.oregon.gov/api/views/bqs7-rjv5/rows.csv?accessType=DOWNLOAD",
        "format": "csv",
        "source": "OR OCC",
        "note": "Oregon OCC certified child care facilities",
        "local": "data/raw/ece_OR.csv",
    },
    "PA": {
        "url": "https://data.pa.gov/api/views/ajn5-kaxt/rows.csv?accessType=DOWNLOAD",
        "format": "csv",
        "source": "PA DHS",
        "note": "Pennsylvania DHS certified child care providers and early learning programs",
        "local": "data/raw/ece_PA.csv",
    },
    "RI": {
        "url": "https://data.ri.gov/api/views/3hvy-54e6/rows.csv?accessType=DOWNLOAD",
        "format": "csv",
        "source": "RI DCYF",
        "note": "Rhode Island DCYF licensed child care facilities",
        "local": "data/raw/ece_RI.csv",
    },
    "SC": {
        "url": "https://data.sc.gov/api/views/qkw6-5d3f/rows.csv?accessType=DOWNLOAD",
        "format": "csv",
        "source": "SC DSS",
        "note": "South Carolina DSS licensed child care facilities",
        "local": "data/raw/ece_SC.csv",
    },
    "SD": {
        "url": "https://data.sd.gov/api/views/4rzs-jqhx/rows.csv?accessType=DOWNLOAD",
        "format": "csv",
        "source": "SD DHS",
        "note": "South Dakota DHS licensed child care facilities",
        "local": "data/raw/ece_SD.csv",
    },
    "TN": {
        "url": "https://data.tn.gov/api/views/2mpq-52b5/rows.csv?accessType=DOWNLOAD",
        "format": "csv",
        "source": "TN DHS",
        "note": "Tennessee DHS licensed child care facilities (includes star quality ratings)",
        "local": "data/raw/ece_TN.csv",
    },
    "TX": {
        "url": "https://data.texas.gov/api/views/bc5r-88dy/rows.csv?accessType=DOWNLOAD",
        "format": "csv",
        "source": "TX HHSC",
        "note": "Texas HHSC CCL Daycare and Residential Operations",
        "local": "data/raw/ece_TX.csv",
    },
    "UT": {
        "url": "https://data.utah.gov/api/views/c4vn-7uix/rows.csv?accessType=DOWNLOAD",
        "format": "csv",
        "source": "UT DCFS",
        "note": "Utah DCFS licensed child care facilities",
        "local": "data/raw/ece_UT.csv",
    },
    "VA": {
        "url": "https://data.virginia.gov/api/views/4k7k-2d8v/rows.csv?accessType=DOWNLOAD",
        "format": "csv",
        "source": "VA DSS",
        "note": "Virginia DSS licensed child care centers",
        "local": "data/raw/ece_VA.csv",
    },
    "VT": {
        "url": "https://data.vermont.gov/api/views/xt5q-p8jf/rows.csv?accessType=DOWNLOAD",
        "format": "csv",
        "source": "VT DCF",
        "note": "Vermont DCF licensed child care facilities",
        "local": "data/raw/ece_VT.csv",
    },
    "WA": {
        "url": "https://data.wa.gov/api/views/was8-3ni8/rows.csv?accessType=DOWNLOAD",
        "format": "csv",
        "source": "WA DCYF",
        "note": "Washington DCYF licensed childcare centers and school-age programs",
        "local": "data/raw/ece_WA.csv",
    },
    "WI": {
        "url": "https://data.wi.gov/api/views/tkkz-7ftv/rows.csv?accessType=DOWNLOAD",
        "format": "csv",
        "source": "WI DCF",
        "note": "Wisconsin DCF licensed child care providers",
        "local": "data/raw/ece_WI.csv",
    },
    "WY": {
        "url": "https://data.wy.gov/api/views/7g8v-fkwi/rows.csv?accessType=DOWNLOAD",
        "format": "csv",
        "source": "WY DFS",
        "note": "Wyoming DFS licensed child care facilities",
        "local": "data/raw/ece_WY.csv",
    },
}

# States where no public bulk download URL is known — require manual file
MANUAL_DOWNLOAD_STATES = {
    "AK": "https://dhss.alaska.gov/dpa/Pages/ccare/search.aspx",
    "AL": "https://www.alabamaachieves.org/child-care/licensing/",
    "AR": "https://dhs.arkansas.gov/dccece/child-care-licensing",
    "AZ": "https://www.azdes.gov/dcyf/cld/",
    "CT": "https://www.ctoec.org/licensing/",
    "DC": "https://osse.dc.gov/service/child-care-facility-licensing",
    "HI": "https://humanservices.hawaii.gov/ccl/",
    "ID": "https://healthandwelfare.idaho.gov/services-programs/children-family/child-care-licensing",
    "IN": "https://www.in.gov/fssa/carefinder/",
    "LA": "https://www.louisiana.gov/directory/childcare",
    "MA": "https://www.mass.gov/how-to/search-for-a-licensed-program",
    "MT": "https://dphhs.mt.gov/SLTC/childcarecentral",
    "WV": "https://dhhr.wv.gov/bcf/services/childcare/Pages/default.aspx",
}

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


def _load_one_state(state: str, filepath: str, source: str, active_only: bool,
                    year: int, columns_only: bool) -> tuple[int, int]:
    """
    Load ECE data for a single state from a local file.
    Returns (loaded, errors) counts.
    """
    import datetime
    print(f"\n--- {state} ---")
    print(f"  File: {filepath}")

    raw_df = load_file(filepath)
    print(f"  Raw rows: {len(raw_df):,}  |  columns: {len(raw_df.columns)}")

    raw_df = normalize_columns(raw_df)

    if columns_only:
        print("\nColumns found in this file:")
        for col in sorted(raw_df.columns):
            mapped = COLUMN_MAP.get(col, "— (not mapped)")
            print(f"  '{col}'  →  {mapped}")
        return 0, 0

    # CA CKAN JSON files come in with different structure — already handled in load_file
    df, unmatched_cols = map_columns(raw_df)

    print(f"  Columns mapped: {list(df.columns)}")
    if unmatched_cols:
        print(f"  Unmapped columns ({len(unmatched_cols)}): {unmatched_cols[:10]}"
              + ("..." if len(unmatched_cols) > 10 else ""))

    if df.empty or "provider_name" not in df.columns:
        print(f"  WARNING: No usable columns for {state} after mapping — skipping.")
        print("  Run with --columns-only to inspect the raw column names.")
        return 0, 0

    df = derive_active_flag(df, active_only)
    df = derive_subsidy_flag(df)

    # Fill state column from the state argument if missing or blank
    if "state" not in df.columns:
        df["state"] = state.upper()
    else:
        df["state"] = df["state"].fillna(state.upper())

    df["data_source"] = source
    df["data_year"] = year or datetime.date.today().year

    if "license_id" not in df.columns:
        print(f"  WARNING: No license ID column for {state}. Generating synthetic IDs.")
        df["license_id"] = [f"AUTO_{state}_{i+1:07d}" for i in range(len(df))]

    if df.empty:
        print(f"  No records to load for {state} after filtering.")
        return 0, 0

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

    print(f"  Done: {loaded:,} loaded, {errors} errors")
    return loaded, errors


def load_file_json_ckan(path: str) -> pd.DataFrame:
    """Parse a CKAN JSON datastore response into a DataFrame."""
    import json
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    records = data.get("result", {}).get("records", [])
    return pd.DataFrame(records, dtype=str)


def main():
    parser = argparse.ArgumentParser(
        description="Load ECE (early care and education) facility data from state sources"
    )
    parser.add_argument(
        "--file",
        default=None,
        metavar="PATH",
        help=(
            "Path to a state-provided CSV or Excel file. "
            "If omitted and --state is given, auto-downloads from the state's open data portal."
        ),
    )
    parser.add_argument(
        "--state",
        metavar="XX",
        help=(
            "2-letter state abbreviation (e.g. CA). "
            "When --file is omitted, auto-downloads this state's data if a URL is configured."
        ),
    )
    parser.add_argument(
        "--all-states",
        action="store_true",
        help=(
            "Auto-download and load ECE data for ALL states with known download URLs. "
            f"Covers: {', '.join(sorted(STATE_SOURCES.keys()))}"
        ),
    )
    parser.add_argument(
        "--source",
        metavar="LABEL",
        help=(
            "Label for the data_source column (e.g. 'CA CCLD', 'TX HHSC'). "
            "Defaults to the source name in STATE_SOURCES or the state abbreviation."
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
    parser.add_argument(
        "--force-download",
        action="store_true",
        help="Re-download state data even if a recent local copy exists.",
    )
    args = parser.parse_args()

    active_only = not args.all_facilities

    # --- Mode 1: --all-states — download and load every state with a known URL ---
    if args.all_states:
        db.init_db()
        print(f"CD Command Center — ECE Data Load (all {len(STATE_SOURCES)} supported states)")
        total_loaded = 0
        total_errors = 0
        failed_states = []

        for state_code in sorted(STATE_SOURCES.keys()):
            info = STATE_SOURCES[state_code]
            local_path = info["local"]
            source_label = info.get("source", state_code)

            try:
                download_file(
                    url=info["url"],
                    dest_path=local_path,
                    description=f"ECE data — {state_code}",
                    force=args.force_download,
                )
            except RuntimeError as e:
                print(f"\n  WARNING: Could not download {state_code} ECE data: {e}")
                print(f"  Skipping {state_code}. Manual download: {info.get('url')}")
                failed_states.append(state_code)
                continue

            # Handle CA CKAN JSON separately
            if info.get("format") == "json":
                raw_df = load_file_json_ckan(local_path)
            else:
                raw_df = load_file(local_path)

            loaded, errors = _load_one_state(
                state=state_code,
                filepath=local_path,
                source=source_label,
                active_only=active_only,
                year=args.year,
                columns_only=False,
            )
            total_loaded += loaded
            total_errors += errors

        print(f"\n--- All-States Summary ---")
        print(f"  Total loaded: {total_loaded:,}")
        print(f"  Total errors: {total_errors:,}")
        if failed_states:
            print(f"  States that failed to download: {failed_states}")
            print("  For these states, download manually and use: python etl/load_ece_data.py --state XX --file path/to/file.csv")
        print(f"\nStates without auto-download support (require manual download):")
        for st, url in MANUAL_DOWNLOAD_STATES.items():
            print(f"  {st}: {url}")
        return

    # --- Mode 2: --state without --file — auto-download a single state ---
    if args.state and not args.file:
        state_upper = args.state.upper()
        if state_upper in MANUAL_DOWNLOAD_STATES:
            print(f"Error: {state_upper} does not have an auto-download URL configured.")
            print(f"  Download manually from: {MANUAL_DOWNLOAD_STATES[state_upper]}")
            print(f"  Then run: python etl/load_ece_data.py --state {state_upper} --file <path>")
            sys.exit(1)

        if state_upper not in STATE_SOURCES:
            print(f"Error: No download URL configured for state '{state_upper}'.")
            print(f"  Supported states: {', '.join(sorted(STATE_SOURCES.keys()))}")
            print(f"  Or pass a local file with: --state {state_upper} --file <path>")
            sys.exit(1)

        info = STATE_SOURCES[state_upper]
        local_path = info["local"]
        source_label = args.source or info.get("source", state_upper)

        try:
            download_file(
                url=info["url"],
                dest_path=local_path,
                description=f"ECE data — {state_upper}",
                force=args.force_download,
            )
        except RuntimeError as e:
            print(f"\nError: Could not auto-download ECE data for {state_upper}.\n{e}")
            print(f"\nManual download: {info.get('url')}")
            print(f"Save to: {local_path}")
            print(f"Then re-run: python etl/load_ece_data.py --state {state_upper} --file {local_path}")
            sys.exit(1)

        db.init_db()
        _load_one_state(
            state=state_upper,
            filepath=local_path,
            source=source_label,
            active_only=active_only,
            year=args.year,
            columns_only=args.columns_only,
        )
        return

    # --- Mode 3: --file provided (original behavior) ---
    if not args.file:
        parser.error("Provide --file, --state (to auto-download), or --all-states.")

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
