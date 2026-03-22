"""
etl/load_ejscreen.py — Load EPA EJScreen environmental justice indicators into census_tracts.

EJScreen is the EPA's environmental justice mapping and screening tool.
It scores every census tract (and block group) on environmental burden
and demographic vulnerability. These indicators help assess whether a
community faces disproportionate environmental risk.

Relevant for CD finance because:
- Funders increasingly require EJ analysis for place-based investments
- FQHCs, ECE centers, and community facilities in high-EJ tracts may qualify
  for additional grant funding or policy support

Data source:
  EPA publishes the national EJScreen dataset annually. As of 2026, the EPA's
  gaftp.epa.gov FTP server is no longer accessible. The Zenodo archive is used:

    https://zenodo.org/records/14767363
    → Auto-downloads the 2024 zip (~5.2 GB) and extracts the tract-level CSV.

  The file is large (~800MB–1GB uncompressed). Use --states to load a subset.
  Use --file to pass a pre-downloaded CSV and skip the download entirely.

EJScreen variables this script loads (all are national percentile ranks, 0–100):
  EJ_PCTILE_D2_PM25    → pm25_percentile (particulate matter 2.5)
  EJ_PCTILE_D5_DIESEL  → diesel_percentile (diesel particulate exposure)
  EJ_PCTILE_D9_LDPNT   → lead_paint_percentile (lead paint indicator)
  EJ_PCTILE_D10_SFUND  → superfund_percentile (Superfund proximity)
  EJ_PCTILE_D11_RMP    → (not stored — RMP facility proximity)
  EJ_PCTILE_D12_TSDF   → (not stored — hazardous waste proximity)
  EJ_PCTILE_D13_WWDIS  → wastewater_percentile (wastewater discharge)
  EJ_D1_PCTILE         → ej_index (composite EJScreen score, D1 index)

Column names vary slightly between EJScreen versions. This script tries
several known column naming patterns.

Usage:
    python etl/load_ejscreen.py                                                     # auto-download
    python etl/load_ejscreen.py --states CA TX NY                                   # auto-download, filter states
    python etl/load_ejscreen.py --file data/raw/EJSCREEN_2024_Tracts.csv            # use local CSV
    python etl/load_ejscreen.py --file data/raw/EJSCREEN_2024_Tracts.csv --columns-only
"""

import argparse
import sys
import os

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import db
from utils.downloader import download_and_extract_zip

# Zenodo archive for EJScreen 2024 tract-level data.
# The zip contains a large CSV with all tracts nationally.
# If this URL changes, find the new one at: https://zenodo.org/records/14767363
EJSCREEN_ZIP_URL = "https://zenodo.org/records/14767363/files/2024.zip"
EJSCREEN_ZIP_LOCAL = "data/raw/ejscreen_2024.zip"
EJSCREEN_CSV_LOCAL = "data/raw/EJSCREEN_2024_Tracts.csv"
EJSCREEN_CSV_PATTERN = "EJSCREEN_2024_Tracts*.csv"

# Column name candidates for each indicator.
# EJScreen has changed column names across releases; we try all known names.
# Format: {our_column: [list of candidate column names in EPA file]}
COLUMN_MAP = {
    "ej_index":               ["EJ_D1_PCTILE", "EJ_PCTILE", "EJINDEX", "EJSCORE"],
    "pm25_percentile":        ["EJ_PCTILE_D2_PM25",   "PM25_EJ_PCTILE",   "P_PM25",    "PM25PCTL"],
    "diesel_percentile":      ["EJ_PCTILE_D5_DIESEL",  "DIESEL_EJ_PCTILE", "P_DIESEL",  "DSLPM_PCTILE"],
    "lead_paint_percentile":  ["EJ_PCTILE_D9_LDPNT",   "LDPNT_EJ_PCTILE",  "P_LDPNT",   "LDPNT_PCTILE"],
    "superfund_percentile":   ["EJ_PCTILE_D10_SFUND",  "SFUND_EJ_PCTILE",  "P_PNPL",    "SFUND_PCTILE"],
    "wastewater_percentile":  ["EJ_PCTILE_D13_WWDIS",  "WWDIS_EJ_PCTILE",  "P_PWDIS",   "WWDIS_PCTILE"],
}

# Column candidates for the tract FIPS identifier
TRACT_ID_CANDIDATES = ["ID", "GEOID", "GEOID10", "Census_Tract_FIPS", "FIPS", "GEO_ID", "GEOID_DATA"]


def find_column(df: pd.DataFrame, candidates: list) -> str | None:
    """Return the first candidate that exists as a column in df, or None."""
    for col in candidates:
        if col in df.columns:
            return col
    return None


def normalize_tract_id(value) -> str | None:
    """Zero-pad a census tract FIPS to 11 digits."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    try:
        raw = str(value).strip().split(".")[0]  # strip .0 from numeric strings
        return raw.zfill(11) if len(raw) <= 11 else raw[:11]
    except (ValueError, TypeError):
        return None


def main():
    parser = argparse.ArgumentParser(
        description="Load EPA EJScreen environmental justice indicators into census_tracts"
    )
    parser.add_argument(
        "--file",
        default=None,
        help=(
            "Path to a pre-downloaded EJScreen tract CSV. "
            f"If omitted, auto-downloads the 2024 zip from Zenodo to {EJSCREEN_CSV_LOCAL}. "
            "WARNING: The zip is ~5 GB. Use --states to limit what gets loaded into the DB."
        ),
    )
    parser.add_argument(
        "--states",
        nargs="+",
        metavar="STATE",
        help="2-letter state abbreviations to load (default: all). "
             "Use this on the large national file to load a subset first.",
    )
    parser.add_argument(
        "--columns-only",
        action="store_true",
        help="Print column names from the file and exit (useful for debugging).",
    )
    parser.add_argument(
        "--force-download",
        action="store_true",
        help="Re-download even if a recent local copy already exists.",
    )
    args = parser.parse_args()

    # Resolve local CSV path — auto-download if --file not provided
    local_csv = args.file or EJSCREEN_CSV_LOCAL

    if args.file and not os.path.exists(args.file):
        print(f"Error: file not found: {args.file}")
        sys.exit(1)

    if not args.file:
        print("CD Command Center — EJScreen Auto-Download")
        print("  Note: The EJScreen zip is ~5 GB. This may take a while on slow connections.")
        if args.states:
            print(f"  States to load after download: {args.states}")
        try:
            local_csv = download_and_extract_zip(
                url=EJSCREEN_ZIP_URL,
                zip_dest=EJSCREEN_ZIP_LOCAL,
                extract_pattern=EJSCREEN_CSV_PATTERN,
                extract_dest=EJSCREEN_CSV_LOCAL,
                description="EPA EJScreen 2024 (national tract data)",
                force=args.force_download,
            )
        except RuntimeError as e:
            print(f"\nError: Could not auto-download EJScreen data.\n{e}")
            print("\nManual download instructions:")
            print("  1. Go to: https://zenodo.org/records/14767363")
            print("  2. Download '2024.zip'")
            print("  3. Extract the CSV named EJSCREEN_2024_Tracts_*.csv")
            print(f"  4. Save it to: {EJSCREEN_CSV_LOCAL}")
            print(f"  5. Re-run: python etl/load_ejscreen.py --file {EJSCREEN_CSV_LOCAL}")
            sys.exit(1)

    print(f"CD Command Center — EJScreen Load")
    print(f"  File: {local_csv}")
    print(f"  Reading file (may take a moment for the full national file)...")

    # Read with low_memory=False to avoid dtype inference issues on large file
    df = pd.read_csv(local_csv, dtype=str, low_memory=False)
    print(f"  Rows: {len(df):,}  |  Columns: {len(df.columns)}")

    if args.columns_only:
        print("  Columns:")
        for col in df.columns:
            print(f"    {col}")
        return

    # Find the census tract ID column
    tract_col = find_column(df, TRACT_ID_CANDIDATES)
    if not tract_col:
        print(f"Error: could not find tract ID column. Tried: {TRACT_ID_CANDIDATES}")
        print(f"  Available columns: {list(df.columns[:30])} ...")
        sys.exit(1)
    print(f"  Tract ID column: '{tract_col}'")

    # Find the state column (for filtering) — try EJSCREEN's known state column names
    state_col = find_column(df, ["ST_ABBREV", "STATE_NAME", "STATENAME", "STATE", "ST"])
    if args.states and not state_col:
        print(f"Warning: --states filter requested but no state column found. Loading all rows.")

    # Apply state filter if requested
    if args.states and state_col:
        state_upper = [s.upper() for s in args.states]
        df = df[df[state_col].str.upper().isin(state_upper)]
        print(f"  After state filter ({', '.join(args.states)}): {len(df):,} rows")

    if df.empty:
        print("  No rows after filtering. Check state abbreviations.")
        sys.exit(1)

    # Map EJScreen column names to our schema
    col_mapping = {}
    for our_col, candidates in COLUMN_MAP.items():
        found = find_column(df, candidates)
        if found:
            col_mapping[our_col] = found
        else:
            print(f"  Warning: '{our_col}' not found (tried: {candidates}). Will store NULL.")

    if not col_mapping:
        print("Error: no EJScreen indicator columns found. Check the file format.")
        sys.exit(1)

    print(f"  Indicator columns mapped: {list(col_mapping.keys())}")
    print()

    db.init_db()

    updated = 0
    skipped = 0
    errors = 0

    for _, row in df.iterrows():
        tract_id = normalize_tract_id(row.get(tract_col))
        if not tract_id or len(tract_id) != 11:
            skipped += 1
            continue

        # Build update values dict — only include columns we found
        update_vals = {}
        for our_col, file_col in col_mapping.items():
            raw = row.get(file_col)
            try:
                if raw is None or (isinstance(raw, float) and pd.isna(raw)) or str(raw).strip() in ("", "None", "nan"):
                    update_vals[our_col] = None
                else:
                    update_vals[our_col] = round(float(str(raw).strip()), 1)
            except (ValueError, TypeError):
                update_vals[our_col] = None

        if not any(v is not None for v in update_vals.values()):
            skipped += 1
            continue

        # Build a targeted UPDATE (only EJ columns, don't touch other census_tracts data)
        set_clauses = ", ".join(f"{col} = ?" for col in update_vals)
        values = list(update_vals.values()) + [tract_id]

        try:
            conn = db.get_connection()
            cur = conn.cursor()
            cur.execute(
                f"UPDATE census_tracts SET {set_clauses} WHERE census_tract_id = ?",
                values,
            )
            if cur.rowcount > 0:
                updated += 1
            else:
                skipped += 1   # tract not in DB yet
            conn.commit()
            conn.close()
        except Exception as e:
            errors += 1
            if errors <= 3:
                print(f"  Error updating tract {tract_id}: {e}")

        if (updated + skipped) % 5000 == 0 and updated > 0:
            print(f"  Progress: {updated:,} updated, {skipped:,} skipped...", end="\r")

    print()
    print(f"EJScreen load complete.")
    print(f"  Tracts updated: {updated:,}")
    print(f"  Tracts skipped (not in DB or no data): {skipped:,}")
    if errors:
        print(f"  Errors: {errors:,}")
    print()
    print("EJ Index and environmental indicators are now shown in the census tract context panel.")


if __name__ == "__main__":
    main()
