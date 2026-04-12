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
  EPA publishes the national EJScreen dataset annually as a CSV.
  Download from: https://gaftp.epa.gov/EJSCREEN/
  The most recent version (2023) is:
    https://gaftp.epa.gov/EJSCREEN/2023/EJSCREEN_2023_Tracts_with_AS_CNMI_GU_VI.csv.zip

  Unzip the file, then pass it to this script with --file.
  Or use --auto to download and unzip automatically.

The file is large (~800MB uncompressed). Use --states to load a subset.

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
    python etl/load_ejscreen.py --auto                              # download + load
    python etl/load_ejscreen.py --auto --states CA TX NY           # download + load subset
    python etl/load_ejscreen.py --file data/raw/EJSCREEN_2023_Tracts.csv
    python etl/load_ejscreen.py --file data/raw/EJSCREEN_2023_Tracts.csv --states CA TX NY
    python etl/load_ejscreen.py --file data/raw/EJSCREEN_2023_Tracts.csv --columns-only
"""

import argparse
import sys
import os
import io
import struct
import zipfile
import requests

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import db

# Column name candidates for each indicator.
# EJScreen has changed column names across releases; we try all known names.
# Format: {our_column: [list of candidate column names in EPA file]}
#
# Key naming changes across releases:
#   2020 (block group): P_PM25, P_LDPNT, P_PNPL, P_PWDIS, P_DSLPM; no composite EJ percentile
#   2021-2022 (tracts):  P_PM25, P_LDPNT, P_PNPL, P_PWDIS, P_DSLPM; P_VULEOPCT = EJ index pctile
#   2023+ (tracts):      EJ_PCTILE_D2_PM25, EJ_D1_PCTILE, etc.
COLUMN_MAP = {
    "ej_index":               ["EJ_D1_PCTILE", "EJ_PCTILE", "P_VULEOPCT", "EJINDEX", "EJSCORE"],
    "pm25_percentile":        ["EJ_PCTILE_D2_PM25",   "PM25_EJ_PCTILE",   "P_PM25",    "PM25PCTL"],
    "diesel_percentile":      ["EJ_PCTILE_D5_DIESEL",  "DIESEL_EJ_PCTILE", "P_DSLPM",  "P_DIESEL", "DSLPM_PCTILE"],
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


# Zenodo archive of EJScreen data (EPA took the tool offline Feb 2025).
# Each outer zip contains an inner CSV zip stored uncompressed (method=STORED),
# so we can range-fetch just the CSV portion without downloading the full archive.
# Format: (year, outer_zip_url, inner_csv_zip_path, outer_size_mb, csv_mb)
#
# We prefer the 2022 tract-level file (35 MB) because:
#   - It uses 11-digit tract IDs (not 12-digit block-group IDs like the 2020 USPR file)
#   - It has all 6 indicator columns including P_VULEOPCT (EJ index) and P_DSLPM (diesel)
#   - It covers the contiguous US + AS, CNMI, GU, VI territories
EJSCREEN_SOURCES = [
    (
        "2022",
        "https://zenodo.org/records/14767363/files/2022.zip?download=1",
        "2022/EJSCREEN_2022_Full_with_AS_CNMI_GU_VI_Tracts.csv.zip",
        3_337,   # outer zip size (MB)
        35,      # inner csv.zip size (MB)
    ),
    (
        "2021",
        "https://zenodo.org/records/14767363/files/2021.zip?download=1",
        "2021/EJSCREEN_2021_USPR_Tracts.csv.zip",
        2_240,
        60,
    ),
]

RAW_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "raw")


def _parse_zip_central_directory(url: str) -> dict:
    """
    Fetch and parse the ZIP central directory from a remote URL using HTTP Range requests.
    Returns a dict mapping filename → (local_header_offset, compressed_size, method).
    Only requires fetching the last ~64KB of the remote file.
    """
    # Fetch last 64KB to locate the End-of-Central-Directory record
    resp = requests.get(url, headers={"Range": "bytes=-65536"}, timeout=60)
    resp.raise_for_status()
    tail = resp.content

    eocd_pos = tail.rfind(b'\x50\x4b\x05\x06')
    if eocd_pos == -1:
        raise RuntimeError("Could not find ZIP EOCD record — file may not be a valid ZIP")

    cd_offset = struct.unpack_from('<I', tail, eocd_pos + 16)[0]
    cd_size   = struct.unpack_from('<I', tail, eocd_pos + 12)[0]

    resp2 = requests.get(url, headers={"Range": f"bytes={cd_offset}-{cd_offset + cd_size - 1}"}, timeout=60)
    resp2.raise_for_status()
    cd_data = resp2.content

    entries = {}
    pos = 0
    while pos < len(cd_data):
        if cd_data[pos:pos + 4] != b'\x50\x4b\x01\x02':
            break
        method  = struct.unpack_from('<H', cd_data, pos + 10)[0]
        c_size  = struct.unpack_from('<I', cd_data, pos + 20)[0]
        fn_len  = struct.unpack_from('<H', cd_data, pos + 28)[0]
        ex_len  = struct.unpack_from('<H', cd_data, pos + 30)[0]
        co_len  = struct.unpack_from('<H', cd_data, pos + 32)[0]
        lh_off  = struct.unpack_from('<I', cd_data, pos + 42)[0]
        fname   = cd_data[pos + 46:pos + 46 + fn_len].decode('utf-8', errors='replace')
        entries[fname] = (lh_off, c_size, method)
        pos += 46 + fn_len + ex_len + co_len

    return entries


def _range_fetch_stored_entry(url: str, local_header_offset: int, compressed_size: int) -> bytes:
    """
    Fetch a STORED (uncompressed) ZIP entry from a remote file using a Range request.
    Reads the local file header to find where the data bytes begin, then fetches them.
    """
    # Local file header: 30 bytes fixed + variable filename + extra field
    lh_resp = requests.get(url, headers={"Range": f"bytes={local_header_offset}-{local_header_offset + 29}"}, timeout=60)
    lh_resp.raise_for_status()
    lh = lh_resp.content
    fn_len = struct.unpack_from('<H', lh, 26)[0]
    ex_len = struct.unpack_from('<H', lh, 28)[0]
    data_start = local_header_offset + 30 + fn_len + ex_len
    data_end   = data_start + compressed_size - 1

    # Stream the data with progress reporting
    downloaded = 0
    chunks = []
    with requests.get(url, headers={"Range": f"bytes={data_start}-{data_end}"}, stream=True, timeout=120) as resp:
        resp.raise_for_status()
        for chunk in resp.iter_content(chunk_size=1024 * 1024):
            chunks.append(chunk)
            downloaded += len(chunk)
            pct = downloaded / compressed_size * 100
            print(f"  Downloading EJScreen CSV: {downloaded/1e6:.0f} MB / {compressed_size/1e6:.0f} MB ({pct:.0f}%)", end="\r")
    print()
    return b"".join(chunks)


def auto_download() -> str:
    """
    Download just the EJScreen national CSV (~135 MB) from the Zenodo archive using
    HTTP Range requests, without downloading the full outer zip (~1.2 GB).

    The Zenodo archive stores the inner CSV zip uncompressed (STORED method), so we
    can fetch just the relevant byte range and open it directly as a ZIP.

    Returns the local path to the extracted CSV. Skips download if already cached.
    Raises RuntimeError if the download fails.
    """
    import struct

    os.makedirs(RAW_DIR, exist_ok=True)

    for year, outer_url, inner_path, outer_mb, csv_mb in EJSCREEN_SOURCES:
        # Derive the local CSV name from the inner zip path (strip directory prefix and .zip suffix)
        csv_name = os.path.basename(inner_path).replace(".zip", "")
        csv_path = os.path.join(RAW_DIR, csv_name)

        if os.path.exists(csv_path):
            print(f"  Using cached file: {csv_path}")
            return csv_path

        print(f"  EJScreen {year} — fetching file listing from Zenodo archive ({outer_mb:,} MB outer zip)...")
        try:
            entries = _parse_zip_central_directory(outer_url)
        except Exception as e:
            print(f"  Could not read ZIP directory: {e}")
            continue

        if inner_path not in entries:
            print(f"  '{inner_path}' not found in archive. Available: {list(entries.keys())}")
            continue

        lh_offset, comp_size, method = entries[inner_path]
        if method != 0:  # 0 = STORED
            print(f"  Warning: inner zip uses compression method {method} (expected STORED=0). Falling back to full download.")
            continue

        print(f"  Fetching inner CSV zip via HTTP Range ({comp_size/1e6:.0f} MB of {outer_mb:,} MB)...")
        try:
            inner_zip_bytes = _range_fetch_stored_entry(outer_url, lh_offset, comp_size)
        except Exception as e:
            print(f"  Range fetch failed: {e}")
            continue

        print(f"  Extracting {csv_name} from inner zip...")
        try:
            with zipfile.ZipFile(io.BytesIO(inner_zip_bytes)) as inner_zf:
                csv_members = [m for m in inner_zf.namelist() if m.lower().endswith(".csv")]
                if not csv_members:
                    raise RuntimeError(f"No CSV found in inner zip. Contents: {inner_zf.namelist()}")
                with inner_zf.open(csv_members[0]) as src, open(csv_path, "wb") as dst:
                    dst.write(src.read())
        except Exception as e:
            print(f"  Extraction failed: {e}")
            if os.path.exists(csv_path):
                os.remove(csv_path)
            continue

        print(f"  Extracted to: {csv_path}")
        return csv_path

    raise RuntimeError("Could not download EJScreen data from any known source.")


def main():
    parser = argparse.ArgumentParser(
        description="Load EPA EJScreen environmental justice indicators into census_tracts"
    )
    file_group = parser.add_mutually_exclusive_group(required=True)
    file_group.add_argument(
        "--auto",
        action="store_true",
        help="Auto-download the EJScreen CSV from EPA (recommended). "
             "Skips download if the file is already cached in data/raw/.",
    )
    file_group.add_argument(
        "--file",
        help="Path to a manually downloaded EJScreen CSV.",
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
    args = parser.parse_args()

    if args.auto:
        try:
            args.file = auto_download()
        except RuntimeError as e:
            print(f"Error: {e}")
            sys.exit(1)

    if not os.path.exists(args.file):
        print(f"Error: file not found: {args.file}")
        sys.exit(1)

    print(f"CD Command Center — EJScreen Load")
    print(f"  File: {args.file}")
    print(f"  Reading file (may take a moment for the full national file)...")

    # Read with low_memory=False to avoid dtype inference issues on large file
    df = pd.read_csv(args.file, dtype=str, low_memory=False)
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
