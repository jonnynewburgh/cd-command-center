"""
etl/load_cra_lending.py — Load FFIEC CRA small business lending flat files.

Why this data matters for CD deal origination:
    The CRA (Community Reinvestment Act) requires banks to lend in the communities
    where they take deposits. This data shows WHO is actually lending WHERE and HOW MUCH.
    Two file types:
      - D2-1 (Disclosure): Per-bank lending by census tract. Identifies which banks
        are active in a specific geography — key for sourcing investors and lenders.
      - A2-1 (Aggregate): All-banks total by census tract. Shows the overall lending
        volume in a market — useful for identifying credit deserts vs. competitive markets.

Field layout (reverse-engineered from FFIEC flat files):

    D2-1 (disclosure, per-bank, per-tract) — 145 chars per record:
      [0:4]   = table_id ('D2-1')
      [4]     = space
      [5:15]  = respondent_id (10 chars, FFIEC institution ID)
      [15:16] = agency_code (1=OCC, 2=FRB, 3=FDIC, 4=OTS, 5=NCUA)
      [16:20] = year (4 chars)
      [20:22] = '51' (constant record-type indicator)
      [22:24] = state_fips (2 chars, e.g. '13'=Georgia)
      [24:27] = county_fips (3 chars, '   ' for non-tract records)
      [27:32] = msa_code (5 chars, 'NA   ' if rural/non-metro)
      [32:36] = census_tract (4 chars, 'NA  ' if not tract-level)
      [36:38] = split_code (usually 'NN')
      [38:39] = loan_type ('S'=small biz, 'L'=community dev)
      [39:42] = row_code ('101'='total', '102'='to biz rev≤$1M',
                          '103'='≤$100K', '104'='$100-250K',
                          '105'='$250K-$1M', '106'='small farm')
      [42:45] = spaces (non-blank = county/state subtotal row, skipped)
      [45:145]= 10 numeric fields × 10 chars (amounts in $thousands)

    A2-1 (aggregate, all banks, per-tract) — 116 chars per record:
      [0:4]   = table_id ('A2-1')
      [4]     = space
      [5:9]   = year
      [9:11]  = '51' (constant)
      [11:13] = state_fips
      [13:16] = county_fips
      [16:21] = msa_code (5 chars)
      [21:28] = census_tract (7 chars with decimal, e.g. '0301.01', blank=subtotal)
      [28:30] = type_code (e.g. 'NS')
      [30:33] = row_code
      [33:36] = spaces (non-blank = subtotal row, skipped)
      [36:116]= 8 numeric fields × 10 chars (amounts in $thousands)

Usage:
    # Load all D2-1 and A2-1 files in the CRA data directory:
    python etl/load_cra_lending.py --dir data/raw/cra

    # Load only Georgia records for a specific year:
    python etl/load_cra_lending.py --dir data/raw/cra --year 2023 --states GA

    # Load only disclosure (D2-1) files:
    python etl/load_cra_lending.py --dir data/raw/cra --table d21

    # Load a single file explicitly:
    python etl/load_cra_lending.py --file data/raw/cra/cra2023_Discl_D21.dat --year 2023

    # Preview without inserting:
    python etl/load_cra_lending.py --dir data/raw/cra --year 2023 --dry-run
"""

import argparse
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import db

RAW_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "raw", "cra")

# State abbreviation → FIPS code (for --states filter)
STATE_FIPS = {
    "AL": "01", "AK": "02", "AZ": "04", "AR": "05", "CA": "06",
    "CO": "08", "CT": "09", "DE": "10", "DC": "11", "FL": "12",
    "GA": "13", "HI": "15", "ID": "16", "IL": "17", "IN": "18",
    "IA": "19", "KS": "20", "KY": "21", "LA": "22", "ME": "23",
    "MD": "24", "MA": "25", "MI": "26", "MN": "27", "MS": "28",
    "MO": "29", "MT": "30", "NE": "31", "NV": "32", "NH": "33",
    "NJ": "34", "NM": "35", "NY": "36", "NC": "37", "ND": "38",
    "OH": "39", "OK": "40", "OR": "41", "PA": "42", "RI": "44",
    "SC": "45", "SD": "46", "TN": "47", "TX": "48", "UT": "49",
    "VT": "50", "VA": "51", "WA": "53", "WV": "54", "WI": "55",
    "WY": "56", "PR": "72",
}

BATCH_SIZE = 5000


def infer_year_from_filename(filename: str) -> int | None:
    """
    Extract the year from CRA flat file names.

    Handles:
      cra2023_Discl_D21.dat  ->  2023
      04exp_discl_new.dat    ->  2004
      10exp_aggr.dat         ->  2010
    """
    base = os.path.basename(filename).lower()
    # New format: 4-digit year in name
    m = re.search(r"(\d{4})", base)
    if m:
        return int(m.group(1))
    # Old format: 2-digit year prefix like '04exp...'
    m = re.match(r"^(\d{2})exp", base)
    if m:
        yy = int(m.group(1))
        return (1900 + yy) if yy >= 96 else (2000 + yy)
    return None


def make_tract_id(state_fips: str, county_fips: str, census_tract: str, discl: bool) -> str | None:
    """
    Build the 11-digit census GEOID from FFIEC fields.

    D2-1 stores tract as 4 chars (e.g. '0301') — we append '00' for the decimal part.
    A2-1 stores tract as 7 chars with decimal (e.g. '0301.01') — we remove the decimal.
    """
    if not state_fips or not county_fips or not census_tract:
        return None
    s = state_fips.strip()
    c = county_fips.strip()
    t = census_tract.strip()
    if not s or not c or not t or t in ("NA", ""):
        return None
    if discl:
        # 4-char code → pad to 6 digits (append '00' for the decimal sub-tract)
        t6 = t.zfill(4) + "00"
    else:
        # 7-char 'TTTT.SS' → remove dot → 6 chars
        t6 = t.replace(".", "")
    if len(s) == 2 and len(c) == 3 and len(t6) == 6:
        return s + c + t6
    return None


def parse_d21_record(line: str, state_fips_set: set | None) -> dict | None:
    """
    Parse one D2-1 (small business disclosure) record.
    Returns a dict, or None if the record should be skipped.

    We skip:
      - Lines that are not D2-1 records (pre-2014 combined files contain multiple table types)
      - Records with blank county (county/state subtotals)
      - Records with blank census tract (not tract-level)
      - Records where the padding [42:45] is non-blank (another subtotal indicator)
      - Records not in the requested states
    """
    if len(line) < 145:
        return None
    if line[:4] != "D2-1":
        return None

    county = line[24:27].strip()
    tract = line[32:36].strip()
    padding = line[42:45].strip()

    # Skip county/state subtotals
    if not county or not tract or tract in ("NA", "") or padding:
        return None

    state = line[22:24].strip()
    if state_fips_set and state not in state_fips_set:
        return None

    respondent_id = line[5:15].strip()
    agency_code   = line[15:16].strip()
    year          = line[16:20].strip()
    msa_code      = line[27:32].strip()
    loan_type     = line[38:39].strip()
    row_code      = line[39:42].strip()

    # Parse 10 numeric fields (10 chars each, starting at [45])
    nums = []
    for i in range(10):
        start = 45 + i * 10
        try:
            nums.append(int(line[start:start + 10].strip() or "0"))
        except ValueError:
            nums.append(0)

    tract_id = make_tract_id(state, county, tract, discl=True)

    return {
        "respondent_id": respondent_id,
        "agency_code":   agency_code,
        "year":          int(year) if year.isdigit() else None,
        "state_fips":    state,
        "county_fips":   county,
        "msa_code":      msa_code or "",
        "census_tract":  tract,
        "census_tract_id": tract_id,
        "row_code":      row_code,
        "loan_type":     loan_type,
        "n_total":       nums[0],
        "amt_total":     nums[1],
        "n_small_biz":   nums[2],
        "amt_small_biz": nums[3],
        "n_orig":        nums[4],
        "amt_orig":      nums[5],
        "n_orig_sb":     nums[6],
        "amt_orig_sb":   nums[7],
        "n_purch":       nums[8],
        "amt_purch":     nums[9],
    }


def parse_a21_record(line: str, state_fips_set: set | None) -> dict | None:
    """
    Parse one A2-1 (small business aggregate) record.
    Returns a dict, or None if the record should be skipped.

    We skip:
      - Lines that are not A2-1 records (pre-2014 combined files contain multiple table types)
      - Records with blank census tract (county/state subtotals)
      - Records where [33:36] is non-blank (another subtotal type)
      - Records not in the requested states
    """
    if len(line) < 116:
        return None
    if line[:4] != "A2-1":
        return None

    tract = line[21:28].strip()
    padding = line[33:36].strip()

    # Skip subtotals (blank tract or non-blank aggregate indicator)
    if not tract or padding:
        return None

    state = line[11:13].strip()
    if state_fips_set and state not in state_fips_set:
        return None

    year     = line[5:9].strip()
    county   = line[13:16].strip()
    msa_code = line[16:21].strip()
    row_code = line[30:33].strip()

    # Parse 8 numeric fields (10 chars each, starting at [36])
    nums = []
    for i in range(8):
        start = 36 + i * 10
        try:
            nums.append(int(line[start:start + 10].strip() or "0"))
        except ValueError:
            nums.append(0)

    tract_id = make_tract_id(state, county, tract, discl=False)

    return {
        "year":          int(year) if year.isdigit() else None,
        "state_fips":    state,
        "county_fips":   county,
        "msa_code":      msa_code or "",
        "census_tract":  tract,
        "census_tract_id": tract_id,
        "row_code":      row_code,
        "n_orig":        nums[0],
        "amt_orig":      nums[1],
        "n_orig_sb":     nums[2],
        "amt_orig_sb":   nums[3],
        "n_prev":        nums[4],
        "amt_prev":      nums[5],
        "n_prev_sb":     nums[6],
        "amt_prev_sb":   nums[7],
    }


def load_file(filepath: str, table: str, year: int | None, state_fips_set: set | None,
              dry_run: bool) -> int:
    """
    Parse and load one D2-1 or A2-1 file.
    Returns the number of rows inserted.
    """
    is_discl = table == "cra_sb_discl"
    parse_fn = parse_d21_record if is_discl else parse_a21_record

    print(f"  Reading: {os.path.basename(filepath)}")

    conn = db.get_connection()
    is_pg = db._IS_POSTGRES

    # For PostgreSQL we can use execute_values for fast bulk inserts
    if is_pg:
        import psycopg2.extras

    total = 0
    batch = []

    def flush(batch):
        if not batch or dry_run:
            return
        if is_discl:
            cols = [
                "respondent_id", "agency_code", "year", "state_fips", "county_fips",
                "msa_code", "census_tract", "census_tract_id", "row_code", "loan_type",
                "n_total", "amt_total", "n_small_biz", "amt_small_biz",
                "n_orig", "amt_orig", "n_orig_sb", "amt_orig_sb",
                "n_purch", "amt_purch",
            ]
        else:
            cols = [
                "year", "state_fips", "county_fips", "msa_code", "census_tract",
                "census_tract_id", "row_code",
                "n_orig", "amt_orig", "n_orig_sb", "amt_orig_sb",
                "n_prev", "amt_prev", "n_prev_sb", "amt_prev_sb",
            ]

        rows_as_tuples = [tuple(r[c] for c in cols) for r in batch]
        col_list = ", ".join(cols)
        placeholders = ", ".join(["%s"] * len(cols))

        if is_pg:
            cur = conn.cursor()
            psycopg2.extras.execute_values(
                cur,
                f"INSERT INTO {table} ({col_list}) VALUES %s ON CONFLICT DO NOTHING",
                rows_as_tuples,
            )
            conn.commit()
        else:
            cur = conn.cursor()
            q_marks = ", ".join(["?"] * len(cols))
            cur.executemany(
                f"INSERT OR IGNORE INTO {table} ({col_list}) VALUES ({q_marks})",
                rows_as_tuples,
            )
            conn.commit()

    with open(filepath, "r", encoding="latin-1") as f:
        for line in f:
            line = line.rstrip("\n")
            rec = parse_fn(line, state_fips_set)
            if rec is None:
                continue
            if year and rec.get("year") != year:
                continue
            batch.append(rec)
            total += 1
            if len(batch) >= BATCH_SIZE:
                flush(batch)
                batch = []
                print(f"    {total:,} rows processed...", end="\r")

    flush(batch)
    print(f"    {total:,} rows processed.  ")
    return total


def find_cra_lending_files(directory: str, table_types: list[str]) -> list[tuple[str, str, int | None]]:
    """
    Scan directory for D2-1 and A2-1 files.
    Returns list of (filepath, table_name, year).

    Strict matching so we don't accidentally load D1-1, A1-1, etc.:
      D2-1 files: must contain '_d21' or 'd21.' in the lowercased filename
      A2-1 files: must contain '_a21' or 'a21.' (but not 'a21a')
    """
    results = []

    # Regex patterns — must match specifically D21 or A21, not D11/A11/etc.
    patterns = []
    if "d21" in table_types:
        # e.g. 'cra2023_discl_d21.dat' or '23discld21.dat'
        patterns.append((re.compile(r"(discl_d21|discld21|_d21\b|_d21\.)", re.I), "cra_sb_discl"))
    if "a21" in table_types:
        # e.g. 'cra2023_aggr_a21.dat' but NOT 'cra2023_aggr_a21a.dat'
        patterns.append((re.compile(r"(aggr_a21|aggra21|_a21\b|_a21\.)", re.I), "cra_sb_aggr"))

    # Pre-2014 combined files (e.g. '04exp_discl_new.dat', '10exp_discl.dat')
    # These contain ALL table types mixed together; the parsers filter by [0:4] prefix.
    # Only process 2004+ files — the 2000-2003 files use a shorter 114-char format.
    old_discl = re.compile(r"^(?:0[4-9]|1[0-3])exp_discl", re.I)
    old_aggr  = re.compile(r"^(?:0[4-9]|1[0-3])exp_aggr", re.I)

    for fname in sorted(os.listdir(directory)):
        if not fname.lower().endswith(".dat"):
            continue
        fl = fname.lower()

        # Skip 'a21a' variant (small-institution supplement — different format)
        if "a21a" in fl:
            continue

        # New-style per-table files (2014+)
        matched = False
        for pattern, db_table in patterns:
            if pattern.search(fl):
                year = infer_year_from_filename(fname)
                results.append((os.path.join(directory, fname), db_table, year))
                matched = True
                break

        if matched:
            continue

        # Old-style combined files (2004-2013) — both parsers will filter by table_id
        if "d21" in table_types and old_discl.match(fl):
            year = infer_year_from_filename(fname)
            results.append((os.path.join(directory, fname), "cra_sb_discl", year))
        elif "a21" in table_types and old_aggr.match(fl):
            year = infer_year_from_filename(fname)
            results.append((os.path.join(directory, fname), "cra_sb_aggr", year))

    return results


def main():
    parser = argparse.ArgumentParser(
        description="Load FFIEC CRA small business lending data (D2-1 and A2-1 flat files)"
    )
    parser.add_argument(
        "--dir",
        default=RAW_DIR,
        help=f"Directory containing CRA .dat files (default: {RAW_DIR})",
    )
    parser.add_argument(
        "--file",
        default=None,
        help="Load a single file directly (overrides --dir).",
    )
    parser.add_argument(
        "--year",
        type=int,
        default=None,
        help="Filter to a specific data year (e.g. 2023). Without this, all years in --dir are loaded.",
    )
    parser.add_argument(
        "--states",
        nargs="+",
        metavar="STATE",
        default=None,
        help="Filter to specific states by abbreviation (e.g. GA TX NY). Default: all states.",
    )
    parser.add_argument(
        "--table",
        choices=["d21", "a21", "all"],
        default="all",
        help="Which table type to load: d21 (disclosure/per-bank), a21 (aggregate/all-banks), or all (default).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Parse files and count records without writing to the database.",
    )
    args = parser.parse_args()

    # Resolve state FIPS codes from abbreviations
    state_fips_set = None
    if args.states:
        state_fips_set = set()
        for abbr in args.states:
            fips = STATE_FIPS.get(abbr.upper())
            if fips:
                state_fips_set.add(fips)
            else:
                print(f"  Warning: unknown state abbreviation '{abbr}', skipping")

    table_types = ["d21", "a21"] if args.table == "all" else [args.table]

    print("CD Command Center — FFIEC CRA Lending Load")
    if args.states:
        print(f"  States filter: {', '.join(args.states)}")
    if args.year:
        print(f"  Year filter: {args.year}")
    if args.dry_run:
        print("  DRY RUN — no data will be written")
    print()

    if not args.dry_run:
        db.init_db()

    # --- Single file mode ---
    if args.file:
        if not os.path.exists(args.file):
            print(f"Error: file not found: {args.file}")
            sys.exit(1)
        fl = args.file.lower()
        if any(p in fl for p in ["discl_d21", "discld21", "d21"]):
            table = "cra_sb_discl"
        elif any(p in fl for p in ["aggr_a21", "aggra21", "a21"]):
            table = "cra_sb_aggr"
        else:
            print("Error: cannot determine table type from filename. Use --table d21 or --table a21.")
            sys.exit(1)
        file_year = args.year or infer_year_from_filename(args.file)
        n = load_file(args.file, table, args.year, state_fips_set, args.dry_run)
        print(f"\nDone. {n:,} rows {'counted' if args.dry_run else 'inserted'}.")
        return

    # --- Directory scan mode ---
    if not os.path.isdir(args.dir):
        print(f"Error: directory not found: {args.dir}")
        sys.exit(1)

    files = find_cra_lending_files(args.dir, table_types)

    if args.year:
        files = [(fp, tbl, yr) for fp, tbl, yr in files if yr == args.year]

    if not files:
        print(f"No matching CRA lending files found in {args.dir}")
        print("Expected files named like: cra2023_Discl_D21.dat, cra2023_Aggr_A21.dat")
        sys.exit(1)

    print(f"Found {len(files)} file(s) to process:")
    for fp, tbl, yr in files:
        print(f"  [{yr}] {os.path.basename(fp)} -> {tbl}")
    print()

    grand_total = 0
    errors = []
    for filepath, table, year in files:
        try:
            n = load_file(filepath, table, args.year, state_fips_set, args.dry_run)
            grand_total += n
            print(f"  -> {n:,} rows {'counted' if args.dry_run else 'inserted'}")
        except Exception as e:
            errors.append((os.path.basename(filepath), str(e)))
            print(f"  -> ERROR: {e}")

    print()
    print(f"Total: {grand_total:,} rows {'counted' if args.dry_run else 'inserted'}")

    if errors:
        print(f"\nErrors ({len(errors)} files failed):")
        for fname, err in errors:
            print(f"  {fname}: {err}")
        sys.exit(1)


if __name__ == "__main__":
    main()
