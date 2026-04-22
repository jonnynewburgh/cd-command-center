"""
etl/load_nmtc_coalition.py — Load the NMTC Coalition Transaction Level Report
and match records to the existing nmtc_projects table.

Data source: NMTC Coalition Transaction Level Report (Excel)
  https://nmtccoalition.org/nmtc-fact-sheet/
  Public data, no commercial use restrictions.

The Coalition's project database is more detailed than the CDFI Fund public data.
It includes project addresses, total project costs, jobs created/retained, and
project type classifications that the CDFI Fund aggregate data often omits.

Matching strategy (in priority order):
  1. CDFI Fund Project ID — if the Coalition row includes a CDFI project ID that
     exists in our nmtc_projects table, accept it as an exact match (score=1.0).
  2. CDE name + state + year window + QLICI amount — fuzzy CDE name similarity
     (difflib) >= 0.75 AND same state AND |year difference| <= 1 AND amount within 25%.
     Score = average of name similarity and amount proximity.
  3. CDE name + state + year window (no amount) — when QLICI amount is missing
     in either source.  Score capped at 0.80.

Unmatched Coalition rows are still loaded — they have independent value as a
project database even without a link to the CDFI Fund data.

Usage:
    # Load from a local Excel file
    python etl/load_nmtc_coalition.py --file data/raw/nmtc_transaction_report_2024.xlsx

    # Preview column names without loading
    python etl/load_nmtc_coalition.py --file data/raw/nmtc_transaction_report_2024.xlsx --columns-only

    # Dry run: load + match but don't write to DB
    python etl/load_nmtc_coalition.py --file data/raw/nmtc_transaction_report_2024.xlsx --dry-run

    # Skip the matching step (just load Coalition records as-is)
    python etl/load_nmtc_coalition.py --file data/raw/nmtc_transaction_report_2024.xlsx --no-match

    # Re-run matching on already-loaded Coalition records
    python etl/load_nmtc_coalition.py --match-only

Column mapping:
    Coalition column names change between report versions.  This script tries a
    list of known aliases for each field; run --columns-only to see what your
    file contains and add aliases below if needed.

    COALITION_COL_ALIASES maps our internal field names to lists of possible
    Coalition column name variants (case-insensitive, whitespace-normalised).
"""

import argparse
import os
import sys
import re
from difflib import SequenceMatcher

import pandas as pd
import numpy as np

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import db

# ---------------------------------------------------------------------------
# Column name aliases — add new variants here as Coalition report format changes
# ---------------------------------------------------------------------------

COALITION_COL_ALIASES = {
    # Our field           # Coalition column name variants (lowercase, normalised)
    "coalition_txn_id":   ["transaction id", "txn id", "transaction_id"],
    "cdfi_project_id":    ["cdfi project id", "cdfi_project_id", "project id", "cdfi id"],
    "project_name":       ["project name", "project", "deal name", "borrower name",
                           "purpose of investment"],
    "cde_name":           ["cde name", "cde", "allocatee", "allocatee name",
                           "community development entity",
                           "community development entity (cde) name"],
    "address":            ["address", "street address", "project address"],
    "city":               ["city", "project city"],
    "state":              ["state", "project state", "st"],
    "zip_code":           ["zip", "zip code", "postal code"],
    "census_tract_id":    ["census tract", "tract", "census tract id", "fips tract",
                           "2020 census tract"],
    "total_project_costs":["total project costs", "total project cost", "total costs",
                           "project cost", "total investment"],
    "nmtc_allocation_used":["nmtc allocation", "nmtc allocation used", "qlici amount",
                            "qlici", "allocation amount", "qualified low-income community investment"],
    "jobs_created":       ["jobs created", "permanent jobs created", "perm jobs created",
                           "construction jobs", "new jobs"],
    "jobs_retained":      ["jobs retained", "jobs saved"],
    "project_type":       ["project type", "type", "category", "real estate or operating business",
                           "qalicb type"],
    "investment_year":    ["investment year", "year", "fiscal year", "closing year",
                           "transaction year", "year closed", "origination year"],
}


def _norm_col(name: str) -> str:
    """Normalize a column name for alias matching."""
    return re.sub(r"\s+", " ", str(name).strip().lower())


def _find_col(df: pd.DataFrame, aliases: list) -> str | None:
    """Return the first DataFrame column that matches any alias."""
    norm_map = {_norm_col(c): c for c in df.columns}
    for alias in aliases:
        if alias in norm_map:
            return norm_map[alias]
    return None


def map_columns(df: pd.DataFrame) -> dict:
    """
    Return a dict mapping our internal field names to actual DataFrame column names.
    Fields with no matching column are mapped to None.
    """
    mapping = {}
    for field, aliases in COALITION_COL_ALIASES.items():
        mapping[field] = _find_col(df, aliases)
    return mapping


def _clean_amount(val) -> float | None:
    """Convert '$50,000,000' or '50000000.0' to float, or None if unparseable."""
    if val is None or (isinstance(val, float) and np.isnan(val)):
        return None
    s = str(val).replace(",", "").replace("$", "").strip()
    try:
        return float(s)
    except (ValueError, TypeError):
        return None


def _clean_int(val) -> int | None:
    try:
        f = float(val)
        return int(f) if not np.isnan(f) else None
    except (ValueError, TypeError):
        return None


def _norm_cde(name: str) -> str:
    """Normalise a CDE name for fuzzy comparison."""
    name = str(name or "").lower().strip()
    # Remove common suffixes
    for suffix in (", llc", " llc", ", inc.", ", inc", " inc.", " inc",
                   ", l.l.c.", ", l.l.c", " fund", " capital"):
        if name.endswith(suffix):
            name = name[:-len(suffix)].strip()
    return name


# ---------------------------------------------------------------------------
# Matching logic
# ---------------------------------------------------------------------------

def build_project_index() -> list:
    """
    Load all nmtc_projects from DB into a list of dicts for matching.
    Each dict: {id, cdfi_project_id, cde_name_norm, state, fiscal_year, qlici_amount}
    """
    conn = db.get_connection()
    df = pd.read_sql_query(
        """SELECT id, cdfi_project_id, cde_name, state, fiscal_year, qlici_amount
           FROM nmtc_projects""",
        conn,
    )
    conn.close()
    index = []
    for _, row in df.iterrows():
        index.append({
            "id":               int(row["id"]),
            "cdfi_project_id":  row.get("cdfi_project_id"),
            "cde_name_norm":    _norm_cde(row.get("cde_name", "")),
            "state":            str(row.get("state", "") or "").upper(),
            "year":             row.get("fiscal_year"),
            "amount":           row.get("qlici_amount"),
        })
    return index


def match_project(rec: dict, index: list) -> tuple[int | None, float]:
    """
    Try to match a Coalition record against nmtc_projects.
    Returns (nmtc_project_id, confidence) or (None, 0.0).

    rec keys: cdfi_project_id, cde_name, state, investment_year, nmtc_allocation_used
    """
    # --- Strategy 1: exact CDFI project ID ---
    if rec.get("cdfi_project_id"):
        for proj in index:
            if proj["cdfi_project_id"] and proj["cdfi_project_id"] == rec["cdfi_project_id"]:
                return proj["id"], 1.0

    cde_norm  = _norm_cde(rec.get("cde_name", ""))
    state     = str(rec.get("state", "") or "").upper()
    year      = rec.get("investment_year")
    amount    = _clean_amount(rec.get("nmtc_allocation_used"))

    if not cde_norm or not state:
        return None, 0.0

    best_id    = None
    best_score = 0.0

    for proj in index:
        # Must match state
        if proj["state"] != state:
            continue
        # Year window ±1 (allow for fiscal vs calendar year difference)
        if year is not None and proj["year"] is not None:
            if abs(int(year) - int(proj["year"])) > 1:
                continue

        # --- CDE name similarity ---
        name_sim = SequenceMatcher(None, cde_norm, proj["cde_name_norm"]).ratio()
        if name_sim < 0.75:
            continue

        # --- Strategy 2: name + amount proximity ---
        if amount is not None and proj["amount"] is not None and proj["amount"] > 0:
            ratio = min(amount, proj["amount"]) / max(amount, proj["amount"])
            if ratio < 0.75:  # amounts differ by more than 25% → skip
                continue
            score = (name_sim + ratio) / 2
        else:
            # --- Strategy 3: name + year only (no amount) ---
            score = min(name_sim, 0.80)

        if score > best_score:
            best_score = score
            best_id = proj["id"]

    return best_id, round(best_score, 3)


# ---------------------------------------------------------------------------
# Main ETL logic
# ---------------------------------------------------------------------------

def load_file(filepath: str, columns_only: bool, dry_run: bool,
              no_match: bool, match_threshold: float):
    print(f"Reading: {filepath}")

    # Determine file type
    ext = os.path.splitext(filepath)[1].lower()
    if ext in (".xlsx", ".xls"):
        # Try reading the first non-header sheet; some Coalition files have a cover sheet
        xl = pd.ExcelFile(filepath)
        sheet = xl.sheet_names[0]
        # Prefer a sheet with "project" or "transaction" in the name
        for s in xl.sheet_names:
            if any(kw in s.lower() for kw in ("project", "transaction", "data")):
                sheet = s
                break
        print(f"  Reading sheet: {sheet}")
        df = pd.read_excel(filepath, sheet_name=sheet, dtype=str)
    elif ext == ".csv":
        df = pd.read_csv(filepath, dtype=str, low_memory=False)
    else:
        print(f"ERROR: unsupported file type {ext} — expected .xlsx, .xls, or .csv")
        sys.exit(1)

    # Normalize column names
    df.columns = [_norm_col(c) for c in df.columns]

    if columns_only:
        print(f"\n{len(df.columns)} columns in {filepath}:")
        for c in df.columns:
            print(f"  {c}")
        return

    print(f"  {len(df)} rows")

    col_map = map_columns(df)
    unmapped = [f for f, c in col_map.items() if c is None]
    if unmapped:
        print(f"  Note: could not find columns for: {unmapped}")
        print("  These fields will be NULL. Add aliases to COALITION_COL_ALIASES if needed.")

    # Build project index for matching
    if not no_match:
        print("Building nmtc_projects index for matching...")
        index = build_project_index()
        print(f"  {len(index)} projects in DB")
    else:
        index = []

    loaded = matched = unmatched = errors = 0

    for i, row in df.iterrows():
        def get(field):
            col = col_map.get(field)
            return row.get(col, "").strip() if col else None

        project_name    = get("project_name")
        cde_name        = get("cde_name")
        state           = (get("state") or "").upper()[:2]
        investment_year = _clean_int(get("investment_year"))

        if not cde_name and not project_name:
            continue  # skip blank rows

        # Build a stable ID: prefer transaction ID, then project ID, then row index
        raw_id = get("coalition_txn_id") or get("cdfi_project_id") or f"coalition-{i}"
        coalition_project_id = str(raw_id).strip()

        rec = {
            "coalition_project_id": coalition_project_id,
            "cdfi_project_id":      get("cdfi_project_id") or None,
            "project_name":         project_name or None,
            "cde_name":             cde_name or None,
            "address":              get("address") or None,
            "city":                 get("city") or None,
            "state":                state or None,
            "zip_code":             get("zip_code") or None,
            "census_tract_id":      get("census_tract_id") or None,
            "total_project_costs":  _clean_amount(get("total_project_costs")),
            "nmtc_allocation_used": _clean_amount(get("nmtc_allocation_used")),
            "jobs_created":         _clean_int(get("jobs_created")),
            "jobs_retained":        _clean_int(get("jobs_retained")),
            "project_type":         get("project_type") or None,
            "investment_year":      investment_year,
            "nmtc_project_id":      None,
            "match_confidence":     None,
        }

        # Matching
        if not no_match:
            match_rec = {
                "cdfi_project_id":      rec["cdfi_project_id"],
                "cde_name":             rec["cde_name"],
                "state":                rec["state"],
                "investment_year":      rec["investment_year"],
                "nmtc_allocation_used": rec["nmtc_allocation_used"],
            }
            proj_id, conf = match_project(match_rec, index)
            if conf >= match_threshold:
                rec["nmtc_project_id"]  = proj_id
                rec["match_confidence"] = conf
                matched += 1
            else:
                unmatched += 1
        else:
            unmatched += 1

        if dry_run:
            status = f"→ project_id={rec['nmtc_project_id']} (conf={rec['match_confidence']:.2f})" \
                     if rec["nmtc_project_id"] else "→ no match"
            name   = (cde_name or project_name or "")[:50]
            print(f"  [{state} {investment_year}] {name:<50}  {status}")
        else:
            try:
                db.upsert_nmtc_coalition_project(rec)
                loaded += 1
            except Exception as e:
                print(f"  DB error row {i}: {e}")
                errors += 1

    if not dry_run and matched > 0:
        print("Backfilling nmtc_projects.coalition_id...")
        updated = db.link_nmtc_coalition_to_projects()
        print(f"  Updated {updated} nmtc_projects rows")

    total = matched + unmatched
    print(f"\n{'DRY RUN — ' if dry_run else ''}Results:")
    print(f"  Rows processed: {total}")
    print(f"  Loaded to DB:   {loaded}")
    if not no_match:
        pct = 100 * matched / total if total else 0
        print(f"  Matched:        {matched}  ({pct:.0f}%)")
        print(f"  Unmatched:      {unmatched}  (loaded with nmtc_project_id=NULL)")
    if errors:
        print(f"  DB errors:      {errors}")


def run_match_only(threshold: float):
    """Re-run matching on already-loaded Coalition records and update links."""
    print("Loading Coalition records from DB...")
    df = db.get_nmtc_coalition_projects(limit=None)
    if df.empty:
        print("No Coalition records found. Run without --match-only first.")
        return

    print(f"  {len(df)} Coalition records")
    print("Building nmtc_projects index...")
    index = build_project_index()
    print(f"  {len(index)} DB projects")

    updates = 0
    conn = db.get_connection()
    cur  = conn.cursor()

    for _, row in df.iterrows():
        match_rec = {
            "cdfi_project_id":      row.get("cdfi_project_id"),
            "cde_name":             row.get("cde_name"),
            "state":                row.get("state"),
            "investment_year":      row.get("investment_year"),
            "nmtc_allocation_used": row.get("nmtc_allocation_used"),
        }
        proj_id, conf = match_project(match_rec, index)
        if conf >= threshold:
            cur.execute(
                db.adapt_sql(
                    "UPDATE nmtc_coalition_projects "
                    "SET nmtc_project_id=?, match_confidence=? "
                    "WHERE coalition_project_id=?"
                ),
                (proj_id, conf, row["coalition_project_id"]),
            )
            updates += 1

    conn.commit()
    conn.close()

    print(f"  Updated {updates} Coalition records with matches")
    n = db.link_nmtc_coalition_to_projects()
    print(f"  Backfilled nmtc_projects.coalition_id for {n} projects")


def main():
    parser = argparse.ArgumentParser(
        description="Load NMTC Coalition Transaction Level Report and match to nmtc_projects"
    )
    parser.add_argument("--file", help="Path to Coalition Excel or CSV file")
    parser.add_argument("--columns-only", action="store_true",
                        help="Print column names and exit (no load)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Preview matches without writing to DB")
    parser.add_argument("--no-match", action="store_true",
                        help="Load Coalition records without running matching")
    parser.add_argument("--match-only", action="store_true",
                        help="Re-run matching on already-loaded Coalition records")
    parser.add_argument("--match-threshold", type=float, default=0.78,
                        help="Minimum match confidence to accept (default: 0.78)")
    args = parser.parse_args()

    if args.match_only:
        db.init_db()
        run_match_only(args.match_threshold)
        return

    if not args.file:
        parser.error("--file is required unless using --match-only")

    if not os.path.exists(args.file):
        print(f"ERROR: File not found: {args.file}")
        print("Download the NMTC Coalition Transaction Level Report from:")
        print("  https://nmtccoalition.org/nmtc-fact-sheet/")
        sys.exit(1)

    if not args.dry_run:
        db.init_db()

    load_file(
        filepath=args.file,
        columns_only=args.columns_only,
        dry_run=args.dry_run,
        no_match=args.no_match,
        match_threshold=args.match_threshold,
    )


if __name__ == "__main__":
    main()
