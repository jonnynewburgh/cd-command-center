"""
etl/load_scsc_cpf.py — Load SCSC Comprehensive Performance Framework data
for Georgia charter schools into the scsc_cpf table.

Data source: State Charter Schools Commission of Georgia (SCSC)
  CPF scores are not subject to commercial use restrictions — they are
  published by a state government agency as public records.

The source file lives in the companion charters repo:
  C:/Users/jonny/Documents/GitHub/charters/data/cpf_all_years.csv

Columns in the source file:
  school_year   — e.g. "2023-24"
  school_name   — charter school name (not an NCES ID)
  academic      — academic designation: Exceeds / Meets / Approaches / Does Not Meet
  fin_desig     — financial designation (same scale)
  fin_ind1      — financial indicator 1 score
  fin_ind2      — financial indicator 2 score
  ops_score     — operations composite score
  ops_desig     — operations designation

After loading, the script attempts to match each school name to an NCES ID
in the schools table using fuzzy string matching (difflib SequenceMatcher).
Matches scoring >= 0.82 similarity are accepted.  The match threshold was
chosen to handle common abbreviations and punctuation differences without
pulling in wrong schools.  Unmatched rows are still loaded (nces_id = NULL).

Usage:
    # Load from default path (charters repo)
    python etl/load_scsc_cpf.py

    # Specify a different CSV path
    python etl/load_scsc_cpf.py --file /path/to/cpf_all_years.csv

    # Preview matches without writing to DB
    python etl/load_scsc_cpf.py --dry-run

    # Raise the matching threshold (fewer matches, higher precision)
    python etl/load_scsc_cpf.py --match-threshold 0.9
"""

import argparse
import os
import sys
from difflib import SequenceMatcher

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import db

# Default path — charters repo relative to this repo's parent directory
DEFAULT_FILE = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    "charters", "data", "cpf_all_years.csv",
)

DEFAULT_MATCH_THRESHOLD = 0.82


def _normalize(name: str) -> str:
    """Lowercase, strip punctuation and common suffixes for matching."""
    import re
    name = name.lower().strip()
    # Remove punctuation
    name = re.sub(r"[^\w\s]", " ", name)
    # Collapse whitespace
    name = re.sub(r"\s+", " ", name).strip()
    # Remove common suffixes that differ between sources
    for suffix in (" charter school", " charter", " school", " academy", " inc", " llc"):
        if name.endswith(suffix):
            name = name[: -len(suffix)].strip()
    return name


def build_school_name_index() -> dict:
    """
    Load all GA charter schools from the DB and return a dict:
      normalized_name -> nces_id
    """
    conn = db.get_connection()
    df = pd.read_sql_query(
        "SELECT nces_id, school_name FROM schools WHERE state = 'GA' AND is_charter = 1",
        conn,
    )
    conn.close()
    return {_normalize(row["school_name"]): row["nces_id"] for _, row in df.iterrows()}


def match_school(cpf_name: str, index: dict, threshold: float) -> str | None:
    """
    Return the nces_id for the best-matching school, or None if below threshold.
    Uses difflib SequenceMatcher on normalized names.
    """
    norm = _normalize(cpf_name)

    # Exact match first (fast path)
    if norm in index:
        return index[norm]

    # Fuzzy match
    best_score = 0.0
    best_id = None
    for db_name, nces_id in index.items():
        score = SequenceMatcher(None, norm, db_name).ratio()
        if score > best_score:
            best_score = score
            best_id = nces_id

    return best_id if best_score >= threshold else None


def load_cpf(filepath: str, threshold: float, dry_run: bool):
    print(f"Reading CPF file: {filepath}")
    df = pd.read_csv(filepath, dtype=str)
    df.columns = [c.strip().lower() for c in df.columns]

    # Validate expected columns
    required = {"school_year", "school_name", "academic"}
    missing = required - set(df.columns)
    if missing:
        print(f"ERROR: Missing columns: {missing}")
        print(f"Available columns: {list(df.columns)}")
        sys.exit(1)

    print(f"  {len(df)} rows across {df['school_year'].nunique()} school years")

    # Build NCES name index for GA charters
    print("Loading GA charter school names from database for matching...")
    index = build_school_name_index()
    print(f"  {len(index)} GA charter schools in DB")

    loaded = matched = unmatched = 0
    unmatched_names = []

    for _, row in df.iterrows():
        school_name = str(row.get("school_name", "")).strip()
        school_year = str(row.get("school_year", "")).strip()
        if not school_name or not school_year:
            continue

        nces_id = match_school(school_name, index, threshold)
        if nces_id:
            matched += 1
        else:
            unmatched += 1
            unmatched_names.append(school_name)

        def _str(val):
            """Convert to string, treating NaN/None/empty as None."""
            if val is None:
                return None
            s = str(val).strip()
            return s if s and s.lower() != "nan" else None

        record = {
            "school_name":             school_name,
            "school_year":             school_year,
            "academic_designation":    _str(row.get("academic")),
            "financial_designation":   _str(row.get("fin_desig")),
            "operations_designation":  _str(row.get("ops_desig")),
        }

        # Numeric fields — coerce to float, allow NULL
        for dest, src in [
            ("financial_indicator_1", "fin_ind1"),
            ("financial_indicator_2", "fin_ind2"),
            ("operations_score",      "ops_score"),
        ]:
            val = row.get(src, "")
            try:
                record[dest] = float(val)
            except (ValueError, TypeError):
                record[dest] = None

        if nces_id:
            record["nces_id"] = nces_id

        if dry_run:
            status = f"✓ {nces_id}" if nces_id else "✗ no match"
            print(f"  [{school_year}] {school_name[:50]:<50}  {status}")
        else:
            db.upsert_scsc_cpf(record)
            loaded += 1

    print(f"\n{'DRY RUN — ' if dry_run else ''}Results:")
    print(f"  Total rows:  {loaded + (unmatched if dry_run else 0):>5}")
    print(f"  Matched:     {matched:>5}  ({100*matched/(matched+unmatched):.0f}%)" if matched+unmatched else "")
    print(f"  Unmatched:   {unmatched:>5}  (loaded with nces_id=NULL)")

    if unmatched_names and not dry_run:
        print("\nUnmatched school names (consider adding to DB or lowering --match-threshold):")
        for name in sorted(set(unmatched_names)):
            print(f"  {name}")


def main():
    parser = argparse.ArgumentParser(
        description="Load SCSC CPF accountability data for GA charter schools"
    )
    parser.add_argument(
        "--file", default=DEFAULT_FILE,
        help=f"Path to cpf_all_years.csv (default: {DEFAULT_FILE})",
    )
    parser.add_argument(
        "--match-threshold", type=float, default=DEFAULT_MATCH_THRESHOLD,
        help=f"Minimum fuzzy match score 0-1 (default: {DEFAULT_MATCH_THRESHOLD})",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Print match results without writing to the database",
    )
    args = parser.parse_args()

    if not os.path.exists(args.file):
        print(f"ERROR: File not found: {args.file}")
        print("Download from the SCSC or point --file at the cpf_all_years.csv in the charters repo.")
        sys.exit(1)

    if not args.dry_run:
        db.init_db()

    load_cpf(args.file, args.match_threshold, args.dry_run)

    if not args.dry_run:
        print("\nDone. Run python etl/load_scsc_cpf.py --dry-run to preview match quality.")


if __name__ == "__main__":
    main()
