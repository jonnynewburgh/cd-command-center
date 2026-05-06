"""
Load Georgia charter authorizer entities and school links into:
  - authorizers
  - school_authorizer

This is the GA pilot loader for the authorizer registry model. It expects
two CSVs:
  1) authorizers file (one row per authorizer entity)
  2) links file (one row per school-year link)

Usage:
    python etl/load_ga_authorizers.py \
      --authorizers-file data/raw/charter\ accountability/GA/ga_authorizers.csv \
      --links-file data/raw/charter\ accountability/GA/ga_school_authorizer_links.csv

    python etl/load_ga_authorizers.py --authorizers-file ... --links-file ... --dry-run
"""

import argparse
import os
import sys
from typing import Optional

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import db


REQUIRED_AUTHORIZER_COLS = {"authorizer_name"}
REQUIRED_LINK_COLS = {"nces_school_id", "authorizer_name", "school_year"}


def _clean(v) -> Optional[str]:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    s = str(v).strip()
    return s if s and s.lower() != "nan" else None


def _read_csv(path: str) -> pd.DataFrame:
    df = pd.read_csv(path, dtype=str)
    df.columns = [c.strip().lower() for c in df.columns]
    return df


def _validate_columns(df: pd.DataFrame, required: set[str], label: str):
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"{label} missing required columns: {sorted(missing)}")


def load_authorizers(authorizers_df: pd.DataFrame, dry_run: bool) -> int:
    loaded = 0
    for _, row in authorizers_df.iterrows():
        name = _clean(row.get("authorizer_name"))
        if not name:
            continue
        rec = {
            "state": "GA",
            "name": name,
            "authorizer_kind": _clean(row.get("authorizer_kind")),
            "nces_lea_id": _clean(row.get("nces_lea_id")),
            "state_authorizer_id": _clean(row.get("state_authorizer_id")),
            "source_system": _clean(row.get("source_system")) or "ga_doe_charter_board",
            "source_url": _clean(row.get("source_url")),
            "notes": _clean(row.get("notes")),
            "is_active": 1 if (_clean(row.get("is_active")) or "1") not in ("0", "false", "False") else 0,
        }
        if dry_run:
            print(f"[authorizer] {rec['name']} ({rec['authorizer_kind'] or 'unknown kind'})")
        else:
            db.upsert_authorizer(rec)
        loaded += 1
    return loaded


def _authorizer_name_to_id() -> dict[str, int]:
    lookup = {}
    df = db.get_authorizers(states=["GA"], active_only=False)
    for _, row in df.iterrows():
        name = str(row.get("name", "")).strip().lower()
        if name:
            lookup[name] = int(row["id"])
    return lookup


def load_links(links_df: pd.DataFrame, dry_run: bool) -> tuple[int, int]:
    loaded, skipped = 0, 0
    lookup = _authorizer_name_to_id()
    for _, row in links_df.iterrows():
        nces_school_id = _clean(row.get("nces_school_id"))
        authorizer_name = (_clean(row.get("authorizer_name")) or "").lower()
        school_year = _clean(row.get("school_year"))
        if not nces_school_id or not authorizer_name or not school_year:
            skipped += 1
            continue
        authorizer_id = lookup.get(authorizer_name)
        if not authorizer_id:
            print(f"SKIP link; unknown authorizer_name: {row.get('authorizer_name')}")
            skipped += 1
            continue
        rec = {
            "nces_school_id": nces_school_id,
            "authorizer_id": authorizer_id,
            "school_year": school_year,
            "relationship": _clean(row.get("relationship")) or "authorizer",
            "source_system": _clean(row.get("source_system")) or "ga_doe_charter_board",
        }
        if dry_run:
            print(f"[link] {nces_school_id} -> {row.get('authorizer_name')} ({school_year})")
        else:
            db.upsert_school_authorizer(rec)
        loaded += 1
    return loaded, skipped


def print_validation_summary():
    conn = db.get_connection()
    cur = conn.cursor()
    cur.execute(
        db.adapt_sql(
            """
            SELECT COUNT(*) FROM school_authorizer sa
            LEFT JOIN schools s ON s.nces_id = sa.nces_school_id
            WHERE s.nces_id IS NULL
            """
        )
    )
    orphan_links = cur.fetchone()[0]

    cur.execute(
        db.adapt_sql(
            """
            SELECT COUNT(*) FROM (
              SELECT nces_school_id, school_year, COUNT(*) AS n
              FROM school_authorizer
              GROUP BY nces_school_id, school_year
              HAVING COUNT(*) > 1
            ) t
            """
        )
    )
    duplicate_school_year_links = cur.fetchone()[0]

    cur.execute(
        db.adapt_sql(
            """
            SELECT COUNT(*) FROM authorizers WHERE state = 'GA'
            """
        )
    )
    ga_authorizers = cur.fetchone()[0]
    conn.close()

    print("\nValidation checks:")
    print(f"  GA authorizers loaded: {ga_authorizers}")
    print(f"  school_authorizer links without matching NCES school: {orphan_links}")
    print(f"  school-year rows with multiple authorizers: {duplicate_school_year_links}")


def main():
    parser = argparse.ArgumentParser(
        description="Load GA authorizer entities + school links into authorizer registry tables"
    )
    parser.add_argument("--authorizers-file", required=True, help="CSV path for GA authorizers")
    parser.add_argument("--links-file", required=True, help="CSV path for GA school-authorizer links")
    parser.add_argument("--dry-run", action="store_true", help="Print actions, no DB writes")
    args = parser.parse_args()

    for p in (args.authorizers_file, args.links_file):
        if not os.path.isfile(p):
            print(f"ERROR: file not found: {p}")
            sys.exit(1)

    authorizers_df = _read_csv(args.authorizers_file)
    links_df = _read_csv(args.links_file)
    _validate_columns(authorizers_df, REQUIRED_AUTHORIZER_COLS, "authorizers file")
    _validate_columns(links_df, REQUIRED_LINK_COLS, "links file")

    db.init_db()

    n_authorizers = load_authorizers(authorizers_df, args.dry_run)
    n_links, n_skipped = load_links(links_df, args.dry_run)
    print(f"\nProcessed authorizers: {n_authorizers}")
    print(f"Processed school-authorizer links: {n_links}")
    print(f"Skipped links: {n_skipped}")

    if not args.dry_run:
        print_validation_summary()


if __name__ == "__main__":
    main()

