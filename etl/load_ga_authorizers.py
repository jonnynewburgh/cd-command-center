"""
Load Georgia charter authorizer entities and school links into:
  - authorizers
  - school_authorizer

This GA pilot loader expects two CSVs:
  1) authorizers file (one row per authorizer entity)
  2) links file (one row per school-year link)

The links file should include `nces_school_id`, but if that is missing
the loader resolves IDs from `school_name` using GA charter rows in
`schools` plus `scsc_cpf` mappings, with the same exact → normalized →
fuzzy fallback chain that etl/build_ga_authorizer_inputs.py uses.
"""

import argparse
import os
import re
import sys
from difflib import SequenceMatcher
from typing import Optional

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import db  # noqa: E402


REQUIRED_AUTHORIZER_COLS = {"authorizer_name"}
REQUIRED_LINK_COLS = {"authorizer_name", "school_year"}
DEFAULT_MATCH_THRESHOLD = 0.82


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


def _normalize_name(name: str) -> str:
    """Kept aligned with etl/build_ga_authorizer_inputs.py — change both."""
    s = name.lower().strip()
    s = re.sub(r"[^\w\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    for suffix in (
        " charter school",
        " charter academy",
        " charter",
        " academy",
        " school",
        " inc",
        " llc",
        " corporation",
    ):
        if s.endswith(suffix):
            s = s[: -len(suffix)].strip()
    return s


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
    lookup: dict[str, int] = {}
    df = db.get_authorizers(states=["GA"], active_only=False)
    for _, row in df.iterrows():
        name = str(row.get("name", "")).strip().lower()
        if name:
            lookup[name] = int(row["id"])
    return lookup


def _build_ga_name_index() -> dict[str, str]:
    """Normalized GA charter school_name -> nces_id from schools + scsc_cpf.

    schools is the authoritative source; scsc_cpf fills gaps (it carries names
    SCSC reports on that may not have made it into the schools roster yet).
    """
    lookup: dict[str, str] = {}
    schools_df = db._pd_read_sql(
        """
        SELECT nces_id, school_name
        FROM schools
        WHERE state = 'GA' AND is_charter = 1 AND nces_id IS NOT NULL
        """
    )
    for _, row in schools_df.iterrows():
        nm = _clean(row.get("school_name"))
        nces = _clean(row.get("nces_id"))
        if nm and nces:
            lookup.setdefault(_normalize_name(nm), nces)

    scsc_df = db._pd_read_sql(
        """
        SELECT nces_id, school_name
        FROM scsc_cpf
        WHERE nces_id IS NOT NULL
        """
    )
    for _, row in scsc_df.iterrows():
        nm = _clean(row.get("school_name"))
        nces = _clean(row.get("nces_id"))
        if nm and nces:
            lookup.setdefault(_normalize_name(nm), nces)
    return lookup


def _resolve_nces(name: str, norm_map: dict, choices: list, threshold: float):
    """Return (nces_id, method). method in {'norm', 'fuzzy', 'unmatched'}."""
    if not name:
        return None, "unmatched"
    norm = _normalize_name(name)
    if norm in norm_map:
        return norm_map[norm], "norm"
    best_id, best_score = None, 0.0
    for choice_norm, choice_id in choices:
        score = SequenceMatcher(None, norm, choice_norm).ratio()
        if score > best_score:
            best_id, best_score = choice_id, score
    if best_score >= threshold:
        return best_id, "fuzzy"
    return None, "unmatched"


def load_links(
    links_df: pd.DataFrame,
    dry_run: bool,
    authorizers_df: pd.DataFrame,
    threshold: float,
) -> dict:
    auth_lookup = _authorizer_name_to_id()
    # In dry-run, let unknown authorizer names from the CSV resolve so we can
    # preview without first upserting the authorizers.
    if dry_run:
        for idx, row in authorizers_df.iterrows():
            nm = _clean(row.get("authorizer_name"))
            if nm:
                auth_lookup.setdefault(nm.lower(), -(idx + 1))

    norm_map = _build_ga_name_index()
    fuzzy_choices = list(norm_map.items())

    loaded = 0
    resolved_norm = 0
    resolved_fuzzy = 0
    skipped_no_nces = 0
    skipped_unknown_authorizer: dict[str, int] = {}
    skipped_incomplete = 0

    for _, row in links_df.iterrows():
        nces_school_id = _clean(row.get("nces_school_id"))
        school_name = _clean(row.get("school_name"))
        if not nces_school_id and school_name:
            resolved_id, method = _resolve_nces(
                school_name, norm_map, fuzzy_choices, threshold
            )
            if resolved_id:
                nces_school_id = resolved_id
                if method == "norm":
                    resolved_norm += 1
                else:
                    resolved_fuzzy += 1

        authorizer_name = (_clean(row.get("authorizer_name")) or "").lower()
        school_year = _clean(row.get("school_year"))

        if not authorizer_name or not school_year:
            skipped_incomplete += 1
            continue
        if not nces_school_id:
            skipped_no_nces += 1
            continue
        authorizer_id = auth_lookup.get(authorizer_name)
        if not authorizer_id:
            skipped_unknown_authorizer[authorizer_name] = (
                skipped_unknown_authorizer.get(authorizer_name, 0) + 1
            )
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

    return {
        "loaded": loaded,
        "resolved_norm": resolved_norm,
        "resolved_fuzzy": resolved_fuzzy,
        "skipped_no_nces": skipped_no_nces,
        "skipped_incomplete": skipped_incomplete,
        "skipped_unknown_authorizer": skipped_unknown_authorizer,
    }


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
    parser.add_argument(
        "--match-threshold",
        type=float,
        default=DEFAULT_MATCH_THRESHOLD,
        help=f"Fuzzy cutoff for resolving school_name -> NCES id when "
             f"nces_school_id is blank (default: {DEFAULT_MATCH_THRESHOLD})",
    )
    args = parser.parse_args()

    for p in (args.authorizers_file, args.links_file):
        if not os.path.isfile(p):
            print(f"ERROR: file not found: {p}")
            sys.exit(1)

    authorizers_df = _read_csv(args.authorizers_file)
    links_df = _read_csv(args.links_file)
    _validate_columns(authorizers_df, REQUIRED_AUTHORIZER_COLS, "authorizers file")
    _validate_columns(links_df, REQUIRED_LINK_COLS, "links file")
    if "nces_school_id" not in links_df.columns and "school_name" not in links_df.columns:
        raise ValueError(
            "links file must include either 'nces_school_id' or 'school_name' for NCES matching"
        )

    db.init_db()

    n_authorizers = load_authorizers(authorizers_df, args.dry_run)
    stats = load_links(links_df, args.dry_run, authorizers_df, args.match_threshold)

    print(f"\nProcessed authorizers: {n_authorizers}")
    print(f"Processed school-authorizer links: {stats['loaded']}")
    print(f"  resolved NCES from name (normalized): {stats['resolved_norm']}")
    print(f"  resolved NCES from name (fuzzy):      {stats['resolved_fuzzy']}")
    print(f"Skipped links:")
    print(f"  incomplete (missing year or authorizer): {stats['skipped_incomplete']}")
    print(f"  no NCES id and name did not resolve:     {stats['skipped_no_nces']}")
    if stats["skipped_unknown_authorizer"]:
        total = sum(stats["skipped_unknown_authorizer"].values())
        print(f"  unknown authorizer_name ({total} rows):")
        for name, n in sorted(
            stats["skipped_unknown_authorizer"].items(), key=lambda x: -x[1]
        ):
            print(f"    {n:>4}  {name}")

    if not args.dry_run:
        print_validation_summary()


if __name__ == "__main__":
    main()
