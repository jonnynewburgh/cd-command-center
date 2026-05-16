"""
etl/load_ga_charter_pilot.py -- Load GA local charter pilot snapshot

Source: data/raw/charter accountability/GA/local_charter_dataset.csv

This is the analyst-curated dataset behind the GA pilot: identity +
authorizer (LEA or SCSC), school metadata (year_opened, grade span,
CMO), renewal status (charter_end, years_to_renewal), GA DOE CCRPI
scores (ccrpi_24, ccrpi_23, ccrpi_avg, ccrpi_desig), per-pupil
expenditure, FESR star rating, and pre-computed analytic composites
(acad_proxy, fin_proxy, risk_score).

Pairs with etl/load_scsc_cpf.py: that loader covers SCSC-authorized
charters via the SCSC CPF framework; this loader covers every GA
charter in the pilot dataset (mostly LEA-authorized but includes a
few SCSC schools too) via the CCRPI-based framework. After both run,
every GA charter has an accountability row -- either CPF, CCRPI, or
both.

NCES matching uses the same exact -> normalized -> fuzzy chain as
build_ga_authorizer_inputs.py, plus the ga_authorizer_overrides.csv
file when present (so manual overrides flow through to both pipelines).

Usage:
    python etl/load_ga_charter_pilot.py
    python etl/load_ga_charter_pilot.py --file path/to/local_charter_dataset.csv
    python etl/load_ga_charter_pilot.py --school-year 2023-24
    python etl/load_ga_charter_pilot.py --dry-run
    python etl/load_ga_charter_pilot.py --match-threshold 0.85
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


DEFAULT_MATCH_THRESHOLD = 0.82
DEFAULT_SCHOOL_YEAR = "2023-24"

REAL_COLS = (
    "ccrpi_24", "ccrpi_23", "ccrpi_avg", "ppe_avg", "fesr_stars",
    "acad_proxy", "fin_proxy", "risk_score",
)
INT_COLS = (
    "year_opened", "has_cmo", "is_conversion", "charter_end", "years_to_renewal",
)


def _repo_root() -> str:
    return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _default_csv() -> str:
    return os.path.join(
        _repo_root(), "data", "raw", "charter accountability", "GA",
        "local_charter_dataset.csv",
    )


def _default_overrides() -> str:
    return os.path.join(
        _repo_root(), "data", "seed", "authorizers", "ga_authorizer_overrides.csv",
    )


def _normalize_name(name: str) -> str:
    """Kept aligned with etl/build_ga_authorizer_inputs.py."""
    s = name.lower().strip()
    s = re.sub(r"[^\w\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    for suffix in (
        " charter school", " charter academy", " charter",
        " academy", " school", " inc", " llc", " corporation",
    ):
        if s.endswith(suffix):
            s = s[: -len(suffix)].strip()
    return s


def _clean(v) -> Optional[str]:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    s = str(v).strip()
    return s if s and s.lower() != "nan" else None


def _to_real(v):
    s = _clean(v)
    if s is None:
        return None
    try:
        return float(s)
    except ValueError:
        return None


def _to_int(v):
    s = _clean(v)
    if s is None:
        return None
    try:
        return int(float(s))
    except ValueError:
        return None


def _load_schools_index() -> tuple[dict, dict, list]:
    """Return (exact_map, norm_map, fuzzy_choices) for GA charter schools."""
    df = db._pd_read_sql(
        """
        SELECT nces_id, school_name
        FROM schools
        WHERE state = 'GA' AND is_charter = 1
          AND nces_id IS NOT NULL AND school_name IS NOT NULL
        """
    )
    exact_map = dict(zip(df["school_name"], df["nces_id"]))
    norm_map: dict[str, str] = {}
    for _, row in df.iterrows():
        norm_map.setdefault(_normalize_name(row["school_name"]), row["nces_id"])
    return exact_map, norm_map, list(norm_map.items())


def _load_overrides(path: str) -> dict[str, str]:
    if not os.path.isfile(path):
        return {}
    df = pd.read_csv(path, dtype=str)
    df.columns = [c.strip().lower() for c in df.columns]
    out: dict[str, str] = {}
    for _, row in df.iterrows():
        name = (row.get("school_name") or "").strip()
        nces = (row.get("nces_school_id") or "").strip()
        if name and nces:
            out[name] = nces
    return out


def _resolve_nces(
    school_name: str, overrides: dict, exact_map: dict,
    norm_map: dict, fuzzy_choices: list, threshold: float,
):
    if not school_name:
        return None, "unmatched"
    if school_name in overrides:
        return overrides[school_name], "override"
    if school_name in exact_map:
        return exact_map[school_name], "exact"
    norm = _normalize_name(school_name)
    if norm in norm_map:
        return norm_map[norm], "norm"
    best_id, best_score = None, 0.0
    for choice_norm, choice_id in fuzzy_choices:
        score = SequenceMatcher(None, norm, choice_norm).ratio()
        if score > best_score:
            best_id, best_score = choice_id, score
    if best_score >= threshold:
        return best_id, "fuzzy"
    return None, "unmatched"


def load(filepath: str, school_year: str, dry_run: bool, threshold: float, overrides_path: str):
    print(f"Reading: {filepath}")
    df = pd.read_csv(filepath, dtype=str)
    df.columns = [c.strip().lower() for c in df.columns]

    required = {"school_name", "authorizer"}
    missing = required - set(df.columns)
    if missing:
        print(f"ERROR: missing columns: {sorted(missing)}")
        sys.exit(1)
    print(f"  {len(df)} rows")

    exact_map, norm_map, fuzzy_choices = _load_schools_index()
    overrides = _load_overrides(overrides_path)
    print(f"  {len(exact_map)} GA charter schools loaded for matching")
    print(f"  {len(overrides)} curated overrides loaded")

    method_counts: dict[str, int] = {}
    loaded = 0

    for _, row in df.iterrows():
        school_name = _clean(row.get("school_name"))
        if not school_name:
            continue

        nces_id, method = _resolve_nces(
            school_name, overrides, exact_map, norm_map, fuzzy_choices, threshold,
        )
        method_counts[method] = method_counts.get(method, 0) + 1

        rec = {
            "nces_id": nces_id,
            "school_name": school_name,
            "school_year": school_year,
            "authorizer": _clean(row.get("authorizer")),
            "grade_span": _clean(row.get("grade_span")),
            "curriculum_type": _clean(row.get("curriculum_type")),
            "location_type": _clean(row.get("location_type")),
            "cmo_name": _clean(row.get("cmo_name")),
            "ccrpi_desig": _clean(row.get("ccrpi_desig")),
        }
        for col in INT_COLS:
            rec[col] = _to_int(row.get(col))
        for col in REAL_COLS:
            rec[col] = _to_real(row.get(col))

        if dry_run:
            status = f"OK {nces_id}" if nces_id else "?? no nces"
            print(f"  [{school_year}] {school_name[:55]:<55}  {status}  ({method})")
        else:
            db.upsert_ga_charter_pilot(rec)
            loaded += 1

    print(f"\n{'DRY RUN -- ' if dry_run else ''}Results:")
    print(f"  Total rows processed: {loaded if not dry_run else sum(method_counts.values())}")
    print(f"  NCES resolution:")
    for m in ("override", "exact", "norm", "fuzzy", "unmatched"):
        if m in method_counts:
            print(f"    {m:<10} {method_counts[m]}")


def main():
    parser = argparse.ArgumentParser(
        description="Load GA local charter pilot snapshot into ga_charter_pilot table"
    )
    parser.add_argument("--file", default=_default_csv(),
                        help=f"Path to local_charter_dataset.csv (default: {_default_csv()})")
    parser.add_argument("--school-year", default=DEFAULT_SCHOOL_YEAR,
                        help=f"School year tag for this snapshot (default: {DEFAULT_SCHOOL_YEAR})")
    parser.add_argument("--match-threshold", type=float, default=DEFAULT_MATCH_THRESHOLD,
                        help=f"Fuzzy match cutoff 0-1 (default: {DEFAULT_MATCH_THRESHOLD})")
    parser.add_argument("--overrides-file", default=_default_overrides(),
                        help="CSV of curated school_name -> nces_school_id overrides")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print actions without writing to DB")
    args = parser.parse_args()

    if not os.path.isfile(args.file):
        print(f"ERROR: file not found: {args.file}")
        sys.exit(1)

    load(args.file, args.school_year, args.dry_run, args.match_threshold, args.overrides_file)


if __name__ == "__main__":
    main()
