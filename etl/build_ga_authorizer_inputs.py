"""
Build GA authorizer input CSVs used by etl/load_ga_authorizers.py.

Outputs (default location: data/raw/charter accountability/GA):
  - ga_authorizers.csv                       — one row per authorizer entity
  - ga_school_authorizer_links.csv           — one row per (school, year, authorizer)
  - ga_school_authorizer_links_missing_nces.csv — links the matcher could not
                                                  resolve to an NCES school id;
                                                  edit by hand and re-load

Sources:
  - local_charter_dataset.csv     — schools authorized by LEAs, with the
                                    authorizer (district) named explicitly.
  - cpf_all_years.csv             — every charter SCSC reports on. Schools in
                                    this file but not in the local dataset are
                                    treated as SCSC-authorized for the pilot.
  - scsc_schools_opening_years.csv — explicit SCSC roster; if a school appears
                                     here it is forced to SCSC even when the
                                     local file said otherwise (SCSC roster is
                                     authoritative for current authorizer).

NCES resolution (one nces_id per school, in priority order):
  1. exact school_name match against schools (GA charters only)
  2. normalized-name match (lowercased, punctuation stripped, common suffixes
     like " charter school", " academy" removed)
  3. fuzzy match via difflib.SequenceMatcher with --match-threshold
     (default 0.82; same threshold load_scsc_cpf.py uses on the same data)
"""

import argparse
import os
import re
import sys
from difflib import SequenceMatcher

import pandas as pd

# Make the sibling db.py importable. All DB access in this repo goes through
# db.py — see CLAUDE.md ("All database access goes through db.py").
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import db  # noqa: E402


DEFAULT_MATCH_THRESHOLD = 0.82


def _repo_root() -> str:
    return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _default_base_dir() -> str:
    return os.path.join(_repo_root(), "data", "raw", "charter accountability", "GA")


def _normalize_name(name: str) -> str:
    """Lowercase, strip punctuation, collapse whitespace, drop common suffixes.

    Kept aligned with the matching helper in etl/load_ga_authorizers.py and
    etl/load_scsc_cpf.py — if you change one, change the others.
    """
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


def _load_ga_charter_schools() -> pd.DataFrame:
    """Pull GA charter schools (nces_id, school_name) from the schools table."""
    return db._pd_read_sql(
        """
        SELECT nces_id, school_name
        FROM schools
        WHERE state = 'GA' AND is_charter = 1
          AND nces_id IS NOT NULL AND school_name IS NOT NULL
        """
    )


def _build_match_index(schools: pd.DataFrame):
    """Return (exact_map, norm_map, fuzzy_choices).

    exact_map:    school_name -> nces_id          (case-sensitive exact)
    norm_map:     normalized_name -> nces_id      (first-wins on collisions)
    fuzzy_choices: list of (normalized_name, nces_id) for SequenceMatcher
    """
    exact_map = dict(zip(schools["school_name"], schools["nces_id"]))

    norm_map: dict[str, str] = {}
    for _, row in schools.iterrows():
        norm = _normalize_name(row["school_name"])
        norm_map.setdefault(norm, row["nces_id"])

    fuzzy_choices = list(norm_map.items())
    return exact_map, norm_map, fuzzy_choices


def _overrides_path() -> str:
    return os.path.join(_repo_root(), "data", "seed", "authorizers", "ga_authorizer_overrides.csv")


def _load_overrides() -> dict[str, str]:
    """Read data/seed/authorizers/ga_authorizer_overrides.csv if present.

    Format: school_name, nces_school_id (extra columns like 'note' are ignored).
    Analyst-curated and tracked in git -- takes priority over exact /
    normalized / fuzzy resolution. Use this for cases the matcher can't handle:
    parenthetical campus markers, acronyms, name changes, etc.
    """
    path = _overrides_path()
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
    school_name: str,
    overrides: dict,
    exact_map: dict,
    norm_map: dict,
    fuzzy_choices: list,
    threshold: float,
):
    """Return (nces_id, method, score).

    method is one of: 'override', 'exact', 'norm', 'fuzzy', 'unmatched'.
    score is 1.0 for override/exact/norm, the SequenceMatcher ratio for fuzzy,
    and 0.0 for unmatched.
    """
    if not school_name:
        return None, "unmatched", 0.0

    if school_name in overrides:
        return overrides[school_name], "override", 1.0

    if school_name in exact_map:
        return exact_map[school_name], "exact", 1.0

    norm = _normalize_name(school_name)
    if norm in norm_map:
        return norm_map[norm], "norm", 1.0

    best_id, best_score = None, 0.0
    for choice_norm, choice_id in fuzzy_choices:
        score = SequenceMatcher(None, norm, choice_norm).ratio()
        if score > best_score:
            best_id, best_score = choice_id, score
    if best_score >= threshold:
        return best_id, "fuzzy", best_score
    return None, "unmatched", best_score


def _build_authorizers(local: pd.DataFrame) -> pd.DataFrame:
    """One row per distinct authorizer in the local dataset, plus SCSC."""
    if "authorizer" not in local.columns:
        raise ValueError("local_charter_dataset.csv missing 'authorizer' column")

    authorizers = (
        local[["authorizer"]]
        .dropna()
        .rename(columns={"authorizer": "authorizer_name"})
        .drop_duplicates()
    )

    if not (authorizers["authorizer_name"].str.lower() == "scsc").any():
        authorizers = pd.concat(
            [authorizers, pd.DataFrame([{"authorizer_name": "SCSC"}])],
            ignore_index=True,
        )

    authorizers["authorizer_kind"] = authorizers["authorizer_name"].str.lower().map(
        lambda v: "ICB" if v == "scsc" else "LEA"
    )
    authorizers["source_system"] = "ga_charter_pilot"
    return authorizers.sort_values("authorizer_name").reset_index(drop=True)


def _build_links(
    local: pd.DataFrame,
    scsc: pd.DataFrame,
    cpf: pd.DataFrame,
    school_year: str,
) -> pd.DataFrame:
    """Combine the three sources into a single (school, authorizer, year) frame.

    Rules:
      - Start from local_charter_dataset (LEA-authorized rows).
      - SCSC roster overrides authorizer to SCSC for any school it lists.
      - SCSC roster schools missing from local are added as SCSC-authorized.
      - CPF schools missing from BOTH are added as SCSC-authorized (pilot assumption:
        if SCSC reports on a school it is in SCSC's portfolio).
    """
    local_links = (
        local[["school_name", "authorizer"]]
        .dropna()
        .rename(columns={"authorizer": "authorizer_name"})
        .copy()
    )
    local_links["school_year"] = school_year
    local_links["school_name_norm"] = local_links["school_name"].map(_normalize_name)

    scsc_names = {
        _normalize_name(s)
        for s in scsc.get("school_name", pd.Series(dtype=str)).dropna().tolist()
        if str(s).strip()
    }
    local_links.loc[
        local_links["school_name_norm"].isin(scsc_names), "authorizer_name"
    ] = "SCSC"

    local_norms = set(local_links["school_name_norm"].dropna())

    scsc_only_names = scsc_names - local_norms
    if scsc_only_names and "school_name" in scsc.columns:
        scsc_missing = scsc[["school_name"]].dropna().copy()
        scsc_missing["school_name_norm"] = scsc_missing["school_name"].map(_normalize_name)
        scsc_missing = scsc_missing[scsc_missing["school_name_norm"].isin(scsc_only_names)]
        scsc_missing = scsc_missing.drop_duplicates(subset=["school_name_norm"])
        scsc_missing["authorizer_name"] = "SCSC"
        scsc_missing["school_year"] = school_year
    else:
        scsc_missing = pd.DataFrame(
            columns=["school_name", "school_name_norm", "authorizer_name", "school_year"]
        )

    covered_norms = local_norms | set(scsc_missing["school_name_norm"].dropna())

    cpf_names = cpf[["school_name"]].dropna().drop_duplicates().copy()
    cpf_names["school_name_norm"] = cpf_names["school_name"].map(_normalize_name)
    cpf_missing = cpf_names[~cpf_names["school_name_norm"].isin(covered_norms)].copy()
    cpf_missing["authorizer_name"] = "SCSC"
    cpf_missing["school_year"] = school_year

    links = pd.concat(
        [local_links, scsc_missing, cpf_missing], ignore_index=True
    )

    # Defensive dedup: same (school_name_norm, school_year, authorizer_name).
    links = links.drop_duplicates(
        subset=["school_name_norm", "school_year", "authorizer_name"]
    )
    return links


def _check_conflicting_assignments(links: pd.DataFrame) -> pd.DataFrame:
    """Return rows where the same normalized school+year maps to >1 authorizer."""
    counts = (
        links.groupby(["school_name_norm", "school_year"])["authorizer_name"]
        .nunique()
        .reset_index(name="n_authorizers")
    )
    conflicts = counts[counts["n_authorizers"] > 1]
    if conflicts.empty:
        return conflicts
    return links.merge(
        conflicts[["school_name_norm", "school_year"]],
        on=["school_name_norm", "school_year"],
        how="inner",
    ).sort_values(["school_name_norm", "school_year"])


def main():
    parser = argparse.ArgumentParser(description="Build GA authorizer ETL input files.")
    parser.add_argument(
        "--base-dir",
        default=_default_base_dir(),
        help="Directory containing local_charter_dataset.csv, cpf_all_years.csv, "
             "scsc_schools_opening_years.csv (default: repo data/raw/charter accountability/GA)",
    )
    parser.add_argument(
        "--school-year",
        default="2023-24",
        help="School year value for generated links file (default: 2023-24)",
    )
    parser.add_argument(
        "--match-threshold",
        type=float,
        default=DEFAULT_MATCH_THRESHOLD,
        help=f"Fuzzy match cutoff 0-1 (default: {DEFAULT_MATCH_THRESHOLD}). "
             "Lower = more matches, more false positives.",
    )
    args = parser.parse_args()

    base = args.base_dir
    local_path = os.path.join(base, "local_charter_dataset.csv")
    cpf_path = os.path.join(base, "cpf_all_years.csv")
    scsc_path = os.path.join(base, "scsc_schools_opening_years.csv")
    out_auth = os.path.join(base, "ga_authorizers.csv")
    out_links = os.path.join(base, "ga_school_authorizer_links.csv")
    out_missing = os.path.join(base, "ga_school_authorizer_links_missing_nces.csv")

    for p in (local_path, cpf_path, scsc_path):
        if not os.path.isfile(p):
            raise FileNotFoundError(p)

    local = pd.read_csv(local_path, dtype=str)
    cpf = pd.read_csv(cpf_path, dtype=str)
    scsc = pd.read_csv(scsc_path, dtype=str)
    for df in (local, cpf, scsc):
        df.columns = [c.strip().lower() for c in df.columns]

    authorizers = _build_authorizers(local)
    authorizers.to_csv(out_auth, index=False)

    schools = _load_ga_charter_schools()
    if schools.empty:
        print("WARNING: schools table has 0 GA charters — all links will be unmatched.")
        print("         Run etl/fetch_nces_schools.py --states GA --charter-only first.")
    exact_map, norm_map, fuzzy_choices = _build_match_index(schools)
    overrides = _load_overrides()

    links = _build_links(local, scsc, cpf, args.school_year)

    methods, scores, ids = [], [], []
    for name in links["school_name"]:
        nces_id, method, score = _resolve_nces(
            str(name) if pd.notna(name) else "",
            overrides, exact_map, norm_map, fuzzy_choices, args.match_threshold,
        )
        ids.append(nces_id)
        methods.append(method)
        scores.append(score)
    links["nces_school_id"] = ids
    links["_match_method"] = methods
    links["_match_score"] = scores

    output_cols = ["nces_school_id", "school_name", "authorizer_name", "school_year"]
    links_out = links[output_cols].sort_values(["authorizer_name", "school_name"])
    links_out.to_csv(out_links, index=False)

    missing_mask = links["nces_school_id"].isna()
    missing_out = links.loc[missing_mask, output_cols].sort_values(
        ["authorizer_name", "school_name"]
    )
    missing_out.to_csv(out_missing, index=False)

    method_counts = links["_match_method"].value_counts().to_dict()
    fuzzy_rows = links[links["_match_method"] == "fuzzy"][
        ["school_name", "nces_school_id", "_match_score"]
    ].sort_values("_match_score")
    by_authorizer = links.groupby("authorizer_name").size().sort_values(ascending=False)
    conflicts = _check_conflicting_assignments(links)

    print(f"Wrote: {out_auth}")
    print(f"Wrote: {out_links}")
    print(f"Wrote: {out_missing}")
    print(f"\nAuthorizers: {len(authorizers)}")
    print(f"Links total: {len(links_out)}")
    print(f"  matched override: {method_counts.get('override', 0)}")
    print(f"  matched exact:    {method_counts.get('exact', 0)}")
    print(f"  matched norm:     {method_counts.get('norm', 0)}")
    print(f"  matched fuzzy:    {method_counts.get('fuzzy', 0)} (threshold {args.match_threshold})")
    print(f"  unmatched:        {method_counts.get('unmatched', 0)} -> {os.path.basename(out_missing)}")
    print("\nBy authorizer:")
    for name, n in by_authorizer.items():
        print(f"  {n:>4}  {name}")

    if not fuzzy_rows.empty:
        print("\nFuzzy matches (review the low-score ones):")
        for _, row in fuzzy_rows.iterrows():
            print(f"  {row['_match_score']:.2f}  {row['nces_school_id']}  {row['school_name']}")

    if not conflicts.empty:
        print(f"\nWARNING: {conflicts['school_name_norm'].nunique()} schools assigned to "
              "multiple authorizers in the same year:")
        for _, row in conflicts.iterrows():
            print(f"  {row['school_year']}  {row['authorizer_name']:<8}  {row['school_name']}")

    # Different source-CSV names resolving to the same nces_id usually means
    # the matcher collapsed two distinct campuses, or the schools table merges
    # them under one record. Either way, worth a human eyeball.
    matched_links = links.dropna(subset=["nces_school_id"])
    nces_collisions = (
        matched_links.groupby("nces_school_id")["school_name"]
        .nunique()
        .reset_index(name="n_names")
    )
    nces_collisions = nces_collisions[nces_collisions["n_names"] > 1]
    if not nces_collisions.empty:
        print(f"\nWARNING: {len(nces_collisions)} NCES ids matched by more than one "
              "source school_name -- verify these are not separate campuses:")
        for nces_id in nces_collisions["nces_school_id"]:
            names = sorted(
                matched_links.loc[matched_links["nces_school_id"] == nces_id, "school_name"].unique()
            )
            print(f"  {nces_id}  <-  {' | '.join(names)}")


if __name__ == "__main__":
    main()
