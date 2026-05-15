"""
Build GA authorizer input CSVs used by etl/load_ga_authorizers.py.

Outputs in data/raw/charter accountability/GA:
  - ga_authorizers.csv
  - ga_school_authorizer_links.csv

Sources:
  - local_charter_dataset.csv (local-authorized schools + authorizer names)
  - cpf_all_years.csv (all charter schools in CPF; used to add SCSC schools missing
    from local dataset)
  - scsc_schools_opening_years.csv (explicit SCSC school roster)

NCES mapping:
  - exact school_name join to schools table
  - normalized school_name fallback against GA charter rows in schools
"""

import argparse
import os
import re
import sqlite3

import pandas as pd


def _normalize_name(name: str) -> str:
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


def main():
    parser = argparse.ArgumentParser(description="Build GA authorizer ETL input files.")
    parser.add_argument(
        "--base-dir",
        default=r"data\raw\charter accountability\GA",
        help="Directory containing local_charter_dataset.csv and cpf_all_years.csv",
    )
    parser.add_argument(
        "--school-year",
        default="2023-24",
        help="School year value for generated links file",
    )
    args = parser.parse_args()

    base = args.base_dir
    local_path = os.path.join(base, "local_charter_dataset.csv")
    cpf_path = os.path.join(base, "cpf_all_years.csv")
    scsc_path = os.path.join(base, "scsc_schools_opening_years.csv")
    out_auth = os.path.join(base, "ga_authorizers.csv")
    out_links = os.path.join(base, "ga_school_authorizer_links.csv")

    if not os.path.isfile(local_path):
        raise FileNotFoundError(local_path)
    if not os.path.isfile(cpf_path):
        raise FileNotFoundError(cpf_path)
    if not os.path.isfile(scsc_path):
        raise FileNotFoundError(scsc_path)

    local = pd.read_csv(local_path, dtype=str)
    cpf = pd.read_csv(cpf_path, dtype=str)
    scsc = pd.read_csv(scsc_path, dtype=str)
    local.columns = [c.strip().lower() for c in local.columns]
    cpf.columns = [c.strip().lower() for c in cpf.columns]
    scsc.columns = [c.strip().lower() for c in scsc.columns]

    # Build authorizers from local data + SCSC (state commission authorizer).
    authorizers = (
        local[["authorizer"]]
        .dropna()
        .rename(columns={"authorizer": "authorizer_name"})
        .drop_duplicates()
    )
    if "authorizer_name" not in authorizers.columns:
        raise ValueError("local_charter_dataset.csv missing authorizer column")

    if not (authorizers["authorizer_name"].str.lower() == "scsc").any():
        authorizers = pd.concat(
            [authorizers, pd.DataFrame([{"authorizer_name": "SCSC"}])],
            ignore_index=True,
        )
    authorizers["authorizer_kind"] = authorizers["authorizer_name"].str.lower().map(
        lambda v: "ICB" if v == "scsc" else "LEA"
    )
    authorizers["source_system"] = "ga_charter_pilot"
    authorizers.to_csv(out_auth, index=False)

    # Prepare GA NCES lookup.
    conn = sqlite3.connect(r"data\cd_command_center.sqlite")
    schools = pd.read_sql_query(
        "SELECT nces_id, school_name FROM schools WHERE state='GA' AND is_charter=1",
        conn,
    )
    conn.close()
    schools = schools.dropna(subset=["nces_id", "school_name"]).copy()
    schools["school_name_norm"] = schools["school_name"].map(_normalize_name)

    exact_map = dict(zip(schools["school_name"], schools["nces_id"]))
    norm_map = {}
    for _, row in schools.iterrows():
        norm_map.setdefault(row["school_name_norm"], row["nces_id"])

    # Local links.
    local_links = local[["school_name", "authorizer"]].dropna().copy()
    local_links = local_links.rename(columns={"authorizer": "authorizer_name"})
    local_links["school_year"] = args.school_year

    # Force schools listed in SCSC roster to SCSC authorizer.
    scsc_names = {
        _normalize_name(s)
        for s in scsc.get("school_name", pd.Series(dtype=str)).dropna().tolist()
        if str(s).strip()
    }
    local_links["school_name_norm"] = local_links["school_name"].map(_normalize_name)
    local_links.loc[
        local_links["school_name_norm"].isin(scsc_names), "authorizer_name"
    ] = "SCSC"

    # Add SCSC roster schools not present in local dataset.
    local_norms = set(local_links["school_name_norm"].dropna().tolist())
    scsc_missing = scsc[
        scsc.get("school_name", pd.Series(dtype=str)).map(
            lambda x: _normalize_name(str(x)) if pd.notna(x) else None
        ).isin(scsc_names - local_norms)
    ].copy()
    scsc_missing = scsc_missing[["school_name"]].dropna().drop_duplicates()
    scsc_missing["authorizer_name"] = "SCSC"
    scsc_missing["school_year"] = args.school_year
    scsc_missing["school_name_norm"] = scsc_missing["school_name"].map(_normalize_name)

    # CPF schools not in local list => treated as SCSC-authorized for this pilot.
    local_norms = set(local_links["school_name_norm"].dropna().tolist()) | set(
        scsc_missing["school_name_norm"].dropna().tolist()
    )
    cpf_names = cpf[["school_name"]].dropna().drop_duplicates().copy()
    cpf_names["school_name_norm"] = cpf_names["school_name"].map(_normalize_name)
    cpf_missing = cpf_names[~cpf_names["school_name_norm"].isin(local_norms)].copy()
    cpf_missing["authorizer_name"] = "SCSC"
    cpf_missing["school_year"] = args.school_year
    cpf_missing = cpf_missing[["school_name", "authorizer_name", "school_year"]]

    links = pd.concat([local_links, scsc_missing, cpf_missing], ignore_index=True).drop_duplicates()
    links["nces_school_id"] = links["school_name"].map(exact_map)
    missing_mask = links["nces_school_id"].isna()
    links.loc[missing_mask, "nces_school_id"] = (
        links.loc[missing_mask, "school_name"].map(lambda x: norm_map.get(_normalize_name(str(x))))
    )

    links = links[
        ["nces_school_id", "school_name", "authorizer_name", "school_year"]
    ].sort_values(["authorizer_name", "school_name"])
    links.to_csv(out_links, index=False)

    missing = int(links["nces_school_id"].isna().sum())
    print(f"Wrote: {out_auth}")
    print(f"Wrote: {out_links}")
    print(f"Authorizers: {len(authorizers)}")
    print(f"Links total: {len(links)}")
    print(f"Links missing NCES ID: {missing}")


if __name__ == "__main__":
    main()

