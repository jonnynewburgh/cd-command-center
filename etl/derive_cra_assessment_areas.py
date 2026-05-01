"""
etl/derive_cra_assessment_areas.py — Populate cra_assessment_areas by
derivation from cra_sb_discl + cra_institutions.

Why this exists
---------------
FFIEC publishes the bank-level CRA distribution as several flat files —
the transmittal (institution register), aggregate small-biz lending, and
per-bank disclosure files. Some annual zips also include a dedicated
"Assessment Area" register (`Agg_Assessment_Area.dat`) but the historical
zips the user has on disk (1996-2024) don't ship one.

The information is already in the DB though: every row of `cra_sb_discl`
is keyed by (respondent_id, year, state_fips, county_fips, msa_code) —
each unique tuple is an assessment area. This script aggregates them and
writes the result to `cra_assessment_areas`, joining `cra_institutions`
to pick up `institution_name`.

This unblocks P2 #10b (audit 2026-04-26) without needing new source files.

Usage
-----
    python etl/derive_cra_assessment_areas.py
    python etl/derive_cra_assessment_areas.py --years 2022 2023 2024
"""

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import db
from utils.state_fips import FIPS_STATE


def derive(years: list[int] | None = None) -> int:
    """Derive cra_assessment_areas from cra_sb_discl. Returns rows inserted.

    Idempotent: deletes the year-range we're about to write before inserting,
    so re-runs give a clean replacement rather than duplicates.
    """
    conn = db.get_connection()
    cur = conn.cursor()
    ph = "%s" if db._IS_POSTGRES else "?"

    if years:
        cur.execute(
            f"DELETE FROM cra_assessment_areas WHERE report_year IN ({','.join([ph] * len(years))})",
            years,
        )
    else:
        cur.execute("DELETE FROM cra_assessment_areas")
    deleted = cur.rowcount
    print(f"  deleted {deleted:,} stale rows")

    # The aggregation: one row per (bank, year, county[, msa]). Joining
    # cra_institutions picks up the friendly name; LEFT JOIN so banks that
    # filed disclosure but aren't in the transmittal still produce a row.
    where = ""
    where_params: list = []
    if years:
        where = f"WHERE d.year IN ({','.join([ph] * len(years))})"
        where_params = list(years)

    select_sql = f"""
        SELECT
            d.respondent_id,
            i.institution_name,
            d.year                                  AS report_year,
            d.state_fips,
            d.county_fips,
            d.msa_code
        FROM cra_sb_discl d
        LEFT JOIN cra_institutions i
               ON i.respondent_id = d.respondent_id
              AND i.report_year   = d.year
        {where}
        GROUP BY d.respondent_id, i.institution_name, d.year,
                 d.state_fips, d.county_fips, d.msa_code
    """
    cur.execute(select_sql, where_params)
    raw = cur.fetchall()
    print(f"  aggregated {len(raw):,} (bank, year, county) tuples")

    rows = []
    for resp_id, inst_name, year, st_fips, co_fips, msa in raw:
        state = FIPS_STATE.get(str(st_fips).zfill(2)) if st_fips else None
        # area_type: MSA-level if msa_code populated and non-zero; else County.
        # MSA codes of '0' / '00000' / NULL all signify non-metro.
        is_msa = bool(msa) and str(msa).strip() not in ("", "0", "00000", "99999")
        area_type = "MSA" if is_msa else "County"
        # Synthesized label so downstream filters/UI have something readable
        # without requiring a separate county-name lookup. The (state, county
        # FIPS) pair is the canonical join key anyway.
        if state and co_fips:
            name = f"{state} county {str(co_fips).zfill(3)}"
            if is_msa:
                name = f"{name} (MSA {msa})"
        else:
            name = f"FIPS {st_fips or '??'}{co_fips or '???'}"
        rows.append({
            "respondent_id":        resp_id,
            "institution_name":     inst_name,
            "report_year":          int(year) if year is not None else None,
            "state":                state,
            "assessment_area_name": name,
            "area_type":            area_type,
            "county_fips":          str(co_fips).zfill(3) if co_fips else None,
            "msa_code":             str(msa) if msa else None,
        })

    conn.commit()
    conn.close()

    n = db.upsert_rows(
        "cra_assessment_areas", rows,
        unique_cols=["respondent_id", "report_year", "assessment_area_name"],
    )
    return n


def main():
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[1].strip())
    p.add_argument(
        "--years", type=int, nargs="+",
        help="Restrict to specific report years (default: all years in cra_sb_discl).",
    )
    args = p.parse_args()

    print("CD Command Center -- Derive cra_assessment_areas")
    if args.years:
        print(f"  Years: {args.years}")
    else:
        print(f"  Years: all in cra_sb_discl")
    print()

    n = derive(years=args.years)
    print()
    print(f"Done. Wrote {n:,} assessment area rows.")


if __name__ == "__main__":
    main()
