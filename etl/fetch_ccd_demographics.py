"""etl/fetch_ccd_demographics.py — NCES CCD-direct demographic backfill.

Replaces the Urban-Institute-based fetch_school_demographics.py with a
public-domain pipeline that downloads NCES Common Core of Data files directly
from nces.ed.gov. NCES CCD is published by the U.S. Department of Education and
carries no commercial-use restrictions.

Files used (per school year):
  - ccd_sch_052_<YY>_l_1a_<DATE>.zip — School Membership (race × sex × grade × school)
  - ccd_sch_129_<YY>_w_1a_<DATE>.zip — School Lunch (free/reduced-price eligibility)

Both files key on the 12-digit NCESSCH that matches our schools.nces_id column.

What gets populated:
  enrollment_history per (nces_id, school_year):
    enrollment, pct_black, pct_hispanic, pct_white, pct_asian,
    pct_multiracial, pct_free_reduced_lunch
  schools (latest available year only):
    same columns + data_year

ELL and SPED are NOT covered here — they live in EDFacts and IDEA Section 618
files respectively. Build a separate ETL for those when needed.

Usage:
    python etl/fetch_ccd_demographics.py                    # all schools, all years
    python etl/fetch_ccd_demographics.py --years 2023 2022  # specific SY-end years
    python etl/fetch_ccd_demographics.py --states GA TX     # filter by state
    python etl/fetch_ccd_demographics.py --skip-download    # use cached files only
    python etl/fetch_ccd_demographics.py --skip-schools-sync
"""

import argparse
import csv
import io
import os
import sys
import time
import zipfile

import requests

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import db


CCD_BASE = "https://nces.ed.gov/ccd/Data/zip"
RAW_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data", "raw", "ccd",
)

# Probed 2026-05-09 from nces.ed.gov. Each entry maps (school-year-end, file-series)
# to the exact published filename. Update annually when NCES releases a new SY.
# Series: 052 = Membership (race×sex×grade), 129 = Lunch (FRL eligibility).
CCD_FILES = {
    2023: {  # SY 2022-23
        "052": "ccd_sch_052_2223_l_1a_083023.zip",
        "129": "ccd_sch_129_2223_w_1a_083023.zip",
    },
    2022: {  # SY 2021-22
        "052": "ccd_sch_052_2122_l_1a_071722.zip",
        "129": "ccd_sch_129_2122_w_1a_071722.zip",
    },
    2021: {  # SY 2020-21
        "052": "ccd_sch_052_2021_l_1a_080621.zip",
        "129": "ccd_sch_129_2021_w_1a_080621.zip",
    },
    2020: {  # SY 2019-20
        "052": "ccd_sch_052_1920_l_1a_082120.zip",
        "129": "ccd_sch_129_1920_w_1a_082120.zip",
    },
}

# Race labels exactly as published in CCD Membership (RACE_ETHNICITY column)
RACE_BLACK = "Black or African American"
RACE_HISPANIC = "Hispanic/Latino"
RACE_WHITE = "White"
RACE_ASIAN = "Asian"
RACE_MULTI = "Two or more races"

DOWNLOAD_TIMEOUT = 600  # NCES files are big (200MB+); be patient


def _safe_pct(numerator, denominator, digits=1):
    if not denominator or denominator <= 0:
        return None
    return round(numerator / denominator * 100, digits)


# ---------------------------------------------------------------------------
# Download
# ---------------------------------------------------------------------------

def download_file(filename: str) -> str | None:
    """Download a CCD zip into RAW_DIR if not already present. Returns local path or None."""
    os.makedirs(RAW_DIR, exist_ok=True)
    dest = os.path.join(RAW_DIR, filename)
    if os.path.exists(dest) and os.path.getsize(dest) > 1_000_000:
        return dest

    url = f"{CCD_BASE}/{filename}"
    print(f"  Downloading {url} ...", flush=True)
    try:
        with requests.get(url, timeout=DOWNLOAD_TIMEOUT, stream=True) as resp:
            if resp.status_code != 200:
                print(f"    HTTP {resp.status_code} — skipping")
                return None
            with open(dest, "wb") as f:
                for chunk in resp.iter_content(chunk_size=131072):
                    f.write(chunk)
        size_mb = os.path.getsize(dest) / 1_048_576
        print(f"    Saved {size_mb:.1f} MB -> {dest}", flush=True)
        return dest
    except requests.RequestException as e:
        print(f"    Error: {e}")
        if os.path.exists(dest):
            os.remove(dest)
        return None


def open_csv_in_zip(zip_path: str):
    """Return a streaming csv.DictReader for the .csv inside a CCD zip."""
    z = zipfile.ZipFile(zip_path)
    csv_name = next((n for n in z.namelist() if n.lower().endswith(".csv")), None)
    if not csv_name:
        z.close()
        raise RuntimeError(f"No CSV inside {zip_path}")
    raw = z.open(csv_name)
    return csv.DictReader(io.TextIOWrapper(raw, encoding="latin-1")), z


# ---------------------------------------------------------------------------
# Parse — Membership (race)
# ---------------------------------------------------------------------------

def parse_membership(zip_path: str, ncessch_filter: set, states_filter: set | None):
    """
    Parse a CCD school-membership (052) zip file.

    Aggregates from `Category Set A` rows (race × sex × grade) — summing across
    sex and grade gives the per-race totals plus a school grand total. Avoids
    relying on Category Set B which is not always present in every release.

    Returns {ncessch: {pct_black, pct_hispanic, pct_white, pct_asian,
                        pct_multiracial, enrollment}}.
    """
    by_school = {}  # ncessch → {race_label: count, "_total": int}

    reader, z = open_csv_in_zip(zip_path)
    try:
        for row in reader:
            if row.get("TOTAL_INDICATOR", "").startswith("Category Set A") is False:
                continue
            ncessch = row.get("NCESSCH")
            if ncessch_filter and ncessch not in ncessch_filter:
                continue
            if states_filter and row.get("ST") not in states_filter:
                continue

            try:
                count = int(row.get("STUDENT_COUNT") or 0)
            except (TypeError, ValueError):
                count = 0
            if count < 0:
                count = 0

            race = row.get("RACE_ETHNICITY") or ""
            entry = by_school.setdefault(ncessch, {"_total": 0})
            entry[race] = entry.get(race, 0) + count
            entry["_total"] += count
    finally:
        z.close()

    out = {}
    for ncessch, counts in by_school.items():
        total = counts["_total"]
        out[ncessch] = {
            "enrollment":      total if total > 0 else None,
            "pct_black":       _safe_pct(counts.get(RACE_BLACK, 0),    total),
            "pct_hispanic":    _safe_pct(counts.get(RACE_HISPANIC, 0), total),
            "pct_white":       _safe_pct(counts.get(RACE_WHITE, 0),    total),
            "pct_asian":       _safe_pct(counts.get(RACE_ASIAN, 0),    total),
            "pct_multiracial": _safe_pct(counts.get(RACE_MULTI, 0),    total),
        }
    return out


# ---------------------------------------------------------------------------
# Parse — Lunch (FRL)
# ---------------------------------------------------------------------------

def parse_lunch(zip_path: str, ncessch_filter: set, states_filter: set | None):
    """
    Parse a CCD school-lunch (129) zip file.

    Returns {ncessch: pct_free_reduced_lunch}.

    The 129 file reports counts of students eligible for free, reduced-price,
    free+reduced, no-cost (CEP), or full-price lunch. We compute the standard
    "pct FRL" as (free + reduced) / membership when both are reported, falling
    back to the published "Free and Reduced-price Lunch Table" total if the
    component lines aren't broken out for a school.
    """
    free   = {}
    reduced = {}
    fr_total = {}
    membership_by_school = {}

    reader, z = open_csv_in_zip(zip_path)
    try:
        for row in reader:
            ncessch = row.get("NCESSCH")
            if ncessch_filter and ncessch not in ncessch_filter:
                continue
            if states_filter and row.get("ST") not in states_filter:
                continue

            try:
                count = int(row.get("STUDENT_COUNT") or 0)
            except (TypeError, ValueError):
                count = 0
            if count < 0:
                count = 0

            cat = (row.get("LUNCH_PROGRAM") or "").strip()
            ti  = (row.get("TOTAL_INDICATOR") or "").strip()

            # Membership total is published in the same file as a denominator.
            if ti.startswith("Education Unit Total") or cat == "No Category Codes":
                membership_by_school[ncessch] = max(membership_by_school.get(ncessch, 0), count)
            elif cat == "Free lunch qualified":
                free[ncessch] = count
            elif cat == "Reduced-price lunch qualified":
                reduced[ncessch] = count
            elif cat == "Free and Reduced-price lunch qualified":
                fr_total[ncessch] = count
    finally:
        z.close()

    out = {}
    all_ids = set(free) | set(reduced) | set(fr_total) | set(membership_by_school)
    for ncessch in all_ids:
        m = membership_by_school.get(ncessch)
        if m is None or m <= 0:
            continue
        if ncessch in fr_total:
            num = fr_total[ncessch]
        else:
            num = free.get(ncessch, 0) + reduced.get(ncessch, 0)
            if num == 0 and ncessch not in free and ncessch not in reduced:
                continue
        pct = _safe_pct(num, m)
        if pct is not None:
            if pct > 100:  # CEP / under-counted membership
                pct = 100.0
            out[ncessch] = pct
    return out


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def _has_demographic(rec):
    return any(
        rec.get(k) is not None for k in (
            "pct_black", "pct_hispanic", "pct_white", "pct_asian",
            "pct_multiracial", "pct_free_reduced_lunch",
        )
    )


def main():
    parser = argparse.ArgumentParser(description="NCES CCD-direct demographic backfill.")
    parser.add_argument("--years", type=int, nargs="+", default=sorted(CCD_FILES, reverse=True),
                        help=f"School-year-end years (default: {sorted(CCD_FILES, reverse=True)})")
    parser.add_argument("--states", nargs="+", help="Limit to these states (e.g. GA TX)")
    parser.add_argument("--skip-download", action="store_true",
                        help="Use only files already present in data/raw/ccd/")
    parser.add_argument("--skip-schools-sync", action="store_true",
                        help="Don't mirror latest year onto schools table")
    parser.add_argument("--charter-only", action="store_true",
                        help="Restrict to charter schools in the DB")
    args = parser.parse_args()

    # Pull every NCES ID currently in the schools table — we only write demographics
    # for schools we know about, so we can safely filter the CCD's millions of rows.
    schools_df = db.get_schools(states=args.states, charter_only=args.charter_only)
    if schools_df.empty:
        print("No matching schools in DB.")
        return
    ncessch_filter = set(schools_df["nces_id"].dropna().astype(str).str.zfill(12))
    states_filter  = set(s.upper() for s in args.states) if args.states else None

    print("CD Command Center — NCES CCD demographics")
    print(f"  Schools in DB to fill: {len(ncessch_filter):,}")
    print(f"  Years:                 {sorted(args.years, reverse=True)}")
    print(f"  States filter:         {sorted(states_filter) if states_filter else 'all'}")
    print()

    latest_per_school = {}  # ncessch → (year, record)
    history_rows = 0

    for year in sorted(args.years, reverse=True):
        if year not in CCD_FILES:
            print(f"  {year}: no known CCD URLs — skipping (update CCD_FILES dict)")
            continue
        files = CCD_FILES[year]
        print(f"  Year {year - 1}-{str(year)[-2:]}:")

        # ---- download / locate inputs ----
        membership_zip = lunch_zip = None
        if args.skip_download:
            cand = os.path.join(RAW_DIR, files["052"])
            membership_zip = cand if os.path.exists(cand) else None
            cand = os.path.join(RAW_DIR, files["129"])
            lunch_zip = cand if os.path.exists(cand) else None
        else:
            membership_zip = download_file(files["052"])
            lunch_zip      = download_file(files["129"])

        if not membership_zip:
            print(f"    Membership file missing — skipping year")
            continue

        # ---- parse ----
        print(f"    Parsing membership...", flush=True)
        race = parse_membership(membership_zip, ncessch_filter, states_filter)
        print(f"      {len(race):,} schools with race data")

        frl = {}
        if lunch_zip:
            print(f"    Parsing lunch...", flush=True)
            frl = parse_lunch(lunch_zip, ncessch_filter, states_filter)
            print(f"      {len(frl):,} schools with FRL data")

        # ---- merge + write per-school ----
        merged = {}
        for ncessch, rec in race.items():
            merged[ncessch] = dict(rec)
        for ncessch, pct in frl.items():
            merged.setdefault(ncessch, {})["pct_free_reduced_lunch"] = pct

        year_rows = 0
        for ncessch, rec in merged.items():
            if not _has_demographic(rec):
                continue
            row = {"nces_id": ncessch, "school_year": year, **rec}
            db.upsert_enrollment_history(row)
            year_rows += 1
            prev = latest_per_school.get(ncessch)
            if prev is None or year > prev[0]:
                latest_per_school[ncessch] = (year, rec)

        print(f"    Stored {year_rows:,} demographic rows for {year}")
        history_rows += year_rows

    print()
    print(f"Total enrollment_history rows written: {history_rows:,}")
    print(f"Schools with at least one populated year: {len(latest_per_school):,}")

    if args.skip_schools_sync:
        return

    print()
    print("Mirroring most recent year onto schools table...")
    synced = 0
    for ncessch, (year, rec) in latest_per_school.items():
        db.update_school_fields(ncessch, {
            "data_year":              year,
            "enrollment":             rec.get("enrollment"),
            "pct_black":              rec.get("pct_black"),
            "pct_hispanic":           rec.get("pct_hispanic"),
            "pct_white":              rec.get("pct_white"),
            "pct_asian":              rec.get("pct_asian"),
            "pct_multiracial":        rec.get("pct_multiracial"),
            "pct_free_reduced_lunch": rec.get("pct_free_reduced_lunch"),
        })
        synced += 1
    print(f"Schools rows updated: {synced:,}")


if __name__ == "__main__":
    main()
