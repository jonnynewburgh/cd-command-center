"""etl/fetch_crdc_ell_sped.py — CRDC English-Learner and IDEA-served rates per school.

Reads public-use Civil Rights Data Collection ZIPs from data/raw/crdc/. The
files must be downloaded manually from https://ocrdata.ed.gov/downloads
(the portal is an Angular SPA so URLs aren't scrapable). Place ZIPs as-is.

Source license: CRDC is a U.S. Department of Education public-use dataset
and carries no commercial-use restrictions.

What gets populated:
  enrollment_history per (nces_id, school_year):
    pct_ell, pct_sped
  schools (latest CRDC year available):
    pct_ell, pct_sped

Format coverage:
  Modern (2015-16, 2017-18, 2020-21, 2021-22): single wide CSV (`Enrollment.csv`
  or `... School Data.csv`) with per-sex per-race columns. This script handles
  these layouts.

  Older (2009-10, 2011-12, 2013-14): one XLSX per topic. NOT supported here;
  defer until those years are actually needed.

Column conventions across modern releases:
  - Denominator: TOT_ENR_M + TOT_ENR_F (+ TOT_ENR_X where present)
  - EL/LEP numerator: SCH_ENR_EL_* (2017-18+) or SCH_ENR_LEP_* (2015-16)
  - IDEA numerator: SCH_ENR_IDEA_*
  - School key: COMBOKEY (12-digit; equals our schools.nces_id)

Negative values in CRDC are suppression codes (-3 = skip logic, -5 / -8 = not
collected, -6 = force-zero, -9 = not applicable, -11 = suppressed). Treated as 0.

Usage:
    python etl/fetch_crdc_ell_sped.py                    # all CRDC zips found
    python etl/fetch_crdc_ell_sped.py --years 2022 2021  # only specific year-ends
    python etl/fetch_crdc_ell_sped.py --skip-schools-sync
"""

import argparse
import csv
import io
import os
import sys
import zipfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import db

try:
    import zipfile_deflate64 as _zf
except ImportError:
    _zf = zipfile


RAW_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data", "raw", "crdc",
)

# Each CRDC release maps a school-year-end (e.g. 2022 for SY 2021-22) to:
#   - zip filename in data/raw/crdc/
#   - the inner path of the school-level enrollment CSV
# Update when new CRDC releases land. Older XLSX-only releases are intentionally
# omitted.
CRDC_RELEASES = {
    2022: {
        "zip":   "2021-22-crdc-data.zip",
        "entry": "SCH/Enrollment.csv",
        "el_prefix":   "SCH_ENR_EL",    # SCH_ENR_EL_M, _F, _X
        "idea_prefix": "SCH_ENR_IDEA",
    },
    2021: {
        "zip":   "2020-21-crdc-data.zip",
        "entry": "CRDC/School/Enrollment.csv",
        "el_prefix":   "SCH_ENR_EL",
        "idea_prefix": "SCH_ENR_IDEA",
    },
    2018: {
        "zip":   "2017-18-crdc-data.zip",
        "entry": "2017-18-crdc-data-corrected-publication 2/2017-18 Public-Use Files/Data/SCH/CRDC/CSV/Enrollment.csv",
        "el_prefix":   "SCH_ENR_LEP",   # 2017-18 still used "LEP" naming
        "idea_prefix": "SCH_ENR_IDEA",
    },
    2016: {
        "zip":   "2015-16-crdc-data.zip",
        "entry": "Data Files and Layouts/CRDC 2015-16 School Data.csv",
        "el_prefix":   "SCH_ENR_LEP",
        "idea_prefix": "SCH_ENR_IDEA",
    },
}

# Sex suffixes present across releases. Older years lack the nonbinary "X".
SEX_SUFFIXES = ("M", "F", "X")


def _safe_int(v):
    """Return int >= 0; CRDC suppression codes are all negative and treated as 0."""
    try:
        n = int(v)
    except (TypeError, ValueError):
        return 0
    return n if n > 0 else 0


def _sum_sex(row, prefix):
    """Sum across sex suffixes for a column prefix (e.g. SCH_ENR_EL → _M, _F, _X)."""
    total = 0
    for suf in SEX_SUFFIXES:
        col = f"{prefix}_{suf}"
        if col in row:
            total += _safe_int(row.get(col))
    return total


def _safe_pct(numerator, denominator):
    if not denominator or denominator <= 0:
        return None
    pct = round(numerator / denominator * 100, 1)
    if pct > 100:
        pct = 100.0
    if pct < 0:
        pct = None
    return pct


def open_csv_in_zip(zip_path, entry):
    """Open the named entry in a possibly-deflate64 zip and return a csv.DictReader + closeables list."""
    z = _zf.ZipFile(zip_path)
    if entry not in z.namelist():
        # Some releases put the file under variant paths — fall back to suffix match.
        candidates = [n for n in z.namelist() if n.endswith(entry.split("/")[-1])]
        if not candidates:
            z.close()
            raise RuntimeError(f"{entry} not found in {zip_path}; got {z.namelist()[:5]}...")
        entry = candidates[0]
    raw = z.open(entry)
    # CRDC public-use CSVs are latin-1 (windows-1252 is also safe here).
    reader = csv.DictReader(io.TextIOWrapper(raw, encoding="latin-1"))
    return reader, [z, raw]


def parse_release(zip_path, entry, el_prefix, idea_prefix, ncessch_filter):
    """Return {ncessch: {pct_ell, pct_sped, enrollment_crdc}}.

    enrollment_crdc is included for traceability — CRDC counts can differ from
    CCD because of reporting cycles and grade coverage, so we keep it visible.
    """
    out = {}
    reader, closeables = open_csv_in_zip(zip_path, entry)
    try:
        for row in reader:
            ncessch = (row.get("COMBOKEY") or "").strip()
            if not ncessch:
                continue
            ncessch = ncessch.zfill(12)
            if ncessch_filter and ncessch not in ncessch_filter:
                continue

            total = _sum_sex(row, "TOT_ENR")
            if total <= 0:
                continue
            el_n   = _sum_sex(row, el_prefix)
            idea_n = _sum_sex(row, idea_prefix)

            out[ncessch] = {
                "enrollment_crdc": total,
                "pct_ell":         _safe_pct(el_n,   total),
                "pct_sped":        _safe_pct(idea_n, total),
            }
    finally:
        for c in reversed(closeables):
            try:
                c.close()
            except Exception:
                pass
    return out


def main():
    parser = argparse.ArgumentParser(description="Load ELL/SPED per school from CRDC public-use files.")
    parser.add_argument("--years", type=int, nargs="+",
                        default=sorted(CRDC_RELEASES.keys(), reverse=True),
                        help=f"School-year-end years to load (default: {sorted(CRDC_RELEASES, reverse=True)})")
    parser.add_argument("--skip-schools-sync", action="store_true",
                        help="Don't mirror latest year onto schools table")
    args = parser.parse_args()

    # Schools currently in DB — we only write demographics for known schools.
    schools_df = db.get_schools()
    ncessch_filter = set(schools_df["nces_id"].dropna().astype(str).str.zfill(12))

    print("CD Command Center — CRDC ELL/SPED loader")
    print(f"  Schools in DB to fill: {len(ncessch_filter):,}")
    print(f"  Years: {sorted(args.years, reverse=True)}")
    print()

    latest_per_school = {}
    total_rows = 0

    for year in sorted(args.years, reverse=True):
        meta = CRDC_RELEASES.get(year)
        if not meta:
            print(f"  {year}: no release configured — skipping")
            continue
        zip_path = os.path.join(RAW_DIR, meta["zip"])
        if not os.path.exists(zip_path):
            print(f"  {year}: {meta['zip']} not found in data/raw/crdc/ — skipping")
            continue

        print(f"  Year {year - 1}-{str(year)[-2:]}: parsing {meta['zip']}...", flush=True)
        recs = parse_release(zip_path, meta["entry"], meta["el_prefix"], meta["idea_prefix"], ncessch_filter)
        print(f"    {len(recs):,} schools with ELL/SPED data")

        year_rows = 0
        for ncessch, rec in recs.items():
            if rec.get("pct_ell") is None and rec.get("pct_sped") is None:
                continue
            db.upsert_enrollment_history({
                "nces_id":     ncessch,
                "school_year": year,
                "pct_ell":     rec["pct_ell"],
                "pct_sped":    rec["pct_sped"],
            })
            year_rows += 1
            prev = latest_per_school.get(ncessch)
            if prev is None or year > prev[0]:
                latest_per_school[ncessch] = (year, rec)
        print(f"    Stored {year_rows:,} ELL/SPED rows for {year}")
        total_rows += year_rows

    print()
    print(f"Total enrollment_history rows written: {total_rows:,}")
    print(f"Schools with at least one CRDC year: {len(latest_per_school):,}")

    if args.skip_schools_sync:
        return

    # Pull the per-school latest year directly from enrollment_history so
    # running this script with --years <subset> can never clobber a fresher
    # value already on the schools table. Source of truth is the history table.
    print()
    print("Mirroring most recent CRDC year onto schools table (from enrollment_history)...")
    conn = db.get_connection()
    cur = conn.cursor()
    cur.execute(db.adapt_sql(
        """
        SELECT DISTINCT ON (nces_id) nces_id, pct_ell, pct_sped
        FROM enrollment_history
        WHERE pct_ell IS NOT NULL OR pct_sped IS NOT NULL
        ORDER BY nces_id, school_year DESC
        """
    ))
    rows = cur.fetchall()
    conn.close()
    synced = 0
    for ncessch, pct_ell, pct_sped in rows:
        # Do NOT overwrite data_year here — that field reflects the latest
        # CCD race/FRL year, typically newer than the CRDC biennial cadence.
        db.update_school_fields(ncessch, {
            "pct_ell":  pct_ell,
            "pct_sped": pct_sped,
        })
        synced += 1
    print(f"Schools rows updated: {synced:,}")


if __name__ == "__main__":
    main()
