"""
etl/fetch_edfacts_auto.py — Auto-download EDFacts federal data files from Ed.gov
and load them into the lea_accountability table.

Data source: US Department of Education EDFacts
  https://www2.ed.gov/about/inits/ed/edfacts/data-files/index.html
  Public domain — no commercial use restrictions.

Three file types are downloaded per school year:
  1. Math proficiency by LEA
  2. Reading/language arts (RLA) proficiency by LEA
  3. Adjusted cohort graduation rate (ACGR) by LEA

Files are saved to data/raw/ so you can re-run the load without re-downloading.
Parsing is handled by the existing fetch_edfacts.py logic.

Usage:
    # Download and load the most recent available year (default: 2023)
    python etl/fetch_edfacts_auto.py

    # Specific year
    python etl/fetch_edfacts_auto.py --year 2023

    # Multiple years
    python etl/fetch_edfacts_auto.py --years 2021 2022 2023

    # Specific states only
    python etl/fetch_edfacts_auto.py --year 2023 --states GA TX CA

    # Download only, skip loading
    python etl/fetch_edfacts_auto.py --year 2023 --download-only

    # Load from already-downloaded files (skip download)
    python etl/fetch_edfacts_auto.py --year 2023 --no-download
"""

import argparse
import os
import sys
import time

import requests

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import db

# Import parsing + loading functions from the existing script
from etl.fetch_edfacts import load_math_file, load_rla_file, load_grad_file, merge_and_load

RAW_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "raw")

# ---------------------------------------------------------------------------
# Ed.gov URL patterns for EDFacts data files.
#
# Ed.gov uses a consistent naming convention, but the exact slug has shifted
# slightly between releases.  We try a prioritized list of URL templates for
# each file type and take the first that returns HTTP 200.
#
# URL template placeholders:
#   {YY1} = last two digits of the start year (e.g. "22" for 2022-23)
#   {YY2} = last two digits of the end year   (e.g. "23" for 2022-23)
#   {YYYY1} = full start year                 (e.g. "2022")
#   {YYYY2} = full end year                   (e.g. "2023")
# ---------------------------------------------------------------------------

_BASE = "https://www2.ed.gov/about/inits/ed/edfacts/data-files"

MATH_URL_TEMPLATES = [
    f"{_BASE}/math-achievement-lea-sy{{YYYY1}}-{{YY2}}.csv",
    f"{_BASE}/math-achievement-lea-sy{{YY1}}-{{YY2}}.csv",
    f"{_BASE}/math-proficiency-lea-sy{{YYYY1}}-{{YY2}}.csv",
    f"{_BASE}/rla-math-achievement-lea-sy{{YYYY1}}-{{YY2}}.zip",
]

RLA_URL_TEMPLATES = [
    f"{_BASE}/rla-achievement-lea-sy{{YYYY1}}-{{YY2}}.csv",
    f"{_BASE}/rla-achievement-lea-sy{{YY1}}-{{YY2}}.csv",
    f"{_BASE}/reading-achievement-lea-sy{{YYYY1}}-{{YY2}}.csv",
]

GRAD_URL_TEMPLATES = [
    f"{_BASE}/acgr-lea-and-state-sy{{YYYY1}}-{{YY2}}.csv",
    f"{_BASE}/acgr-lea-sy{{YYYY1}}-{{YY2}}.csv",
    f"{_BASE}/adj-cohort-grad-rate-lea-sy{{YYYY1}}-{{YY2}}.csv",
]

TIMEOUT = 60  # seconds per download attempt


def _format_url(template: str, year: int) -> str:
    """Fill URL template placeholders for a given school-year-end (e.g. year=2023 → 2022-23)."""
    start = year - 1
    return template.format(
        YYYY1=str(start),
        YYYY2=str(year),
        YY1=str(start)[-2:],
        YY2=str(year)[-2:],
    )


def _try_download(templates: list, year: int, dest_path: str, label: str) -> bool:
    """
    Try each URL template in order.  Save to dest_path on success.
    Returns True if a file was downloaded, False if all URLs failed.
    """
    for template in templates:
        url = _format_url(template, year)
        print(f"  Trying {url} ...")
        try:
            resp = requests.get(url, timeout=TIMEOUT, stream=True)
            if resp.status_code == 200:
                content_type = resp.headers.get("Content-Type", "")
                # Skip HTML error pages (Ed.gov returns 200 with HTML for 404s sometimes)
                if "text/html" in content_type and b"<html" in resp.content[:200]:
                    print(f"    Skipped — server returned an HTML page (not a CSV)")
                    continue
                os.makedirs(os.path.dirname(dest_path), exist_ok=True)
                with open(dest_path, "wb") as f:
                    for chunk in resp.iter_content(chunk_size=65536):
                        f.write(chunk)
                size_kb = os.path.getsize(dest_path) // 1024
                print(f"    Downloaded {label} ({size_kb} KB) → {dest_path}")
                return True
            else:
                print(f"    HTTP {resp.status_code}")
        except requests.RequestException as e:
            print(f"    Error: {e}")
        time.sleep(0.5)
    return False


def download_year(year: int, raw_dir: str) -> dict:
    """
    Download the three EDFacts files for a given year.
    Returns a dict with keys math/rla/grad mapping to local paths (or None).
    """
    start = year - 1
    suffix = f"sy{start}-{str(year)[-2:]}"

    paths = {
        "math": os.path.join(raw_dir, f"edfacts_math_{suffix}.csv"),
        "rla":  os.path.join(raw_dir, f"edfacts_rla_{suffix}.csv"),
        "grad": os.path.join(raw_dir, f"edfacts_grad_{suffix}.csv"),
    }

    result = {}

    for key, templates, label in [
        ("math", MATH_URL_TEMPLATES, "Math proficiency"),
        ("rla",  RLA_URL_TEMPLATES,  "Reading/RLA proficiency"),
        ("grad", GRAD_URL_TEMPLATES, "Graduation rate"),
    ]:
        dest = paths[key]
        if os.path.exists(dest):
            print(f"  {label}: already downloaded → {dest}")
            result[key] = dest
            continue

        ok = _try_download(templates, year, dest, label)
        result[key] = dest if ok else None

    return result


def load_year(paths: dict, year: int, states: list = None):
    """Load the downloaded files into the database using fetch_edfacts.py parsers."""
    import pandas as pd

    math_df = load_math_file(paths["math"], year, states) if paths.get("math") else pd.DataFrame()
    rla_df  = load_rla_file(paths["rla"],   year, states) if paths.get("rla")  else pd.DataFrame()
    grad_df = load_grad_file(paths["grad"],  year, states) if paths.get("grad") else pd.DataFrame()

    if math_df.empty and rla_df.empty and grad_df.empty:
        print(f"  No data to load for {year}.")
        return 0, 0

    return merge_and_load(math_df, rla_df, grad_df, year)


def main():
    parser = argparse.ArgumentParser(
        description="Auto-download EDFacts data from Ed.gov and load into lea_accountability"
    )
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--year",  type=int, default=2023,
                       help="School year ending (default: 2023 = 2022-23)")
    group.add_argument("--years", type=int, nargs="+",
                       help="Load multiple years, e.g. --years 2021 2022 2023")
    parser.add_argument("--states", nargs="+",
                        help="Only load these states (e.g. --states GA TX CA)")
    parser.add_argument("--raw-dir", default=RAW_DIR,
                        help=f"Directory for downloaded files (default: {RAW_DIR})")
    parser.add_argument("--download-only", action="store_true",
                        help="Download files but skip loading into the database")
    parser.add_argument("--no-download", action="store_true",
                        help="Use already-downloaded files; skip download step")
    args = parser.parse_args()

    years = args.years if args.years else [args.year]

    if not args.no_download:
        db.init_db()

    total_loaded = 0
    total_errors = 0

    for year in years:
        start = year - 1
        print(f"\n{'='*60}")
        print(f"School year {start}-{str(year)[-2:]}  (year={year})")
        print(f"{'='*60}")

        if args.no_download:
            # Build expected paths without downloading
            suffix = f"sy{start}-{str(year)[-2:]}"
            paths = {
                "math": os.path.join(args.raw_dir, f"edfacts_math_{suffix}.csv"),
                "rla":  os.path.join(args.raw_dir, f"edfacts_rla_{suffix}.csv"),
                "grad": os.path.join(args.raw_dir, f"edfacts_grad_{suffix}.csv"),
            }
            # Only include paths that actually exist
            paths = {k: v for k, v in paths.items() if os.path.exists(v)}
            if not paths:
                print(f"  No downloaded files found for {year} in {args.raw_dir}")
                continue
        else:
            paths = download_year(year, args.raw_dir)

        if args.download_only:
            downloaded = sum(1 for v in paths.values() if v)
            print(f"  Download-only mode: {downloaded}/3 files downloaded")
            continue

        loaded, errors = load_year(paths, year, states=args.states)
        total_loaded += loaded
        total_errors += errors

    if not args.download_only:
        print(f"\nAll years complete: {total_loaded} LEA records loaded, {total_errors} errors")


if __name__ == "__main__":
    main()
