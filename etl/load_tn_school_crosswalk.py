"""Load tn_school_crosswalk from NCES CCD school directory files.

Source: data/raw/ccd/ccd_sch_129_*.zip

Each year's CCD file contains, for every TN school:
    ST_SCHID  = "TN-{system_id:05d}-{school_id:04d}"   (TDOE state-native ID)
    NCESSCH   = NCES 12-digit school id
    SCHOOL_YEAR = "YYYY-YYYY"                          (end-year is the canonical year)

We load every available CCD file (one etl_load_log row per file), then collapse
the (system_id, school_id, ncessch) observations into validity bands:

  year_valid_start = first CCD end-year the (system, school, ncessch) tuple appeared
  year_valid_end   = NULL if it persists into the most recent loaded CCD year,
                     else the last CCD end-year it appeared

A school that gets renumbered (different NCESSCH for the same TDOE ID pair)
yields two rows differentiated by year_valid_start — the schema PK
(system_id, school_id, year_valid_start) supports this naturally.

source_load_id on each crosswalk row points at the CCD file that first
introduced the tuple. Re-runs are idempotent: TRUNCATE + reload.

Postgres-only (matches the rest of the TN pipeline).

Usage:
    python etl/load_tn_school_crosswalk.py                 # all CCD files
    python etl/load_tn_school_crosswalk.py --truncate      # wipe table first
    python etl/load_tn_school_crosswalk.py --file <name>   # one file only
"""
from __future__ import annotations

import argparse
import logging
import re
import sys
import zipfile
from collections import defaultdict
from pathlib import Path

import pandas as pd
from psycopg2.extras import execute_values

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import db
from db import DATABASE_URL
from etl.state_accountability.log import insert_success_log, sha256_file

CCD_DIR = Path("data/raw/ccd")
CCD_PATTERN = "ccd_sch_129_*.zip"
FILE_KIND = "ccd_sch_129_directory"

# CCD ST_SCHID for TN: "TN-{system:05d}-{school:04d}"
_ST_SCHID_RE = re.compile(r"^TN-(\d+)-(\d+)$")

# CCD SCHOOL_YEAR is "2022-2023"; canonical year is the end-year (2023).
_SCHOOL_YEAR_RE = re.compile(r"^(\d{4})-(\d{4})$")


logger = logging.getLogger(__name__)


def _parse_school_year(s: str) -> int:
    m = _SCHOOL_YEAR_RE.match(s.strip())
    if not m:
        raise ValueError(f"Unrecognized SCHOOL_YEAR: {s!r}")
    return int(m.group(2))


def _read_ccd_tn(filepath: Path) -> pd.DataFrame:
    """Open a ccd_sch_129 zip, return TN rows with the four columns we need."""
    with zipfile.ZipFile(filepath) as z:
        csv_name = next((n for n in z.namelist() if n.endswith(".csv")), None)
        if csv_name is None:
            raise ValueError(f"No CSV inside {filepath.name}")
        with z.open(csv_name) as f:
            df = pd.read_csv(
                f,
                dtype=str,
                encoding="latin-1",
                usecols=["ST", "SCHOOL_YEAR", "ST_SCHID", "NCESSCH"],
            )
    tn = df[df["ST"] == "TN"].copy()
    return tn


def _ingest_one_file(cur, filepath: Path) -> tuple[int, list[tuple[int, int, int, str]]]:
    """Read one CCD file → (load_id, list of (year, system_id, school_id, ncessch)).

    Inserts the etl_load_log row; the caller commits/rolls back.
    """
    file_hash = sha256_file(filepath)
    df = _read_ccd_tn(filepath)
    if df.empty:
        raise ValueError(f"{filepath.name}: no TN rows")

    observations: list[tuple[int, int, int, str]] = []
    bad_schid = 0
    for _, r in df.iterrows():
        st_schid = (r["ST_SCHID"] or "").strip()
        m = _ST_SCHID_RE.match(st_schid)
        if not m:
            bad_schid += 1
            continue
        ncessch = (r["NCESSCH"] or "").strip()
        if not ncessch:
            continue
        year = _parse_school_year(r["SCHOOL_YEAR"])
        observations.append((year, int(m.group(1)), int(m.group(2)), ncessch))

    if bad_schid:
        logger.warning(f"{filepath.name}: {bad_schid} TN rows had unparseable ST_SCHID")

    load_id = insert_success_log(
        cur,
        source_file=filepath.name,
        source_file_hash=file_hash,
        source_file_kind=FILE_KIND,
        row_counts_by_table={"_observations": len(observations)},
        notes=f"observations={len(observations)}; unparseable_st_schid={bad_schid}",
    )
    return load_id, observations


def _collapse_to_bands(
    obs_with_load: list[tuple[int, int, int, str, int]],
    latest_loaded_year: int,
) -> list[dict]:
    """Collapse year-stamped observations into validity bands.

    Input: list of (year, system_id, school_id, ncessch, load_id)
    Output: list of {tdoe_system_id, tdoe_school_id, ncessch, year_valid_start,
                     year_valid_end, source_load_id} dicts

    One band per distinct (system_id, school_id, ncessch). year_valid_end is
    NULL if the band extends into the most recent loaded CCD year, else the
    last year that triple was observed.
    """
    grouped: dict[tuple[int, int, str], list[tuple[int, int]]] = defaultdict(list)
    for year, sys_id, sch_id, ncessch, load_id in obs_with_load:
        grouped[(sys_id, sch_id, ncessch)].append((year, load_id))

    bands: list[dict] = []
    for (sys_id, sch_id, ncessch), entries in grouped.items():
        entries.sort()
        years = [y for y, _ in entries]
        first_year, first_load = entries[0]
        last_year = years[-1]
        bands.append({
            "tdoe_system_id":   sys_id,
            "tdoe_school_id":   sch_id,
            "ncessch":          ncessch,
            "year_valid_start": first_year,
            "year_valid_end":   None if last_year >= latest_loaded_year else last_year,
            "source_load_id":   first_load,
        })
    return bands


def _insert_bands(cur, bands: list[dict]) -> int:
    if not bands:
        return 0
    cols = ["tdoe_system_id", "tdoe_school_id", "ncessch",
            "year_valid_start", "year_valid_end", "source_load_id"]
    values = [tuple(b[c] for c in cols) for b in bands]
    execute_values(
        cur,
        f"INSERT INTO tn_school_crosswalk ({','.join(cols)}) VALUES %s",
        values,
    )
    return len(values)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dir", type=Path, default=CCD_DIR,
                        help=f"directory of CCD school directory zips (default: {CCD_DIR})")
    parser.add_argument("--file", type=str, default=None,
                        help="load only this one CCD file (filename in --dir)")
    parser.add_argument("--truncate", action="store_true",
                        help="TRUNCATE tn_school_crosswalk before loading "
                             "(re-runs are idempotent only with this flag — the "
                             "schema PK rejects conflicting (system,school,year_start) tuples)")
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    if not args.dir.exists():
        logger.error(f"CCD directory not found: {args.dir}")
        return 2

    if args.file:
        files = [args.dir / args.file]
        if not files[0].exists():
            logger.error(f"file not found: {files[0]}")
            return 2
    else:
        files = sorted(args.dir.glob(CCD_PATTERN))
    if not files:
        logger.error(f"no files matching {CCD_PATTERN} in {args.dir}")
        return 2

    if not DATABASE_URL.startswith(("postgres://", "postgresql://")):
        logger.error("This loader requires Postgres. Set DATABASE_URL.")
        return 2

    conn = db.get_connection()
    try:
        cur = conn.cursor()

        if args.truncate:
            cur.execute("TRUNCATE tn_school_crosswalk")
            logger.info("TRUNCATE tn_school_crosswalk")

        # Ingest every file inside ONE transaction so the load_log rows and
        # the crosswalk rows commit together (same atomicity guarantee the
        # state_accountability runner provides per-file).
        all_obs: list[tuple[int, int, int, str, int]] = []  # (year, sys, sch, ncessch, load_id)
        for filepath in files:
            logger.info(f"reading {filepath.name}")
            load_id, observations = _ingest_one_file(cur, filepath)
            for year, sys_id, sch_id, ncessch in observations:
                all_obs.append((year, sys_id, sch_id, ncessch, load_id))
            logger.info(f"  → load_id={load_id}, observations={len(observations)}")

        if not all_obs:
            logger.error("no observations collected from any file — aborting")
            conn.rollback()
            return 1

        latest_loaded_year = max(o[0] for o in all_obs)
        logger.info(f"latest CCD end-year loaded: {latest_loaded_year}")

        bands = _collapse_to_bands(all_obs, latest_loaded_year)
        inserted = _insert_bands(cur, bands)
        conn.commit()

        # Quick sanity check: how many distinct TVAAS schools now have a
        # crosswalk row?
        cur.execute("""
            SELECT COUNT(DISTINCT (t.tdoe_system_id, t.tdoe_school_id))
            FROM tn_tvaas_school_composite t
            JOIN tn_school_crosswalk c
              ON c.tdoe_system_id = t.tdoe_system_id
             AND c.tdoe_school_id = t.tdoe_school_id
        """)
        matched = cur.fetchone()[0]
        cur.execute("SELECT COUNT(DISTINCT (tdoe_system_id, tdoe_school_id)) FROM tn_tvaas_school_composite")
        total = cur.fetchone()[0]

        logger.info(
            f"done. files={len(files)}, "
            f"bands={inserted}, "
            f"TVAAS schools with crosswalk = {matched}/{total} "
            f"({100*matched/total:.1f}%)"
        )
        return 0

    except Exception:
        conn.rollback()
        logger.exception("crosswalk load failed; rolled back")
        return 1
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
