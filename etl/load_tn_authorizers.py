"""Seed TN charter authorizers + school-authorizer links.

Source: the `schools` table itself. In NCES data, every TN charter's `lea_id`
already identifies its authorizer — local LEAs authorize most charters, while
TPCSC and ASD show up as their own LEA records.

Authorizer kind assignment (TN-specific):
    Tennessee Public Charter School Commission → ICB
    Achievement School District                → ICB  (state-created chartering
                                                       body; appears as its own
                                                       LEA in NCES, but functions
                                                       as an independent authorizer)
    everything else                            → LEA

Idempotent: re-runs are safe (both target tables are upserted by unique key).

Usage:
    python etl/load_tn_authorizers.py [--dry-run]
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import db

SOURCE_SYSTEM = "tn_charter_seed"
SCHOOL_YEAR = "2023-24"  # NCES data_year=2024 maps to SY 2023-24

# TN special cases. Everything else defaults to 'LEA'.
_KIND_OVERRIDES = {
    "Tennessee Public Charter School Commission": "ICB",
    "Achievement School District":                "ICB",
}

logger = logging.getLogger(__name__)


def _kind_for(lea_name: str) -> str:
    return _KIND_OVERRIDES.get(lea_name, "LEA")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true",
                        help="report what would be loaded without writing")
    parser.add_argument("--school-year", default=SCHOOL_YEAR,
                        help=f"school year for school_authorizer rows (default {SCHOOL_YEAR})")
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    conn = db.get_connection()
    cur = conn.cursor()

    # 1) Distinct (lea_id, lea_name) pairs over TN charters.
    cur.execute("""
        SELECT lea_id, lea_name, COUNT(*) AS n_schools
          FROM schools
         WHERE state='TN' AND is_charter=1
           AND lea_id IS NOT NULL AND lea_name IS NOT NULL
         GROUP BY lea_id, lea_name
         ORDER BY n_schools DESC
    """)
    lea_rows = cur.fetchall()
    logger.info(f"found {len(lea_rows)} distinct TN charter LEAs covering "
                f"{sum(r[2] for r in lea_rows)} schools")

    # 2) Upsert authorizers; build (lea_id → authorizer_id) map.
    lea_to_auth_id: dict[str, int] = {}
    for lea_id, lea_name, n_schools in lea_rows:
        kind = _kind_for(lea_name)
        rec = {
            "state": "TN",
            "name": lea_name,
            "authorizer_kind": kind,
            "nces_lea_id": lea_id,
            "source_system": SOURCE_SYSTEM,
            "is_active": 1,
        }
        logger.info(f"  {kind:3} {lea_name} (lea_id={lea_id}, schools={n_schools})")
        if not args.dry_run:
            db.upsert_authorizer(rec)

    if args.dry_run:
        # In dry-run, skip the lookups for ids and link counting.
        cur.execute("SELECT COUNT(*) FROM schools WHERE state='TN' AND is_charter=1 AND nces_id IS NOT NULL")
        n_charters = cur.fetchone()[0]
        logger.info(f"DRY RUN: would also link {n_charters} TN charters to their LEA authorizer")
        conn.close()
        return 0

    # 3) Resolve authorizer ids for the upserted rows.
    cur.execute("SELECT id, nces_lea_id FROM authorizers WHERE state='TN'")
    for auth_id, lea_id in cur.fetchall():
        if lea_id:
            lea_to_auth_id[lea_id] = auth_id

    # 4) Insert one school_authorizer row per TN charter.
    cur.execute("""
        SELECT nces_id, lea_id, school_name
          FROM schools
         WHERE state='TN' AND is_charter=1
           AND nces_id IS NOT NULL AND lea_id IS NOT NULL
    """)
    schools = cur.fetchall()

    linked, missing = 0, 0
    for nces_id, lea_id, school_name in schools:
        auth_id = lea_to_auth_id.get(lea_id)
        if auth_id is None:
            logger.warning(f"  no authorizer for lea_id={lea_id} school={school_name!r}")
            missing += 1
            continue
        rec = {
            "nces_school_id": nces_id,
            "authorizer_id":  auth_id,
            "school_year":    args.school_year,
            "relationship":   "authorizer",
            "source_system":  SOURCE_SYSTEM,
        }
        db.upsert_school_authorizer(rec)
        linked += 1

    conn.close()
    logger.info(f"done. {len(lea_rows)} authorizers upserted, "
                f"{linked} school links written, {missing} unlinked")
    return 0


if __name__ == "__main__":
    sys.exit(main())
