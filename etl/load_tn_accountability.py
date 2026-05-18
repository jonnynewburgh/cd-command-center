"""Entry point for loading TN charter accountability files.

Usage:
    python -m etl.load_tn_accountability [--dir DIR] [--file FILE] [--force]

Defaults:
    DIR = data/raw/charter accountability/TN/

By default, iterates every recognized file in the raw dir and loads each one.
Files already loaded (matched by sha256) are skipped. --force re-loads,
chaining parent_load_id and deleting prior fact rows.

Single-file mode: --file <name> processes one file (must live in --dir).
"""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

import psycopg2

from db import DATABASE_URL
from etl.state_accountability.runner import load_file
from etl.state_accountability.tn.handlers import HANDLERS
from etl.state_accountability.tn.validators import VALIDATORS_BY_KIND
from etl.tn_filename_parser import UnknownFilenameError, parse_filename

DEFAULT_RAW_DIR = Path("data/raw/charter accountability/TN")

logger = logging.getLogger(__name__)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dir", type=Path, default=DEFAULT_RAW_DIR)
    parser.add_argument("--file", type=str, default=None,
                        help="load only this filename (in --dir)")
    parser.add_argument("--force", action="store_true",
                        help="re-load files even if a successful load with the same hash exists")
    parser.add_argument("--stop-on-error", dest="stop_on_error", action="store_true", default=True,
                        help="bail on the first non-validation error (default during dev). "
                             "Use --continue-on-error to flip.")
    parser.add_argument("--continue-on-error", dest="stop_on_error", action="store_false",
                        help="log and continue past failures instead of bailing")
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    if not args.dir.exists():
        logger.error(f"raw dir not found: {args.dir}")
        return 2

    if args.file:
        candidates = [args.dir / args.file]
        if not candidates[0].exists():
            logger.error(f"file not found: {candidates[0]}")
            return 2
    else:
        candidates = sorted(
            p for p in args.dir.iterdir()
            if p.is_file() and p.suffix.lower() in (".csv", ".xlsx")
        )

    if not DATABASE_URL.startswith(("postgres://", "postgresql://")):
        logger.error("This loader requires Postgres. Set DATABASE_URL.")
        return 2

    conn = psycopg2.connect(DATABASE_URL)
    try:
        results = {"success": 0, "already_loaded": 0, "skipped": 0, "failed": 0}
        for filepath in candidates:
            try:
                parsed = parse_filename(filepath.name)
            except UnknownFilenameError as exc:
                logger.error(f"{filepath.name}: {exc}")
                results["failed"] += 1
                if args.stop_on_error:
                    logger.error("--stop-on-error: bailing")
                    break
                continue

            # Per-file try/except: runner's job is "load this file or fail clearly";
            # CLI's job is "load all files, report what happened". Validation
            # failures already return LoadResult(status='failed'); other exceptions
            # re-raise from the runner and we catch here.
            try:
                result = load_file(
                    filepath, conn,
                    handlers=HANDLERS,
                    validators_by_kind=VALIDATORS_BY_KIND,
                    file_kind=parsed.file_type,
                    year=parsed.year,
                    force=args.force,
                )
                results[result.status] = results.get(result.status, 0) + 1
                if result.status == "failed":
                    for f in result.failures[:5]:
                        logger.error(f"  validation: [{f.rule}] {f.message}")
                    if args.stop_on_error:
                        logger.error("--stop-on-error: bailing after validation failure")
                        break
            except Exception as exc:
                logger.exception(f"{filepath.name}: unhandled exception, see traceback")
                results["failed"] += 1
                if args.stop_on_error:
                    logger.error("--stop-on-error: bailing")
                    break

        logger.info(
            "done. "
            f"success={results['success']} "
            f"already_loaded={results.get('already_loaded',0)} "
            f"skipped={results['skipped']} "
            f"failed={results['failed']}"
        )
        return 0 if results["failed"] == 0 else 1
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
