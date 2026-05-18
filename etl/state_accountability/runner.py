"""State-agnostic load runner.

One public entry point: `load_file(filepath, conn, *, handlers, validators, force=False)`.
The state-specific HANDLERS dict and validator registry are passed in by the
entry-point script (etl/load_tn_accountability.py).

Transaction shape:
    BEGIN (implicit, psycopg2)
        parse the file (no DB writes yet)
        transform → dict[table_name, rows]
        INSERT etl_load_log row with status='success' (optimistic)
        INSERT fact rows (each table's rows, in handler.target_tables order)
        run validators
        if any failures: raise ValidationError → triggers ROLLBACK
    COMMIT (on success) or ROLLBACK (on exception)
    [outside the transaction] on failure: record_failed_attempt() in fresh tx.

Skip handlers run in their own short transaction — they don't share atomicity
with fact rows because they don't write any.

`force=True`: if a prior successful load exists for this file hash, set
parent_load_id to that load and DELETE the prior fact rows before loading.
"""
from __future__ import annotations

import logging
import traceback
from pathlib import Path

from psycopg2.extras import execute_values

from .log import (
    find_successful_load_by_hash,
    insert_skipped_log,
    insert_success_log,
    record_emergency_failure,
    record_failed_attempt,
    sha256_file,
)
from .types import (
    FileHandler,
    LoadResult,
    SkipHandler,
    ValidationError,
)
from .validators import Validator, run_validators, split_by_severity

logger = logging.getLogger(__name__)


# Tradeoff note: etl_load_log.notes is text (not jsonb). Failure tracebacks
# are naturally text-shaped; skip reasons are stored as plain text too. If we
# later need to query skip reasons by category ("show me all loads skipped
# because of no destination table"), add a structured outcome_details jsonb
# column then. Not pre-built — the use case isn't here yet.


def load_file(
    filepath: Path,
    conn,
    *,
    handlers: dict,
    validators_by_kind: dict[str, list[Validator]],
    file_kind: str,
    year: int,
    force: bool = False,
) -> LoadResult:
    """Load a single source file. State-agnostic — caller supplies the per-state
    handlers and validators."""
    file_hash = sha256_file(filepath)
    handler = handlers.get(file_kind)
    if handler is None:
        raise KeyError(
            f"No handler registered for file_kind={file_kind!r}. "
            f"Add it to the state HANDLERS dict or update the parser."
        )

    # Skip path — logged but no transaction over fact rows.
    if isinstance(handler, SkipHandler):
        cur = conn.cursor()
        load_id = insert_skipped_log(
            cur,
            source_file=filepath.name,
            source_file_hash=file_hash,
            source_file_kind=file_kind,
            reason=handler.reason,
        )
        conn.commit()
        logger.info(f"{filepath.name}: skipped — {handler.reason} (load_id={load_id})")
        return LoadResult(filepath=filepath, status="skipped", load_id=load_id, reason=handler.reason)

    if not isinstance(handler, FileHandler):
        raise TypeError(f"Handler for {file_kind} is neither FileHandler nor SkipHandler: {type(handler).__name__}")

    # Idempotency check.
    cur = conn.cursor()
    prior = find_successful_load_by_hash(cur, file_hash)
    conn.commit()  # release the read transaction before we open a write one

    if prior and not force:
        logger.info(f"{filepath.name}: already loaded as load_id={prior.load_id}, skipping (use force=True to reload)")
        return LoadResult(filepath=filepath, status="already_loaded", load_id=prior.load_id)

    parent_load_id = prior.load_id if (prior and force) else None

    # The load transaction. Everything inside this try is one atomic unit.
    try:
        cur = conn.cursor()

        # If force-reloading, delete the prior load's fact rows first.
        # Uses handler.target_tables (not a separate registry) so the runner
        # never disagrees with the handler about which tables it writes to.
        if force and prior:
            for table in handler.target_tables:
                cur.execute(f"DELETE FROM {table} WHERE source_load_id = %s", (prior.load_id,))
                logger.info(f"  force: deleted {cur.rowcount} prior rows from {table} (load_id={prior.load_id})")

        # Parse + transform (no DB writes during these steps, but they happen
        # inside the transaction so the whole operation is one logical unit).
        df = handler.parse(filepath)
        rows_by_table = handler.transform(df, year)

        # row_counts reflects the transform output, not actual INSERT success.
        # That's safe because: (a) executemany raises on first error, aborting
        # the transaction; (b) any constraint violation triggers ROLLBACK,
        # which removes the log row too. So a committed log row always has
        # matching committed fact rows.
        row_counts = {t: len(rs) for t, rs in rows_by_table.items()}
        load_id = insert_success_log(
            cur,
            source_file=filepath.name,
            source_file_hash=file_hash,
            source_file_kind=file_kind,
            row_counts_by_table=row_counts,
            parent_load_id=parent_load_id,
        )

        # Insert fact rows, table by table.
        for table in handler.target_tables:
            rows = rows_by_table.get(table, [])
            _insert_rows(cur, table, rows, load_id)

        # Pre-commit validation. error-severity failures raise → ROLLBACK.
        # warning-severity failures get logged + threaded into the load row's
        # notes column, but do NOT abort the commit.
        validators = validators_by_kind.get(file_kind, [])
        failures = run_validators(cur, load_id, validators)
        errors, warnings = split_by_severity(failures)
        if errors:
            raise ValidationError(errors)
        if warnings:
            note = "Warnings (load committed):\n" + "\n".join(
                f"  [{w.rule}] {w.message}" for w in warnings
            )
            cur.execute(
                "UPDATE etl_load_log SET notes = %s WHERE load_id = %s",
                (note[:4000], load_id),
            )
            for w in warnings:
                logger.warning(f"{filepath.name}: [{w.rule}] {w.message}")

        conn.commit()
        logger.info(
            f"{filepath.name}: loaded (load_id={load_id}, "
            f"rows={', '.join(f'{t}={n}' for t, n in row_counts.items())}"
            f"{', warnings=' + str(len(warnings)) if warnings else ''})"
        )
        return LoadResult(filepath=filepath, status="success", load_id=load_id, row_counts=row_counts)

    except Exception as exc:
        conn.rollback()
        original_notes = f"{type(exc).__name__}: {exc}\n\n{traceback.format_exc()}"
        # Fresh transaction for the failure log row.
        try:
            failed_load_id = record_failed_attempt(
                conn,
                source_file=filepath.name,
                source_file_hash=file_hash,
                source_file_kind=file_kind,
                notes=original_notes,
            )
            logger.error(f"{filepath.name}: FAILED (failure logged as load_id={failed_load_id})")
        except Exception as log_exc:
            # DB-side outage or broken connection. Fall back to disk so the
            # failure is recorded somewhere even when the DB is unreachable.
            logger.error(
                f"{filepath.name}: FAILED, and DB failure-log write also failed: {log_exc}. "
                f"Writing emergency disk log."
            )
            record_emergency_failure(
                source_file=filepath.name,
                original_notes=original_notes,
                log_write_error=f"{type(log_exc).__name__}: {log_exc}",
            )
        if isinstance(exc, ValidationError):
            return LoadResult(
                filepath=filepath, status="failed", failures=exc.failures,
                reason="validation",
            )
        raise


def _insert_rows(cur, table_name: str, rows: list[dict], load_id: int) -> None:
    """Generic bulk INSERT. Handlers produce dict rows; runner injects source_load_id
    and turns them into tuples in the correct column order.

    Assumes all rows in the list have the same key set — checked below. If a
    handler's transform branches conditionally on row content and omits keys
    in some rows, the per-row key check catches it with a clear message.
    """
    if not rows:
        return
    expected_keys = set(rows[0].keys())
    if "source_load_id" in expected_keys:
        raise ValueError(
            f"Handler emitted source_load_id for table {table_name!r}; "
            "the runner injects this — handlers must not set it."
        )
    for i, row in enumerate(rows[1:], start=1):
        row_keys = set(row.keys())
        if row_keys != expected_keys:
            extra = row_keys - expected_keys
            missing = expected_keys - row_keys
            raise ValueError(
                f"Row {i} in {table_name!r} batch has inconsistent keys vs row 0. "
                f"Extra: {sorted(extra) or '(none)'}; Missing: {sorted(missing) or '(none)'}. "
                "All rows in a batch must share the same key set."
            )

    columns = list(rows[0].keys())
    columns_with_load = columns + ["source_load_id"]
    cols_sql = ", ".join(columns_with_load)
    placeholders = "(" + ", ".join(["%s"] * len(columns_with_load)) + ")"
    sql = f"INSERT INTO {table_name} ({cols_sql}) VALUES %s"
    values = [tuple(r[c] for c in columns) + (load_id,) for r in rows]
    execute_values(cur, sql, values, template=placeholders)
