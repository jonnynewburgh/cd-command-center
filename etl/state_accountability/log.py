"""etl_load_log helpers — single source of truth for load-row writes.

Every load writes exactly one log row, even if the source file populates
multiple destination tables. row_counts_by_table holds the per-table breakdown
as JSON. row_count is the total.

Optimistic-success pattern: on a successful load, the log row is INSERTed with
status='success' inside the same transaction as the fact rows. If validators
fail and the transaction rolls back, the log row vanishes with it. Failures
are recorded separately by record_failed_attempt() in a fresh transaction.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class PriorLoad:
    load_id: int
    load_ts: Any
    source_file_hash: str


def sha256_file(filepath: Path, chunk_size: int = 65536) -> str:
    """Compute sha256 of a file's bytes."""
    h = hashlib.sha256()
    with filepath.open("rb") as f:
        while chunk := f.read(chunk_size):
            h.update(chunk)
    return h.hexdigest()


def find_successful_load_by_hash(cur, file_hash: str) -> PriorLoad | None:
    """Return the most recent successful load matching this hash, or None."""
    cur.execute(
        """
        SELECT load_id, load_ts, source_file_hash
        FROM etl_load_log
        WHERE source_file_hash = %s AND status = 'success'
        ORDER BY load_ts DESC
        LIMIT 1
        """,
        (file_hash,),
    )
    row = cur.fetchone()
    if row is None:
        return None
    return PriorLoad(load_id=row[0], load_ts=row[1], source_file_hash=row[2])


def insert_success_log(
    cur,
    *,
    source_file: str,
    source_file_hash: str,
    source_file_kind: str,
    row_counts_by_table: dict[str, int],
    parent_load_id: int | None = None,
    notes: str | None = None,
) -> int:
    """INSERT a status='success' log row, return load_id.

    Called inside the load transaction. If the surrounding transaction rolls
    back, this row vanishes — no orphan log rows on failure.
    """
    total = sum(row_counts_by_table.values())
    cur.execute(
        """
        INSERT INTO etl_load_log
            (source_file, source_file_hash, source_file_kind, row_count,
             row_counts_by_table, status, notes, parent_load_id)
        VALUES (%s, %s, %s, %s, %s::jsonb, 'success', %s, %s)
        RETURNING load_id
        """,
        (
            source_file,
            source_file_hash,
            source_file_kind,
            total,
            json.dumps(row_counts_by_table),
            notes,
            parent_load_id,
        ),
    )
    return cur.fetchone()[0]


def insert_skipped_log(
    cur,
    *,
    source_file: str,
    source_file_hash: str,
    source_file_kind: str,
    reason: str,
) -> int:
    """INSERT a status='skipped' log row. Used by SkipHandler dispatch.

    Has its own transaction (the runner commits after this call) because
    skipped loads don't share atomicity with fact-row inserts (there are none).
    """
    cur.execute(
        """
        INSERT INTO etl_load_log
            (source_file, source_file_hash, source_file_kind, row_count,
             status, notes)
        VALUES (%s, %s, %s, 0, 'skipped', %s)
        RETURNING load_id
        """,
        (source_file, source_file_hash, source_file_kind, reason),
    )
    return cur.fetchone()[0]


EMERGENCY_LOG_PATH = "logs/failed_loads_emergency.log"
"""Disk fallback. Used only when the DB-write of a failure log row itself fails
(e.g. connection broken, server-side abort). Format: one line per event,
timestamped, includes filepath and original exception. This is the last record
that survives DB-down scenarios."""


def record_failed_attempt(
    conn,
    *,
    source_file: str,
    source_file_hash: str,
    source_file_kind: str,
    notes: str,
) -> int:
    """INSERT a status='failed' log row in a fresh transaction.

    Called AFTER the load transaction has rolled back. Calls conn.rollback()
    explicitly first (idempotent) to ensure the connection is in a clean
    transaction state — defense against partial-failure scenarios where the
    outer rollback might not have fully reset state (e.g. network blip,
    server-side abort).
    """
    conn.rollback()  # idempotent; guarantees clean state for the next BEGIN
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO etl_load_log
            (source_file, source_file_hash, source_file_kind, row_count,
             status, notes)
        VALUES (%s, %s, %s, 0, 'failed', %s)
        RETURNING load_id
        """,
        (source_file, source_file_hash, source_file_kind, notes[:4000]),  # cap long tracebacks
    )
    load_id = cur.fetchone()[0]
    conn.commit()
    return load_id


def record_emergency_failure(*, source_file: str, original_notes: str, log_write_error: str) -> None:
    """Last-resort disk write when even the DB failure-log INSERT fails.

    Appends one line to logs/failed_loads_emergency.log. Never raises — there
    is no further fallback. Caller logs this as a warning.
    """
    from datetime import datetime
    import os

    os.makedirs(os.path.dirname(EMERGENCY_LOG_PATH) or ".", exist_ok=True)
    line = (
        f"{datetime.utcnow().isoformat()}Z\t"
        f"source_file={source_file}\t"
        f"log_write_error={log_write_error[:500]}\t"
        f"original={original_notes[:1000]}\n"
    )
    try:
        with open(EMERGENCY_LOG_PATH, "a", encoding="utf-8") as f:
            f.write(line)
    except Exception:
        # If even disk write fails (read-only FS, full disk), there's nothing
        # more we can do. Don't raise — caller is already in an exception path.
        pass
