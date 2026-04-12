"""
utils/db_backup.py -Table-level backup/restore helpers for the ETL pipeline.

Before each ETL stage, call backup_tables() to snapshot the affected tables.
After the stage completes, call validate_and_finalize() to either:
  - keep the new data (drop backups) if no data was lost
  - restore from backups if the load failed or row counts dropped below threshold

Usage in run_pipeline.py:
    from utils.db_backup import backup_tables, validate_and_finalize

    backups = backup_tables(db_path, ["schools", "charter_schools"])
    ok = run_subprocess(cmd)
    validate_and_finalize(db_path, backups, load_succeeded=ok)
"""

import sqlite3
import time


def backup_tables(db_path: str, tables: list[str]) -> list[dict]:
    """
    Snapshot each table by copying it to a timestamped backup table.

    Returns a list of backup records:
        [{"table": "schools", "backup": "schools_bak_1712345678", "pre_count": 97735}, ...]

    If a table doesn't exist yet (first-ever load), it is recorded with
    backup=None and pre_count=0 -no backup is needed because there's nothing to lose.
    """
    conn = sqlite3.connect(db_path)
    records = []
    try:
        cur = conn.cursor()
        ts = int(time.time())
        for table in tables:
            cur.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,)
            )
            if not cur.fetchone():
                # Table doesn't exist yet -nothing to back up
                records.append({"table": table, "backup": None, "pre_count": 0})
                continue

            backup_name = f"{table}_bak_{ts}"
            cur.execute(f"CREATE TABLE {backup_name} AS SELECT * FROM {table}")
            cur.execute(f"SELECT COUNT(*) FROM {backup_name}")
            pre_count = cur.fetchone()[0]
            conn.commit()
            records.append({"table": table, "backup": backup_name, "pre_count": pre_count})
            print(f"  [backup] {table}: {pre_count:,} rows -> {backup_name}")
    finally:
        conn.close()
    return records


def validate_and_finalize(
    db_path: str,
    backups: list[dict],
    load_succeeded: bool,
    min_fraction: float = 0.90,
) -> bool:
    """
    After a load stage, decide whether to keep the new data or restore from backups.

    Rules:
    - If load_succeeded=False (subprocess non-zero exit): always restore.
    - If any table lost more than (1 - min_fraction) of its rows: restore all tables.
    - Otherwise: drop all backups (new data is kept).

    Returns True if the new data was kept, False if backups were restored.
    """
    if not backups:
        return load_succeeded

    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()

        if not load_succeeded:
            print("  [guard] Load failed -restoring all backups.")
            _restore_all(cur, backups)
            conn.commit()
            return False

        # Check each table for data loss
        problems = []
        for rec in backups:
            table = rec["table"]
            pre_count = rec["pre_count"]
            if pre_count == 0:
                # First load -any outcome is acceptable
                cur.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,)
                )
                if cur.fetchone():
                    cur.execute(f"SELECT COUNT(*) FROM {table}")
                    post_count = cur.fetchone()[0]
                    print(f"  [guard] {table}: first load -> {post_count:,} rows")
                continue

            cur.execute(f"SELECT COUNT(*) FROM {table}")
            post_count = cur.fetchone()[0]
            fraction = post_count / pre_count

            if fraction < min_fraction:
                problems.append(
                    f"{table}: {pre_count:,} -> {post_count:,} rows "
                    f"({fraction:.0%} retained, need >={min_fraction:.0%})"
                )
            else:
                print(
                    f"  [guard] {table}: {pre_count:,} -> {post_count:,} rows "
                    f"({fraction:.0%} retained) OK"
                )

        if problems:
            print("  [guard] Data loss detected -restoring all backups:")
            for p in problems:
                print(f"    - {p}")
            _restore_all(cur, backups)
            conn.commit()
            return False

        # All good -drop backups
        for rec in backups:
            if rec["backup"]:
                cur.execute(f"DROP TABLE IF EXISTS {rec['backup']}")
        conn.commit()
        print("  [guard] Validation passed -backups dropped.")
        return True

    finally:
        conn.close()


def _restore_all(cur: sqlite3.Cursor, backups: list[dict]):
    """Drop current tables and rename backups back. Called inside an open connection."""
    for rec in backups:
        table = rec["table"]
        backup = rec["backup"]
        if backup is None:
            # No backup existed (first load that failed) -just drop whatever was loaded
            cur.execute(f"DROP TABLE IF EXISTS {table}")
            print(f"  [restore] {table}: dropped (no prior data to restore)")
        else:
            cur.execute(f"DROP TABLE IF EXISTS {table}")
            cur.execute(f"ALTER TABLE {backup} RENAME TO {table}")
            print(f"  [restore] {table}: restored from {backup}")


def list_orphaned_backups(db_path: str) -> list[str]:
    """
    Return names of any leftover _bak_* tables (e.g. from a crashed run).
    Run this manually to inspect or clean up stranded backups.
    """
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE '%_bak_%'"
        )
        return [r[0] for r in cur.fetchall()]
    finally:
        conn.close()


def drop_orphaned_backups(db_path: str):
    """Drop all leftover _bak_* tables. Use after confirming you don't need them."""
    names = list_orphaned_backups(db_path)
    if not names:
        print("No orphaned backup tables found.")
        return
    conn = sqlite3.connect(db_path)
    try:
        for name in names:
            conn.execute(f"DROP TABLE IF EXISTS {name}")
            print(f"  Dropped: {name}")
        conn.commit()
    finally:
        conn.close()
