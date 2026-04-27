"""
etl/audit_backend_parity.py — Compare row counts between the SQLite snapshot
and the active Postgres backend.

Per the canonical decision recorded in
docs/debug/phase6_reconciliation_2026-04-26.md, Postgres is the canonical
source of truth for this project. This script is a diagnostic; drift between
SQLite and Postgres is informational only, not a failure.

Usage:
    python etl/audit_backend_parity.py
    python etl/audit_backend_parity.py --sqlite path/to/snapshot.sqlite
    python etl/audit_backend_parity.py --postgres "postgresql://..."

If DATABASE_URL is set in the environment, the Postgres URL defaults to it.
The SQLite path defaults to data/cd_command_center.sqlite.
"""

import argparse
import os
import sqlite3
import sys

import psycopg2

DEFAULT_SQLITE = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data",
    "cd_command_center.sqlite",
)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sqlite", default=DEFAULT_SQLITE,
                        help="Path to SQLite file (default: data/cd_command_center.sqlite)")
    parser.add_argument("--postgres", default=os.environ.get("DATABASE_URL", ""),
                        help="Postgres connection string (default: $DATABASE_URL)")
    args = parser.parse_args()

    if not os.path.exists(args.sqlite):
        print(f"SQLite file not found at {args.sqlite}", file=sys.stderr)
        sys.exit(1)
    if not args.postgres or not args.postgres.startswith(("postgres://", "postgresql://")):
        print("Need --postgres or DATABASE_URL set to a postgres:// URL", file=sys.stderr)
        sys.exit(1)

    sqlite_conn = sqlite3.connect(args.sqlite)
    sqlite_cur = sqlite_conn.cursor()
    pg_conn = psycopg2.connect(args.postgres)
    pg_cur = pg_conn.cursor()

    sqlite_cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
    sqlite_tables = [r[0] for r in sqlite_cur.fetchall() if not r[0].startswith("sqlite_")]
    pg_cur.execute("SELECT tablename FROM pg_tables WHERE schemaname='public' ORDER BY tablename")
    pg_tables = [r[0] for r in pg_cur.fetchall()]

    sqlite_only = sorted(set(sqlite_tables) - set(pg_tables))
    pg_only = sorted(set(pg_tables) - set(sqlite_tables))
    both = sorted(set(sqlite_tables) & set(pg_tables))

    print("Schema parity")
    print(f"  SQLite tables:   {len(sqlite_tables)}")
    print(f"  Postgres tables: {len(pg_tables)}")
    print(f"  SQLite-only:     {sqlite_only or 'none'}")
    print(f"  Postgres-only:   {pg_only or 'none'}")
    print()

    print("Row counts")
    print(f"  {'TABLE':30}  {'SQLITE':>10}  {'POSTGRES':>10}  {'DELTA':>12}")
    print("  " + "-" * 66)

    synced = 0
    drifted = 0
    for t in both:
        try:
            sqlite_cur.execute(f"SELECT COUNT(*) FROM {t}")
            s = sqlite_cur.fetchone()[0]
            pg_cur.execute(f"SELECT COUNT(*) FROM {t}")
            p = pg_cur.fetchone()[0]
            d = p - s
            if d == 0:
                synced += 1
                print(f"  {t:30}  {s:>10,}  {p:>10,}  {d:>+12,}")
            else:
                drifted += 1
                print(f"  {t:30}  {s:>10,}  {p:>10,}  {d:>+12,}  drift")
        except Exception as e:
            print(f"  {t:30}  err: {str(e)[:40]}")

    print()
    print(f"  Synced:  {synced} / {len(both)}")
    print(f"  Drifted: {drifted}")
    print()
    print("Note: Postgres is canonical per docs/debug/phase6_reconciliation_2026-04-26.md.")
    print("      SQLite is a frozen snapshot; drift = expected, not a failure.")

    sqlite_conn.close()
    pg_conn.close()


if __name__ == "__main__":
    main()
