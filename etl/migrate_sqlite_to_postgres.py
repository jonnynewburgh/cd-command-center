"""
etl/migrate_sqlite_to_postgres.py — One-time migration from SQLite to PostgreSQL.

Usage:
    # 1. Set DATABASE_URL to point at your PostgreSQL database
    set DATABASE_URL=postgresql://postgres:yourpassword@localhost:5432/cd_command_center

    # 2. Run from the repo root
    python etl/migrate_sqlite_to_postgres.py

What it does:
    1. Creates all tables in PostgreSQL (via db.init_db())
    2. Copies all rows from SQLite → PostgreSQL in batches
    3. Resets PostgreSQL sequences so auto-increment continues from the right value

It is safe to re-run: all inserts use ON CONFLICT DO NOTHING.
"""

import os
import sys
import sqlite3

import psycopg2
import psycopg2.extras

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

SQLITE_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data", "cd_command_center.sqlite"
)

# Tables to skip — SQLite internals or legacy tables superseded by newer ones
SKIP_TABLES = {"sqlite_sequence", "charter_schools"}

# These tables have a SERIAL (auto-increment) 'id' column.
# We insert with explicit id values so foreign keys stay consistent,
# then reset the sequence so future inserts don't collide.
SERIAL_TABLES = {
    "schools", "lea_accountability", "nmtc_projects", "cde_allocations",
    "fqhc", "ece_centers", "irs_990", "irs_990_history", "cdfi_directory",
    "state_programs", "enrollment_history", "cdfi_awards", "user_notes",
    "user_bookmarks", "documents", "financial_ratios", "data_loads",
    "market_rates", "hud_ami", "hud_fmr", "cra_institutions",
    "cra_assessment_areas", "sba_loans", "hmda_activity", "bls_unemployment",
    "bls_qcew", "scsc_cpf", "nmtc_coalition_projects",
    "cra_sb_discl", "cra_sb_aggr",
}

BATCH_SIZE = 5000


def get_sqlite_tables(sqlite_conn):
    cur = sqlite_conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
    return [r[0] for r in cur.fetchall() if r[0] not in SKIP_TABLES]


def get_row_count(sqlite_conn, table):
    cur = sqlite_conn.cursor()
    cur.execute(f"SELECT COUNT(*) FROM {table}")
    return cur.fetchone()[0]


def migrate_table(table, sqlite_conn, pg_conn):
    total = get_row_count(sqlite_conn, table)
    if total == 0:
        print(f"  {table}: empty — skipping")
        return 0

    s_cur = sqlite_conn.cursor()
    pg_cur = pg_conn.cursor()

    # Get column names from SQLite
    s_cur.execute(f"SELECT * FROM {table} LIMIT 1")
    cols = [d[0] for d in s_cur.description]
    col_list = ", ".join(cols)

    # For SERIAL tables, use OVERRIDING SYSTEM VALUE so we can supply explicit id values.
    # This preserves the existing IDs so any foreign key references remain valid.
    if table in SERIAL_TABLES and "id" in cols:
        insert_sql = (
            f"INSERT INTO {table} ({col_list}) "
            f"OVERRIDING SYSTEM VALUE VALUES %s "
            f"ON CONFLICT DO NOTHING"
        )
    else:
        insert_sql = (
            f"INSERT INTO {table} ({col_list}) VALUES %s ON CONFLICT DO NOTHING"
        )

    s_cur.execute(f"SELECT * FROM {table}")
    inserted = 0

    while True:
        rows = s_cur.fetchmany(BATCH_SIZE)
        if not rows:
            break
        # Convert sqlite3.Row objects to plain tuples
        psycopg2.extras.execute_values(pg_cur, insert_sql, [tuple(r) for r in rows])
        inserted += len(rows)
        print(f"  {table}: {inserted:,} / {total:,}", end="\r")

    pg_conn.commit()
    print(f"  {table}: {inserted:,} rows            ")

    # Reset the sequence so the next auto-generated id picks up after the max we just inserted
    if table in SERIAL_TABLES and "id" in cols:
        pg_cur.execute(
            f"SELECT setval(pg_get_serial_sequence('{table}', 'id'), COALESCE(MAX(id), 1)) FROM {table}"
        )
        pg_conn.commit()

    return inserted


def main():
    pg_url = os.environ.get("DATABASE_URL", "")
    if not (pg_url.startswith("postgres://") or pg_url.startswith("postgresql://")):
        print("Error: DATABASE_URL must be set to a PostgreSQL connection string.")
        print()
        print("  Windows:  set DATABASE_URL=postgresql://postgres:yourpassword@localhost:5432/cd_command_center")
        print("  Mac/Linux: export DATABASE_URL=postgresql://postgres:yourpassword@localhost:5432/cd_command_center")
        sys.exit(1)

    if not os.path.exists(SQLITE_PATH):
        print(f"Error: SQLite database not found at {SQLITE_PATH}")
        sys.exit(1)

    print("CD Command Center -- SQLite -> PostgreSQL Migration")
    print(f"  Source: {SQLITE_PATH}")
    print(f"  Target: {pg_url}")
    print()

    # Step 1: Create all tables in PostgreSQL
    print("Step 1: Creating tables in PostgreSQL...")
    import db
    db.init_db()
    print("  Done.\n")

    # Step 2: Connect to both databases
    sqlite_conn = sqlite3.connect(SQLITE_PATH)
    sqlite_conn.row_factory = sqlite3.Row
    pg_conn = psycopg2.connect(pg_url)

    tables = get_sqlite_tables(sqlite_conn)
    print(f"Step 2: Migrating {len(tables)} tables...\n")

    total_rows = 0
    errors = []

    for table in tables:
        try:
            n = migrate_table(table, sqlite_conn, pg_conn)
            total_rows += n
        except Exception as e:
            pg_conn.rollback()
            errors.append((table, str(e)))
            print(f"  {table}: ERROR — {e}")

    sqlite_conn.close()
    pg_conn.close()

    print(f"\nMigration complete. {total_rows:,} total rows copied.")

    if errors:
        print(f"\nErrors ({len(errors)} tables failed):")
        for table, err in errors:
            print(f"  {table}: {err}")
        sys.exit(1)

    print()
    print("Next steps:")
    print("  1. Verify the migration:")
    print('     psql -U postgres -d cd_command_center -c "SELECT COUNT(*) FROM schools"')
    print()
    print("  2. Set DATABASE_URL permanently so all scripts use PostgreSQL:")
    print("     - Open System Properties -> Advanced -> Environment Variables")
    print("     - Add: DATABASE_URL = postgresql://postgres:yourpassword@localhost:5432/cd_command_center")
    print()
    print("  3. Re-run any ETL scripts — they will now write to PostgreSQL automatically.")


if __name__ == "__main__":
    main()
