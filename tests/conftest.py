"""
tests/conftest.py — shared fixtures.

The suite runs against whichever backend DATABASE_URL points at (local
Postgres in dev, SQLite if unset). It assumes the DB is already
populated — every smoke test that depends on row-level lookups asks
the DB for an existing key first and `pytest.skip()`s if none exists,
so a fresh empty DB will skip rather than fail.
"""

import os
import sys

import pytest
from fastapi.testclient import TestClient

# Repo root on the path so `import db` and `import api.main` work
# regardless of where pytest was invoked from.
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))


@pytest.fixture(scope="session")
def client():
    """One TestClient per session — startup/init_db only fires once."""
    from api.main import app
    with TestClient(app) as c:
        yield c


@pytest.fixture(scope="session")
def known_school_nces_id():
    """An NCES ID that actually exists in the current DB.

    Used by /schools/{nces_id} smoke test. Skips if the schools table is
    empty rather than 404'ing on a hardcoded id that may not be loaded
    on every developer's machine.
    """
    import db
    conn = db.get_connection()
    cur = conn.cursor()
    cur.execute("SELECT nces_id FROM schools WHERE nces_id IS NOT NULL LIMIT 1")
    row = cur.fetchone()
    conn.close()
    if not row or not row[0]:
        pytest.skip("schools table empty; load via etl/fetch_nces_schools.py")
    return str(row[0])


@pytest.fixture(scope="session")
def known_school_state():
    """A state abbreviation with at least one school. Used by list filters."""
    import db
    conn = db.get_connection()
    cur = conn.cursor()
    cur.execute("SELECT state FROM schools WHERE state IS NOT NULL GROUP BY state ORDER BY COUNT(*) DESC LIMIT 1")
    row = cur.fetchone()
    conn.close()
    if not row or not row[0]:
        pytest.skip("schools table has no state data")
    return row[0]
