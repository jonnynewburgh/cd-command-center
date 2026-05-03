"""
Alembic environment.

Wired to honor the same DATABASE_URL env var as db.py:
- Default: local SQLite at data/cd_command_center.sqlite
- postgres://... or postgresql://... when set

This avoids putting the connection string in alembic.ini (which is
committed) and keeps every dev / prod environment using the same
backend selection logic as the application.
"""
import os
import sys
from logging.config import fileConfig

from sqlalchemy import engine_from_config, pool

from alembic import context

# Make the repo root importable so this file can read the same env logic
# the app uses, if we want to extend it later (e.g. read target_metadata
# off SQLAlchemy models once they exist).
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)


def _resolve_db_url() -> str:
    """Read the same DATABASE_URL env var that db.py honors.

    SQLAlchemy needs `postgresql+psycopg2://...` rather than the bare
    `postgres://...` short form, so normalize both prefixes.
    """
    url = os.environ.get("DATABASE_URL")
    if url:
        if url.startswith("postgres://"):
            url = "postgresql+psycopg2://" + url[len("postgres://"):]
        elif url.startswith("postgresql://") and "+" not in url.split("://", 1)[0]:
            url = "postgresql+psycopg2://" + url[len("postgresql://"):]
        return url
    # SQLite fallback — matches db.DATABASE_URL default
    sqlite_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "data", "cd_command_center.sqlite",
    )
    return f"sqlite:///{sqlite_path}"


# Inject the resolved URL into Alembic's config so engine_from_config picks it up
config.set_main_option("sqlalchemy.url", _resolve_db_url())

# init_db() is the source of truth for the schema today; SQLAlchemy models
# don't exist yet, so autogenerate is unavailable. Migrations are written
# by hand against op.execute() / op.create_table() until that changes.
target_metadata = None


def run_migrations_offline() -> None:
    """Generate SQL scripts without a live connection."""
    context.configure(
        url=config.get_main_option("sqlalchemy.url"),
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations against a live DB."""
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    with connectable.connect() as connection:
        context.configure(connection=connection, target_metadata=target_metadata)
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
