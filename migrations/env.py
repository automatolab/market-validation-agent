"""Alembic environment for market-validation-agent.

Resolves the SQLite DB path at runtime via
``market_validation.research.resolve_db_path()`` so migrations follow the
same storage convention as the rest of the app. Override with
``MARKET_DB_PATH=/path/to/sqlite.db`` for CI / test isolation.

All migrations in versions/ use raw-SQL ``op.execute(...)`` — we don't
bring in SQLAlchemy models just for this. ``render_as_batch=True`` is
enabled so future ALTER TABLE migrations work on SQLite (which can't
drop/alter columns without a table rebuild).
"""

from __future__ import annotations

import os
from logging.config import fileConfig
from pathlib import Path

from alembic import context
from sqlalchemy import engine_from_config, pool

# Initialize logging from alembic.ini.
config = context.config
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = None  # we use raw SQL migrations, not SQLAlchemy models


def _resolve_database_url() -> str:
    override = os.environ.get("MARKET_DB_PATH")
    if override:
        return f"sqlite:///{override}"

    # Defer the import so ``alembic init`` / script generation works without
    # the full app dependency tree.
    from market_validation.research import resolve_db_path
    db_path = resolve_db_path(Path.cwd())
    return f"sqlite:///{db_path}"


def run_migrations_offline() -> None:
    """Generate SQL scripts without connecting to a live database."""
    url = _resolve_database_url()
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        render_as_batch=True,
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Connect to the DB and run migrations."""
    ini_section = config.get_section(config.config_ini_section) or {}
    ini_section["sqlalchemy.url"] = _resolve_database_url()

    connectable = engine_from_config(
        ini_section,
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            render_as_batch=True,  # required for SQLite ALTER TABLE
        )
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
