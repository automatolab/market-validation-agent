"""Programmatic wrapper around alembic for the market-validation DB.

The CLI entry point is ``market-db-migrate`` (also
``python -m market_validation.db.migrations``). Subcommands:

    upgrade       Bring the DB schema to head (creating it if needed).
    stamp         Mark an existing DB as already at the given revision,
                  without running the migration's upgrade(). Use on DBs
                  that pre-date alembic so the baseline isn't re-executed.
    current       Print the current revision.
    history       List all revisions.

For programmatic use, ``upgrade_to_head()`` / ``stamp_baseline()`` /
``current_revision()`` are exposed on ``market_validation.db``.
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

from market_validation.log import get_logger

_log = get_logger("db.migrations")


# ── Config resolution ───────────────────────────────────────────────────────

# The alembic.ini lives at the repo root (one level above market_validation/).
_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
_ALEMBIC_INI = _PROJECT_ROOT / "alembic.ini"
_BASELINE_REV = "0001_baseline"


def _alembic_config(db_path: str | Path | None = None):
    """Build an alembic Config object.

    Accepts an optional explicit ``db_path`` so callers can point migrations
    at a test database without touching env vars. When provided, it's passed
    to migrations/env.py via MARKET_DB_PATH.
    """
    from alembic.config import Config

    if not _ALEMBIC_INI.exists():
        raise FileNotFoundError(
            f"alembic.ini not found at {_ALEMBIC_INI}. "
            "Expected at the project root."
        )

    cfg = Config(str(_ALEMBIC_INI))
    # script_location is relative in the .ini — resolve it relative to the
    # config file's directory, which works whether we run from the repo root
    # or from inside the package.
    cfg.set_main_option("script_location", str(_PROJECT_ROOT / "migrations"))

    if db_path is not None:
        os.environ["MARKET_DB_PATH"] = str(db_path)

    return cfg


# ── Public API ──────────────────────────────────────────────────────────────

def upgrade_to_head(db_path: str | Path | None = None) -> None:
    """Run all pending migrations on the target DB."""
    from alembic import command

    cfg = _alembic_config(db_path)
    _log.info("alembic upgrade head (db=%s)", db_path or "default")
    command.upgrade(cfg, "head")


def stamp_baseline(db_path: str | Path | None = None) -> None:
    """Mark an existing DB as being at the baseline revision without running
    the baseline's upgrade() — used when adopting alembic for a database
    that was already created via ``_ensure_schema``."""
    from alembic import command

    cfg = _alembic_config(db_path)
    _log.info("alembic stamp %s (db=%s)", _BASELINE_REV, db_path or "default")
    command.stamp(cfg, _BASELINE_REV)


def current_revision(db_path: str | Path | None = None) -> str | None:
    """Return the current DB revision string, or None if the alembic_version
    table doesn't exist yet (fresh/unstamped DB)."""
    import sqlalchemy as sa

    cfg = _alembic_config(db_path)
    url = cfg.get_main_option("sqlalchemy.url") or _resolve_db_url()
    engine = sa.create_engine(url)
    with engine.connect() as conn:
        result = conn.exec_driver_sql(
            "SELECT version_num FROM alembic_version LIMIT 1"
        ).fetchone()
        return result[0] if result else None


def _resolve_db_url() -> str:
    override = os.environ.get("MARKET_DB_PATH")
    if override:
        return f"sqlite:///{override}"
    from market_validation.research import resolve_db_path
    return f"sqlite:///{resolve_db_path(Path.cwd())}"


# ── CLI ─────────────────────────────────────────────────────────────────────

def main() -> int:
    parser = argparse.ArgumentParser(
        prog="market-db-migrate",
        description="Schema migration CLI for market-validation-agent.",
    )
    parser.add_argument(
        "--db",
        help="Override DB path (also settable via MARKET_DB_PATH env var).",
    )
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("upgrade", help="Bring the DB to the latest schema version.")
    sub.add_parser(
        "stamp",
        help="Mark the DB as being at baseline without running the migration "
             "(use for DBs created before alembic was adopted).",
    )
    sub.add_parser("current", help="Print the current schema revision.")
    sub.add_parser("history", help="List all schema revisions.")

    args = parser.parse_args()

    try:
        if args.command == "upgrade":
            upgrade_to_head(args.db)
            print("OK: database is at head.")
            return 0
        if args.command == "stamp":
            stamp_baseline(args.db)
            print(f"OK: database stamped at {_BASELINE_REV}.")
            return 0
        if args.command == "current":
            rev = current_revision(args.db)
            print(rev if rev else "<unstamped>")
            return 0
        if args.command == "history":
            from alembic import command
            command.history(_alembic_config(args.db), indicate_current=True)
            return 0
    except Exception as exc:
        _log.exception("migration command failed: %s", exc)
        print(f"FAILED: {exc}", file=sys.stderr)
        return 1

    parser.print_help()
    return 2


if __name__ == "__main__":
    sys.exit(main())
