"""Database helpers: connection, schema, migrations.

The SQLite access layer (``_connect``, ``_ensure_schema``, and CRUD helpers)
still lives in ``market_validation.research`` for backward-compat. This package
is the new home for alembic-driven schema evolution — anything that touches
schema version should live here.
"""

from market_validation.db.migrations import (
    current_revision,
    stamp_baseline,
    upgrade_to_head,
)

__all__ = ["current_revision", "stamp_baseline", "upgrade_to_head"]
