"""Centralized logging for the market-validation agent.

Two output modes, selected by environment:

  MARKET_LOG_FORMAT=json   → structured JSON (one object per line) for
                             production / log aggregators
  MARKET_LOG_FORMAT=text    → human-readable (default, for local dev)

Log level is controlled by MARKET_LOG_LEVEL (default INFO).

A per-request context id can be set via ``set_request_id(...)`` — the
JSON formatter will include it as the ``request_id`` field, and the text
formatter will prefix messages with ``[req=<id>]``. The FastAPI app
installs a middleware that sets/clears this around every request so all
logs emitted inside a handler share the same correlation id.
"""

from __future__ import annotations

import contextvars
import json
import logging
import os
import sys
import time
import uuid
from typing import Any

# ── Request-id context (thread-safe via contextvars) ─────────────────────────

# ContextVar works correctly under both ``asyncio`` (FastAPI handlers) and
# ``ThreadPoolExecutor`` child threads as long as the executor forwards the
# parent context — asyncio's default executor and ``contextvars.copy_context``
# do this. For plain threads we fall back to a module global for the main
# thread, which keeps older CLI paths behaving as before.
_REQUEST_ID: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "mv_request_id", default=None
)


def set_request_id(request_id: str | None) -> contextvars.Token[str | None]:
    """Set the current request id. Returns a token to restore via reset()."""
    return _REQUEST_ID.set(request_id)


def reset_request_id(token: contextvars.Token[str | None]) -> None:
    _REQUEST_ID.reset(token)


def current_request_id() -> str | None:
    return _REQUEST_ID.get()


def new_request_id() -> str:
    """Generate a short random request id (11 chars)."""
    return uuid.uuid4().hex[:11]


# ── Formatters ───────────────────────────────────────────────────────────────

# Fields that ``logging`` injects into every LogRecord. Anything beyond these
# is treated as caller-provided ``extra=`` and emitted in the JSON output.
_RESERVED_LOGRECORD_FIELDS = frozenset(
    {
        "name", "msg", "args", "levelname", "levelno", "pathname", "filename",
        "module", "exc_info", "exc_text", "stack_info", "lineno", "funcName",
        "created", "msecs", "relativeCreated", "thread", "threadName",
        "processName", "process", "taskName", "message", "asctime",
    }
)


class _JsonFormatter(logging.Formatter):
    """Emit each record as a single-line JSON object.

    Fields:
      ts, level, logger, msg           — always present
      request_id                        — if set in context
      exception                         — if exc_info was passed
      <any extra kwargs>                — caller-provided via extra={...}
    """

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(record.created)),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }

        rid = current_request_id()
        if rid:
            payload["request_id"] = rid

        # Surface caller-provided extras (e.g. _log.info("...", extra={"x": 1})).
        for key, value in record.__dict__.items():
            if key in _RESERVED_LOGRECORD_FIELDS or key.startswith("_"):
                continue
            # Keep the payload JSON-safe even if the caller passed something
            # exotic — fall back to repr() rather than crashing the logger.
            try:
                json.dumps(value)
                payload[key] = value
            except (TypeError, ValueError):
                payload[key] = repr(value)

        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)

        return json.dumps(payload, ensure_ascii=False)


class _TextFormatter(logging.Formatter):
    """Human-readable formatter that includes request_id when set."""

    def format(self, record: logging.LogRecord) -> str:
        rid = current_request_id()
        prefix = f"[{record.name}]"
        if rid:
            prefix = f"[{record.name} req={rid}]"
        return f"{prefix} {record.getMessage()}"


# ── Public API ───────────────────────────────────────────────────────────────

_HANDLER_ATTR = "_mv_handler_installed"


def _configure_root_handler() -> logging.Handler:
    """Install one stderr handler with the configured formatter.

    Idempotent — multiple calls reuse the same handler so repeated
    ``get_logger()`` calls never double-emit.
    """
    root = logging.getLogger("mv")

    # If we've already wired a handler, reuse it.
    for h in root.handlers:
        if getattr(h, _HANDLER_ATTR, False):
            return h

    fmt_name = os.environ.get("MARKET_LOG_FORMAT", "text").lower()
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(_JsonFormatter() if fmt_name == "json" else _TextFormatter())
    setattr(handler, _HANDLER_ATTR, True)

    level_name = os.environ.get("MARKET_LOG_LEVEL", "INFO").upper()
    root.setLevel(getattr(logging, level_name, logging.INFO))
    root.addHandler(handler)
    root.propagate = False
    return handler


def get_logger(name: str) -> logging.Logger:
    """Return a child logger under the ``mv.<name>`` namespace.

    All child loggers inherit the single stderr handler installed on the
    ``mv`` parent, so downstream callers don't need to care about config.
    """
    _configure_root_handler()
    return logging.getLogger(f"mv.{name}")
