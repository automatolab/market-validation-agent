"""HTTP middleware for the FastAPI app.

``RequestIDMiddleware`` attaches a per-request correlation id to
``market_validation.log``'s contextvar so every log line emitted during the
request includes the same id. The id is taken from an incoming
``X-Request-ID`` header (trusted when present, so upstream proxies /
load balancers can thread their own id through) or generated otherwise.
It's also echoed back on the response for client-side correlation.
"""

from __future__ import annotations

import time
from collections.abc import Callable

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

from market_validation.log import (
    current_request_id,
    get_logger,
    new_request_id,
    reset_request_id,
    set_request_id,
)

_log = get_logger("api.http")


class RequestIDMiddleware(BaseHTTPMiddleware):
    """Generate / propagate ``X-Request-ID`` and wire it into the log context.

    Header name is case-insensitive (FastAPI normalizes). Incoming values are
    length-capped at 64 chars to avoid turning logs into DoS vectors.
    """

    HEADER = "x-request-id"
    MAX_INCOMING_ID = 64

    async def dispatch(
        self,
        request: Request,
        call_next: Callable[[Request], Response],
    ) -> Response:
        incoming = (request.headers.get(self.HEADER) or "").strip()
        rid = incoming[: self.MAX_INCOMING_ID] if incoming else new_request_id()

        token = set_request_id(rid)
        start = time.perf_counter()
        status: int | None = None
        try:
            response = await call_next(request)
            status = response.status_code
            return _attach_header(response, rid)
        except Exception:
            # Re-raise so FastAPI's exception handlers still fire, but log
            # access *before* propagating so we never lose the correlation
            # id on 500s.
            _log_access(request, 500, start)
            raise
        finally:
            if status is not None:
                _log_access(request, status, start)
            reset_request_id(token)


def _attach_header(response: Response, rid: str) -> Response:
    """Echo the request id back so clients can reference it in bug reports."""
    response.headers[RequestIDMiddleware.HEADER] = rid
    return response


def _log_access(request: Request, status: int, start: float) -> None:
    """Emit one structured access-log line per request.

    In JSON mode these become a directly-pipeable access log — the
    ``extra=`` keys land as top-level JSON fields.
    """
    duration_ms = round((time.perf_counter() - start) * 1000, 2)
    # Health checks are noisy and not useful in the access log; skip them
    # unless DEBUG is on.
    if request.url.path == "/health" and not _log.isEnabledFor(10):  # 10 = DEBUG
        return
    _log.info(
        "%s %s -> %s (%.2fms)",
        request.method,
        request.url.path,
        status,
        duration_ms,
        extra={
            "http_method": request.method,
            "http_path": request.url.path,
            "http_status": status,
            "http_duration_ms": duration_ms,
            "http_client": _client_ip(request),
        },
    )


def _client_ip(request: Request) -> str:
    """Best-effort client IP — checks X-Forwarded-For first for proxied setups."""
    fwd = request.headers.get("x-forwarded-for")
    if fwd:
        return fwd.split(",")[0].strip()
    if request.client is not None:
        return request.client.host
    return ""


__all__ = ["RequestIDMiddleware", "current_request_id"]
