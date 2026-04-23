"""FastAPI application factory.

``create_app()`` wires together the static-file mount, Jinja2 templates, and
all route modules. Kept as a factory so tests can construct an isolated app
instance without touching module-level state.
"""

from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from market_validation.api.middleware import RequestIDMiddleware
from market_validation.environment import load_project_env
from market_validation.log import get_logger

_log = get_logger("api")

_PACKAGE_DIR = Path(__file__).resolve().parent.parent


def create_app() -> FastAPI:
    """Build the FastAPI app with routes, static files, and templates."""
    # Load .env before any handler imports — matches the behavior of the
    # stdlib dashboard server.
    load_project_env()

    app = FastAPI(
        title="Market Validation API",
        description="Dashboard + data endpoints for the market-validation pipeline.",
        version="0.1.0",
    )

    # Correlation-id + access-log middleware — must be the outermost layer so
    # every log line emitted during the request (including from inner
    # middleware and exception handlers) carries the same request id.
    app.add_middleware(RequestIDMiddleware)

    # Static files (CSS/JS) — served from market_validation/static/
    app.mount(
        "/static",
        StaticFiles(directory=str(_PACKAGE_DIR / "static")),
        name="static",
    )

    # Jinja2 templates — attached to app.state so routes can access them via
    # request.app.state.templates. Autoescape is opt-in per-extension.
    templates = Jinja2Templates(directory=str(_PACKAGE_DIR / "templates"))
    app.state.templates = templates

    # Register routers — kept in a separate module so routes are easy to
    # find and the app factory stays small.
    from market_validation.api.routes import register_routes
    register_routes(app)

    _log.info("FastAPI app created: static=%s templates=%s",
              _PACKAGE_DIR / "static", _PACKAGE_DIR / "templates")
    return app


# Module-level app for ``uvicorn market_validation.api.app:app``.
app = create_app()
