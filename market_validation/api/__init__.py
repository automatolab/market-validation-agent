"""FastAPI backend for the market-validation dashboard.

The public entry point is ``create_app()`` which returns a fully-wired
``FastAPI`` instance. ``python -m market_validation.api`` (or the
``market-api`` console script) runs it under uvicorn.
"""

from market_validation.api.app import create_app

__all__ = ["create_app"]
