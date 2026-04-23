"""Run the FastAPI dashboard API under uvicorn.

Launched by ``python -m market_validation.api`` or the ``market-api``
console script. Honors ``MARKET_API_HOST`` / ``MARKET_API_PORT`` env vars
so deployment tooling can override without CLI args.
"""

from __future__ import annotations

import argparse
import os
import sys


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="market-api",
        description="Run the market-validation dashboard API.",
    )
    parser.add_argument(
        "--host",
        default=os.environ.get("MARKET_API_HOST", "127.0.0.1"),
        help="Bind host (default: 127.0.0.1, override via MARKET_API_HOST).",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=int(os.environ.get("MARKET_API_PORT", "8788")),
        help="Bind port (default: 8788, override via MARKET_API_PORT).",
    )
    parser.add_argument(
        "--reload",
        action="store_true",
        help="Enable uvicorn auto-reload (development only).",
    )
    parser.add_argument(
        "--log-level",
        default="info",
        choices=["critical", "error", "warning", "info", "debug", "trace"],
        help="uvicorn log level.",
    )
    args = parser.parse_args()

    try:
        import uvicorn
    except ImportError:
        print("uvicorn not installed. Run: pip install 'uvicorn[standard]>=0.27'", file=sys.stderr)
        sys.exit(1)

    # Passing the import string (rather than the app object) is what enables
    # --reload and worker scaling; uvicorn re-imports the module on reload.
    uvicorn.run(
        "market_validation.api.app:app",
        host=args.host,
        port=args.port,
        reload=args.reload,
        log_level=args.log_level,
    )


if __name__ == "__main__":
    main()
