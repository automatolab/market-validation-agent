"""Centralized logging for market validation agent."""
import logging
import sys


def get_logger(name: str) -> logging.Logger:
    """Get a logger with consistent formatting. Logs to stderr."""
    logger = logging.getLogger(f"mv.{name}")
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stderr)
        handler.setFormatter(logging.Formatter(
            "[%(name)s] %(message)s"
        ))
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
    return logger
