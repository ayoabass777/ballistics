"""Shared logger setup for API utility modules."""

import logging
import os
import sys
from typing import Optional


def _resolve_level(explicit_level: Optional[str] = None) -> int:
    """Resolve effective log level from explicit arg or env vars."""
    level_name = (
        explicit_level
        or os.getenv("HEADLINER_LOG_LEVEL")
        or os.getenv("API_LOG_LEVEL")
        or "INFO"
    ).upper()
    return getattr(logging, level_name, logging.INFO)


def get_logger(name: str, level: Optional[str] = None) -> logging.Logger:
    """
    Create or return a configured logger.

    Logging is sent to stdout for easy collection by container/platform logging.
    """
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger

    logger.setLevel(_resolve_level(level))
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(
        logging.Formatter(
            fmt="%(asctime)s %(levelname)s %(name)s %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
    )
    logger.addHandler(handler)
    logger.propagate = False
    return logger
