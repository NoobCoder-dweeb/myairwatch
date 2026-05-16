"""Logging configuration for air quality data extraction."""

import logging
import sys


def get_logger(name: str, level: int = logging.INFO) -> logging.Logger:
    """Get configured logger."""
    # create basic logger
    logger = logging.getLogger(name)
    logger.setLevel(level)

    # if there is no log handling, create a StreamHandler object
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(level)
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    return logger


DEFAULT_LOGGER = get_logger("air_quality")
